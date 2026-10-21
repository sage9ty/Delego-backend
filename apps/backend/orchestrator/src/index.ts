/**
 * @delegolabs/orchestrator — Workflow coordination
 * #64 Purchase Recovery Engine — reconcileWorkflows compares DB state with on-chain escrow.
 */
import {
  createLogger,
  json,
  route,
  startHttpServer,
  tracedFetch,
  initTelemetry,
  corsMiddleware,
  securityHeadersMiddleware,
  requireAuth,
} from "@delegolabs/utils";
import { Pool } from "pg";
import { Redis } from "ioredis";
import { getCacheClient, acquireRedlock } from "@delegolabs/cache";
import { createOrchestratorHealthRoutes } from "./health.js";
import {
  createWorkflow,
  transitionWorkflow,
  getWorkflow,
  listWorkflows,
} from "../workflows/purchase/index.js";
import {
  checkoutWorkflow,
  createCheckoutWorkflowInput,
  createCheckoutSagaCoordinator,
  type CheckoutWorkflowInput,
} from "../workflows/checkout/index.js";
import { InsufficientStockError } from "./inventory/reservation.js";
import { connectSagaDb, PostgresSagaStore, serializeSagaExecution } from "./saga/index.js";
import { startOutboxRelay, type OutboxRelayHandle } from "./events/outboxRelay.js";
import { PostgresServiceEventOutboxStore } from "./events/postgres-service-event-outbox.js";
import { setServiceEventOutboxStore } from "./events/service-event-outbox.js";
import {
  DistributedLockManager,
  createLockRoutes,
  distributedLocksEnabled,
  resolveOrchestratorInstanceId,
} from "./locks/index.js";
import type { SagaCoordinator } from "./saga/index.js";
import type { CheckoutContext } from "../workflows/checkout/index.js";
import { PostgresTaskStore, tasksSequelize, TaskService, TaskEventBroker } from "./tasks/index.js";
import { createTaskRoutes } from "./tasks/routes.js";
import { DLQStore, DLQService, createDLQRoutes } from "./dlq/index.js";
import { createMultiMerchantCheckoutRoutes } from "./routes/checkout.js";
import type { ConnectionOptions } from "bullmq";
import {
  IdempotencyStore,
  createCheckoutIdempotencyPreHandler,
  type IdempotencyRedis,
} from "./idempotency/index.js";

const SERVICE_NAME = "orchestrator";
const DEFAULT_PORT = 3010;
const MAX_REQUEST_BODY_BYTES = Number(process.env.MAX_REQUEST_BODY_BYTES ?? 1_048_576);

const logLevel = process.env.LOG_LEVEL ?? "info";
const log = createLogger(SERVICE_NAME, logLevel);

// Distributed tracing (Issue #307): enabled when OTEL_EXPORTER_OTLP_ENDPOINT is set.
void initTelemetry(SERVICE_NAME).catch((err: unknown) =>
  log.warn("Telemetry init failed", { error: err instanceof Error ? err.message : String(err) })
);
const port = Number(process.env.ORCHESTRATOR_PORT ?? DEFAULT_PORT);

const sagaStore = new PostgresSagaStore();
let lockManager: DistributedLockManager | null = null;
let checkoutSagaCoordinator: SagaCoordinator<CheckoutContext> = createCheckoutSagaCoordinator(sagaStore);

// ─── #33 Transactional Outbox Relay ──────────────────────────────────────────
// Backs service_event_outbox writes (see workflows/purchase/index.ts transitionWorkflow)
// with an actual Redis publisher, so events survive an orchestrator crash between the
// DB commit and the publish. Disable with ENABLE_OUTBOX_RELAY=false (e.g. for a
// single-purpose worker deployment that doesn't own delivery).
const outboxPool = new Pool({ connectionString: process.env.DATABASE_URL });
setServiceEventOutboxStore(new PostgresServiceEventOutboxStore(outboxPool));
let outboxRelay: OutboxRelayHandle | null = null;

// ─── Checkout Idempotency ────────────────────────────────────────────────────
// Collapses retried / double-submitted checkouts onto a single execution using
// a Redis SET NX EX lease per request fingerprint; duplicates replay the
// original response verbatim.
const checkoutIdempotency = createCheckoutIdempotencyPreHandler({
  store: new IdempotencyStore(
    new Redis(process.env.REDIS_URL ?? "redis://localhost:6379", {
      lazyConnect: true,
      maxRetriesPerRequest: 1,
    }) as unknown as IdempotencyRedis,
  ),
});

// ─── Human Task Management ─────────────────────────────────────────────────
// Durable store backed by human_tasks (database/migrations/027_human_tasks.sql),
// a Redis publisher for real-time inbox updates, and the task service that
// implements routing, assignment, claiming, completion, delegation and SLAs.
const taskStore = new PostgresTaskStore();
let taskBroker: TaskEventBroker | null = null;
const taskService = new TaskService({ store: taskStore });

// ─── Dead Letter Queue (DLQ) Management ────────────────────────────────────
// #310 Auto-replay & remediation worker for failed BullMQ jobs
// Monitors failed jobs (e.g. temporary network partitions) and safely replays them
// after circuit breaker resets to prevent retry storms.
const dlqPool = new Pool({ connectionString: process.env.DATABASE_URL });
const dlqStore = new DLQStore(dlqPool);
let dlqService: DLQService | null = null;

// ─── #64 Reconciliation Engine ───────────────────────────────────────────────

export interface ReconciliationFinding {
  orderId: string;
  workflowState: string;
  escrowStatus: string;
  recommendedAction: "resume" | "settle" | "refund" | "mark_failed" | "noop";
  reason: string;
}

function getPool(): Pool {
  return new Pool({ connectionString: process.env.DATABASE_URL });
}

// ─── #54 Workflow State Persistence ─────────────────────────────────────────

export interface WorkflowSnapshot {
  orderId: string;
  userId: string;
  state: string;
  context: Record<string, unknown>;
  version: number;
  updatedAt: string;
}

/**
 * Persists current XState purchase workflow state and context to PostgreSQL using optimistic versioning.
 */
export async function persistWorkflowState(
  orderId: string,
  state: string,
  context: object,
  expectedVersion?: number,
  userId?: string
): Promise<WorkflowSnapshot> {
  const pool = getPool();
  const uid = (context as any)?.userId ?? userId ?? "00000000-0000-0000-0000-000000000000";
  const jsonContext = JSON.stringify(context);

  if (expectedVersion !== undefined && expectedVersion !== null) {
    const updateRes = await pool.query<{
      order_id: string;
      user_id: string;
      state: string;
      context: Record<string, unknown>;
      version: number;
      updated_at: Date;
    }>(
      `UPDATE purchase_workflows
       SET state = $2, context = $3, version = version + 1, updated_at = CURRENT_TIMESTAMP
       WHERE order_id = $1 AND version = $4
       RETURNING order_id, user_id, state, context, version, updated_at`,
      [orderId, state, jsonContext, expectedVersion]
    );

    if (updateRes.rowCount === 0) {
      const existing = await pool.query(`SELECT version FROM purchase_workflows WHERE order_id = $1`, [orderId]);
      if (existing.rowCount && existing.rowCount > 0) {
        throw new Error(`Optimistic locking conflict: workflow ${orderId} version mismatch (expected ${expectedVersion})`);
      }
      const insertRes = await pool.query<{
        order_id: string;
        user_id: string;
        state: string;
        context: Record<string, unknown>;
        version: number;
        updated_at: Date;
      }>(
        `INSERT INTO purchase_workflows (order_id, user_id, state, context, version, updated_at)
         VALUES ($1, $2, $3, $4, 1, CURRENT_TIMESTAMP)
         RETURNING order_id, user_id, state, context, version, updated_at`,
        [orderId, uid, state, jsonContext]
      );
      const row = insertRes.rows[0];
      return {
        orderId: row.order_id,
        userId: row.user_id,
        state: row.state,
        context: row.context,
        version: row.version,
        updatedAt: row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at),
      };
    }

    const row = updateRes.rows[0];
    return {
      orderId: row.order_id,
      userId: row.user_id,
      state: row.state,
      context: row.context,
      version: row.version,
      updatedAt: row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at),
    };
  } else {
    const upsertRes = await pool.query<{
      order_id: string;
      user_id: string;
      state: string;
      context: Record<string, unknown>;
      version: number;
      updated_at: Date;
    }>(
      `INSERT INTO purchase_workflows (order_id, user_id, state, context, version, updated_at)
       VALUES ($1, $2, $3, $4, 1, CURRENT_TIMESTAMP)
       ON CONFLICT (order_id) DO UPDATE
       SET state = EXCLUDED.state, context = EXCLUDED.context, version = purchase_workflows.version + 1, updated_at = CURRENT_TIMESTAMP
       RETURNING order_id, user_id, state, context, version, updated_at`,
      [orderId, uid, state, jsonContext]
    );
    const row = upsertRes.rows[0];
    return {
      orderId: row.order_id,
      userId: row.user_id,
      state: row.state,
      context: row.context,
      version: row.version,
      updatedAt: row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at),
    };
  }
}

/**
 * Retrieves persisted workflow state and context for recovery on orchestrator restart.
 */
export async function recoverWorkflowState(
  orderId: string
): Promise<{ state: string; context: object; orderId: string; userId: string; version: number; updatedAt: string } | null> {
  const pool = getPool();
  const { rows } = await pool.query<{
    order_id: string;
    user_id: string;
    state: string;
    context: Record<string, unknown>;
    version: number;
    updated_at: Date;
  }>(
    `SELECT order_id, user_id, state, context, version, updated_at
     FROM purchase_workflows
     WHERE order_id = $1`,
    [orderId]
  );

  if (!rows[0]) return null;
  const r = rows[0];
  return {
    orderId: r.order_id,
    userId: r.user_id,
    state: r.state,
    context: r.context,
    version: r.version,
    updatedAt: r.updated_at instanceof Date ? r.updated_at.toISOString() : String(r.updated_at),
  };
}

/**
 * Recovers all in-progress / unfinished purchase workflows during orchestrator startup.
 */
export async function recoverUnfinishedWorkflows(): Promise<WorkflowSnapshot[]> {
  const pool = getPool();
  const { rows } = await pool.query<{
    order_id: string;
    user_id: string;
    state: string;
    context: Record<string, unknown>;
    version: number;
    updated_at: Date;
  }>(
    `SELECT order_id, user_id, state, context, version, updated_at
     FROM purchase_workflows
     WHERE state NOT IN ('COMPLETED', 'CANCELLED', 'FAILED', 'Completed', 'Refunded')
     ORDER BY updated_at ASC`
  );

  return rows.map((r) => ({
    orderId: r.order_id,
    userId: r.user_id,
    state: r.state,
    context: r.context,
    version: r.version,
    updatedAt: r.updated_at instanceof Date ? r.updated_at.toISOString() : String(r.updated_at),
  }));
}

/**
 * Simulated on-chain escrow state lookup.
 * In production this would query the Soroban escrow contract via the wallet service.
 */
async function fetchOnChainEscrowStatus(escrowId: string): Promise<"funded" | "released" | "refunded" | "not_found"> {
  const walletUrl = process.env.WALLET_SERVICE_URL ?? "http://localhost:3012";
  try {
    const res = await tracedFetch(`${walletUrl}/escrow/${encodeURIComponent(escrowId)}/status`);
    if (!res.ok) return "not_found";
    const body = await res.json() as { data?: { status?: string } };
    const status = body.data?.status;
    if (status === "released" || status === "refunded" || status === "funded") return status;
    return "not_found";
  } catch {
    return "not_found";
  }
}

function recommend(
  workflowState: string,
  escrowStatus: string
): { action: ReconciliationFinding["recommendedAction"]; reason: string } {
  // Terminal states — nothing to do
  if (workflowState === "COMPLETED" || workflowState === "CANCELLED") {
    return { action: "noop", reason: "Workflow is in terminal state" };
  }

  if (workflowState === "ESCROW_FUNDED" && escrowStatus === "released") {
    return { action: "settle", reason: "Escrow already released on-chain; advance to COMPLETED" };
  }
  if (workflowState === "ESCROW_FUNDED" && escrowStatus === "refunded") {
    return { action: "refund", reason: "Escrow refunded on-chain; cancel workflow" };
  }
  if ((workflowState === "APPROVED" || workflowState === "INITIATED") && escrowStatus === "funded") {
    return { action: "resume", reason: "Escrow funded on-chain but workflow not advanced" };
  }
  if (workflowState === "FAILED") {
    return { action: "mark_failed", reason: "Workflow already marked failed" };
  }

  return { action: "noop", reason: "State appears consistent" };
}

/**
 * Reconciles in-progress workflows against on-chain escrow state.
 * Resolves discrepancies from server crashes or missed events.
 */
export async function reconcileWorkflows(): Promise<ReconciliationFinding[]> {
  const pool = getPool();
  const findings: ReconciliationFinding[] = [];

  try {
    const { rows } = await pool.query<{
      order_id: string;
      state: string;
      context: { escrowId: string | null };
    }>(
      `SELECT order_id, state, context
       FROM purchase_workflows
       WHERE state NOT IN ('COMPLETED', 'CANCELLED', 'FAILED')
       ORDER BY updated_at ASC`
    );

    for (const row of rows) {
      const escrowId = row.context?.escrowId;
      const escrowStatus = escrowId
        ? await fetchOnChainEscrowStatus(escrowId)
        : "not_found";

      const { action, reason } = recommend(row.state, escrowStatus);

      const finding: ReconciliationFinding = {
        orderId: row.order_id,
        workflowState: row.state,
        escrowStatus,
        recommendedAction: action,
        reason,
      };

      findings.push(finding);

      // Apply safe automated remediations
      if (action === "settle") {
        try {
          await transitionWorkflow(row.order_id, { type: "SETTLE", txHash: "" });
          log.info("Reconciler: advanced workflow to settled", { orderId: row.order_id });
        } catch (err) {
          log.warn("Reconciler: could not settle workflow", { orderId: row.order_id, error: (err as Error).message });
        }
      } else if (action === "refund") {
        try {
          await transitionWorkflow(row.order_id, { type: "CANCEL", reason: "escrow_refunded_on_chain" });
          log.info("Reconciler: cancelled refunded workflow", { orderId: row.order_id });
        } catch (err) {
          log.warn("Reconciler: could not cancel workflow", { orderId: row.order_id, error: (err as Error).message });
        }
      } else if (action !== "noop") {
        log.info("Reconciler: manual action required", { ...finding });
      }
    }

    log.info("Reconciliation complete", { total: rows.length, findings: findings.length });
  } finally {
    await pool.end();
  }

  return findings;
}

// Export workflows for internal use
export {
  createWorkflow,
  transitionWorkflow,
  getWorkflow,
  listWorkflows,
};
function readJsonBody(req: import("node:http").IncomingMessage): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    let body = "";
    let size = 0;
    req.on("data", (chunk) => {
      size += Buffer.byteLength(chunk);
      if (size > MAX_REQUEST_BODY_BYTES) {
        reject(new Error("Request body too large"));
        req.destroy();
        return;
      }
      body += chunk;
    });
    req.on("end", () => {
      try {
        const parsed = body ? JSON.parse(body) : {};
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          throw new Error("JSON body must be an object");
        }
        resolve(parsed as Record<string, unknown>);
      } catch {
        reject(new Error("Invalid JSON body"));
      }
    });
    req.on("error", reject);
  });
}

async function main(): Promise<void> {
  if (distributedLocksEnabled()) {
    lockManager = new DistributedLockManager({
      client: getCacheClient(),
      owner: resolveOrchestratorInstanceId(),
      log: createLogger("orchestrator:locks", logLevel),
    });
    checkoutSagaCoordinator = createCheckoutSagaCoordinator(sagaStore, lockManager);
    log.info("Distributed locks enabled", { owner: lockManager.owner });
  }

  // Connect and recover before accepting traffic so checkout requests never race startup
  // recovery — and fail fast (rather than just logging) if durable saga storage isn't ready.
  await connectSagaDb();
  try {
    await tasksSequelize.authenticate();
    log.info("Human task store database connection established");
  } catch (err) {
    log.warn("Human task store unavailable on startup", { error: err instanceof Error ? err.message : String(err) });
  }

  try {
    const unfinished = await recoverUnfinishedWorkflows();
    log.info("Recovered unfinished purchase workflows during startup", { count: unfinished.length });
  } catch (err) {
    log.warn("Failed to recover unfinished purchase workflows during startup", { error: (err as Error).message });
  }

  if (process.env.ENABLE_OUTBOX_RELAY !== "false") {
    const redisClient = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379", {
      lazyConnect: true,
      maxRetriesPerRequest: 3,
    });
    outboxRelay = startOutboxRelay({ redisClient, log });
  }

  // Real-time task inbox broker: publishes lifecycle events to Redis channels
  // (`human-task:*`) for live UI updates. Disabled when ENABLE_TASK_EVENTS=false or
  // when Redis is unavailable — the service still works, just without live events.
  if (process.env.ENABLE_TASK_EVENTS !== "false") {
    try {
      const redisClient = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379", {
        lazyConnect: true,
        maxRetriesPerRequest: 1,
      });
      taskBroker = new TaskEventBroker(redisClient as never);
      taskService.setBroker(taskBroker);
    } catch (err) {
      log.warn("Task event broker unavailable; disabling real-time inbox events", {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  // Initialize DLQ service with Redis connection for BullMQ
  const redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379";
  const redisConnection: ConnectionOptions = {
    host: new URL(redisUrl).hostname,
    port: Number(new URL(redisUrl).port) || 6379,
  };
  dlqService = new DLQService(dlqStore, redisConnection, log);
  log.info("DLQ service initialized");

  log.info("Starting orchestrator", { port });
  startHttpServer({
    port,
    serviceName: SERVICE_NAME,
    middleware: [corsMiddleware(), securityHeadersMiddleware(), requireAuth()],
    routes: [
      ...createOrchestratorHealthRoutes(
        undefined,
        () => lockManager?.metrics.toPrometheusText() ?? "",
      ),
      ...(lockManager ? createLockRoutes(lockManager) : []),

      ...createTaskRoutes(taskService, taskStore),

      ...(dlqService ? createDLQRoutes(dlqService) : []),

      ...createMultiMerchantCheckoutRoutes(),

      route("POST", "/checkout", async (req, res) => {
        const authenticatedUserId = (req as typeof req & { userId?: string }).userId;
        if (!authenticatedUserId) {
          json(res, 401, {
            data: null,
            error: { code: "UNAUTHORIZED", message: "Authentication required" },
          });
          return;
        }

        let body: Record<string, unknown>;
        try {
          body = await readJsonBody(req);
        } catch (err) {
          json(res, 400, {
            data: null,
            error: {
              code: "VALIDATION_ERROR",
              message: err instanceof Error ? err.message : "Invalid JSON body",
            },
          });
          return;
        }

        const input = body as Partial<Pick<
          CheckoutWorkflowInput,
          "orderId" | "sourceAddress" | "buyerAddress" | "sellerAddress"
        >>;
        if (
          typeof input.orderId !== "string" ||
          typeof input.sourceAddress !== "string" ||
          typeof input.buyerAddress !== "string" ||
          typeof input.sellerAddress !== "string"
        ) {
          json(res, 400, {
            data: null,
            error: {
              code: "VALIDATION_ERROR",
              message: "orderId, sourceAddress, buyerAddress and sellerAddress are required",
            },
          });
          return;
        }

        const idempotency = await checkoutIdempotency(req, res, {
          userId: authenticatedUserId,
          walletAddress: input.buyerAddress,
          items: body.items ?? body.inventoryItems ?? [],
          nonce: typeof body.nonce === "string" ? body.nonce : input.orderId,
        });
        if (idempotency === "replied") return;

        try {
          const sagaId = `checkout:${input.orderId}`;
          const workflowInput = createCheckoutWorkflowInput(
            {
              orderId: input.orderId,
              sourceAddress: input.sourceAddress,
              buyerAddress: input.buyerAddress,
              sellerAddress: input.sellerAddress,
            },
            authenticatedUserId,
          );
          const lock = await acquireRedlock(getCacheClient(), {
            resource: `cart:${workflowInput.orderId}`,
            ttlMs: 15_000,
            retryCount: 0,
            retryDelayMs: 50,
          });

          if (!lock) {
            json(res, 409, {
              data: null,
              error: { code: "CART_ALREADY_LOCKED", message: `Checkout already in progress for cart:${workflowInput.orderId}` },
            });
            return;
          }

          try {
            const result = await checkoutWorkflow(workflowInput, checkoutSagaCoordinator, sagaId);
            json(res, result.status === "completed" || result.status === "compensated" ? 200 : 502, {
              data: serializeSagaExecution(result),
              error:
                result.status === "completed" || result.status === "compensated"
                  ? null
                  : { code: "CHECKOUT_SAGA_FAILED", message: result.error ?? "Checkout saga failed" },
            });
          } finally {
            await lock.release();
          }
        } catch (err) {
          if (err instanceof InsufficientStockError) {
            json(res, 409, {
              data: null,
              error: { code: "INSUFFICIENT_STOCK", message: err.message },
            });
            return;
          }
          json(res, 502, {
            data: null,
            error: {
              code: "CHECKOUT_SAGA_FAILED",
              message: err instanceof Error ? err.message : "Checkout saga failed",
            },
          });
        }
      }),

      route("GET", "/sagas/:sagaId", async (_req, res, params) => {
        const record = await sagaStore.get(params.sagaId);
        if (!record) {
          json(res, 404, {
            data: null,
            error: { code: "NOT_FOUND", message: `Saga not found: ${params.sagaId}` },
          });
          return;
        }
        json(res, 200, {
          data: serializeSagaExecution(record),
          error: null,
        });
      }),

      route("POST", "/sagas/:sagaId/resume", async (_req, res, params) => {
        try {
          const result = await checkoutSagaCoordinator.resume(params.sagaId);
          json(res, 200, {
            data: serializeSagaExecution(result),
            error: null,
          });
        } catch (err) {
          const message = err instanceof Error ? err.message : "Failed to resume saga";
          const status = message.startsWith("Saga not found") ? 404 : 502;
          json(res, status, {
            data: null,
            error: { code: status === 404 ? "NOT_FOUND" : "SAGA_RESUME_FAILED", message },
          });
        }
      }),

      // Issue #48 — Saga event-sourcing audit trail
      route("GET", "/sagas/:sagaId/events", async (_req, res, params) => {
        const events = await sagaStore.getEvents(params.sagaId);
        json(res, 200, {
          data: events.map((event) => ({
            sagaId: event.sagaId,
            correlationId: event.correlationId,
            eventType: event.eventType,
            fromStatus: event.fromStatus,
            toStatus: event.toStatus,
            payload: event.payload,
            createdAt: event.createdAt.toISOString(),
          })),
          error: null,
        });
      }),

      // Issue #146 — Workflow state migration endpoints
      route("POST", "/migrations", async (req, res) => {
        let body: Record<string, unknown>;
        try {
          body = await readJsonBody(req);
        } catch {
          json(res, 400, { data: null, error: { code: "VALIDATION_ERROR", message: "Invalid JSON body" } });
          return;
        }
        const { plan, instances, dryRun } = body as {
          plan?: { workflowType: string; fromVersion: string; toVersion: string; stateMappings: unknown[]; contextTransforms: unknown[]; safetyChecks: string[]; estimatedDurationMs: number };
          instances?: Array<{ instanceId: string; state: string; context: Record<string, unknown> }>;
          dryRun?: boolean;
        };
        if (!plan || !instances) {
          json(res, 400, { data: null, error: { code: "VALIDATION_ERROR", message: "plan and instances are required" } });
          return;
        }
        try {
          const { createMigration } = await import("./migration/index.js");
          const migration = await createMigration(plan as any, instances, dryRun ?? false);
          json(res, 201, { data: migration, error: null });
        } catch (err) {
          json(res, 500, { data: null, error: { code: "MIGRATION_FAILED", message: (err as Error).message } });
        }
      }),

      route("GET", "/migrations/:migrationId", async (_req, res, params) => {
        const { getMigration, getMigrationProgress } = await import("./migration/index.js");
        const migration = getMigration(params.migrationId);
        if (!migration) {
          json(res, 404, { data: null, error: { code: "NOT_FOUND", message: "Migration not found" } });
          return;
        }
        const progress = getMigrationProgress(params.migrationId);
        json(res, 200, { data: { ...migration, progress }, error: null });
      }),

      route("GET", "/migrations", async (_req, res) => {
        const { listMigrations } = await import("./migration/index.js");
        const migrations = listMigrations();
        json(res, 200, { data: migrations, error: null });
      }),

      // Issue #145 — Timeout escalation endpoints
      route("GET", "/timeout/analytics/:workflowType", async (_req, res, params) => {
        const { getWorkflowTimeoutHandler } = await import("./timeout/escalation.js");
        const handler = getWorkflowTimeoutHandler();
        const analytics = handler.getAnalytics(params.workflowType);
        json(res, 200, { data: analytics, error: null });
      }),

      route("GET", "/timeout/events", async (req, res) => {
        const url = new URL(req.url ?? "/", `http://localhost`);
        const workflowType = url.searchParams.get("workflowType") ?? undefined;
        const { getWorkflowTimeoutHandler } = await import("./timeout/escalation.js");
        const handler = getWorkflowTimeoutHandler();
        const events = handler.getTimeoutEvents(workflowType);
        json(res, 200, { data: events, error: null });
      }),
    ],
  });

  // Issue #48 — Saga crash recovery runs after the server is accepting traffic so startup
  // never blocks new requests. Failed/time-out sagas are auto-recovered or compensated in the
  // background; the timeout sweeper keeps healing them while the service is live.
  void checkoutSagaCoordinator.recoverAll().then(
    (result) => {
      log.info("Saga startup recovery complete", {
        recovered: result.recovered,
        failed: result.failed,
        details: result.details,
      });
    },
    (err) => {
      log.error("Saga startup recovery failed", { error: err instanceof Error ? err.message : String(err) });
    }
  );

  const sagaSweeper = checkoutSagaCoordinator.startTimeoutSweeper();
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, () => sagaSweeper.stop());
  }

  // ─── Human task SLA sweeper ──────────────────────────────────────────────
  // Periodically escalates/expires tasks that breach their SLA. Interval defaults
  // to 5 minutes; set TASK_SLA_SCAN_INTERVAL_MS=0 to disable the background loop
  // (escalations can still be triggered on demand via POST /tasks/sla/scan).
  const slaIntervalMs = Number(process.env.TASK_SLA_SCAN_INTERVAL_MS ?? 300_000);
  if (slaIntervalMs > 0) {
    const { scanSla } = await import("./tasks/sla.js");
    const graceHours = Number(process.env.TASK_SLA_GRACE_HOURS ?? 24);
    await scanSla(taskStore, { graceHours }).catch((err) =>
      log.warn("Initial human task SLA scan failed", { error: err instanceof Error ? err.message : String(err) })
    );
    const slaTimer = setInterval(() => {
      void scanSla(taskStore, { graceHours }).catch((err) =>
        log.warn("Human task SLA scan failed", { error: err instanceof Error ? err.message : String(err) })
      );
    }, slaIntervalMs);
    slaTimer.unref();
    for (const signal of ["SIGINT", "SIGTERM"] as const) {
      process.once(signal, () => clearInterval(slaTimer));
    }
  }

  // ─── DLQ Auto-Replay Cron ──────────────────────────────────────────────
  // Issue #310: Periodically auto-replays failed jobs with recoverable network errors
  // Interval defaults to 5 minutes; set DLQ_AUTO_REPLAY_INTERVAL_MS=0 to disable
  const dlqReplayIntervalMs = Number(process.env.DLQ_AUTO_REPLAY_INTERVAL_MS ?? 300_000);
  if (dlqReplayIntervalMs > 0 && dlqService) {
    await dlqService.autoReplayRecoverableJobs().catch((err) =>
      log.warn("Initial DLQ auto-replay failed", { error: err instanceof Error ? err.message : String(err) })
    );
    const dlqTimer = setInterval(() => {
      void dlqService?.autoReplayRecoverableJobs().catch((err) =>
        log.warn("DLQ auto-replay failed", { error: err instanceof Error ? err.message : String(err) })
      );
    }, dlqReplayIntervalMs);
    dlqTimer.unref();
    for (const signal of ["SIGINT", "SIGTERM"] as const) {
      process.once(signal, () => clearInterval(dlqTimer));
    }
    log.info("DLQ auto-replay cron started", { intervalMs: dlqReplayIntervalMs });
  }
}

main().catch((err) => {
  log.error("Orchestrator startup failed", {
    error: err instanceof Error ? err.message : String(err),
  });
  process.exitCode = 1;
});

// ─── Graceful Shutdown ─────────────────────────────────────────────────────
// Stops the OutboxRelay's poll loop and awaits its in-flight batch before the
// process exits, so a deploy/restart never abandons a claimed-but-not-yet-published
// batch — see events/outboxRelay.ts's stop() for the drain semantics.

async function gracefulShutdown(signal: NodeJS.Signals): Promise<void> {
  log.info("Received shutdown signal", { signal });

  if (lockManager) {
    try {
      await lockManager.releaseAll();
    } catch (err) {
      log.error("Error releasing distributed locks", { error: (err as Error).message });
    }
  }

  if (outboxRelay) {
    try {
      await outboxRelay.stop();
    } catch (err) {
      log.error("Error stopping outbox relay", { error: (err as Error).message });
    }
  }

  if (dlqService) {
    try {
      await dlqService.close();
    } catch (err) {
      log.error("Error closing DLQ service", { error: (err as Error).message });
    }
  }

  process.exit(0);
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    void gracefulShutdown(signal);
  });
}

// Export workflows and state machine for internal use (issue #7 & #54)
export { RedisPublisher } from "./pubsub/index.js";
export type { PublishResult, RedisClient } from "./pubsub/index.js";

export { purchaseWorkflow, restorePurchaseWorkflow } from "../workflows/purchase/index.js";
export { checkoutWorkflow } from "../workflows/checkout/index.js";
export { publishWorkflowEvent, createWorkflowCorrelationId } from "./workflow-events.js";
export type { WorkflowEventEnvelope } from "./workflow-events.js";
export { PurchaseWorkflowMachine } from "../state/index.js";
export type { PurchaseState, PurchaseEvent } from "../state/index.js";
