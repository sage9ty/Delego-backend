/**
 * Multi-merchant checkout orchestrator
 * POST /api/v1/orders/multi-merchant/checkout
 */

import { Pool } from "pg";
import { Redis } from "ioredis";
import Redlock from "redlock";
import { randomUUID } from "node:crypto";
import { getInventoryReservationService } from "./inventory/reservation.js";

function createLogger(name: string) {
  return {
    info: (...args: any[]) => console.log(JSON.stringify({ level: "info", service: name, args })),
    warn: (...args: any[]) => console.warn(JSON.stringify({ level: "warn", service: name, args })),
    error: (...args: any[]) => console.error(JSON.stringify({ level: "error", service: name, args })),
    debug: (...args: any[]) => console.debug(JSON.stringify({ level: "debug", service: name, args })),
  };
}

const log = createLogger("orchestrator:multi-merchant-checkout");

export interface MultiMerchantCheckoutRequest {
  cartId: string;
  shippingAddressId: string;
  settlementCurrency: string;
  discountCodes?: Record<string, string>; // merchantId -> code
}

export interface MultiMerchantCheckoutResponse {
  parentOrderId: string;
  childOrders: Array<{
    orderId: string;
    merchantId: string;
    subtotalStroops: string;
    shippingStroops: string;
    taxStroops: string;
    totalStroops: string;
  }>;
  totalPayableStroops: string;
  escrowPaymentAddress: string;
  reservationExpiry: string;
}

interface CartLineItem {
  id: string;
  cart_id?: string;
  user_id?: string;
  merchant_id?: string;
  product_id?: string;
  quantity: number;
  unit_price_stroops?: string | bigint | number;
  price_stroops?: string | bigint | number;
  unitPriceStroops?: string | bigint | number;
  price?: string | bigint | number;
  name?: string;
}

function getPool(): Pool {
  return new Pool({ connectionString: process.env.DATABASE_URL });
}

const RESERVATION_TTL_MS = 15 * 60 * 1000; // 15 minutes

function getRedis() {
  return new Redis(process.env.REDIS_URL ?? "redis://localhost:6379", { lazyConnect: true });
}

function getRedlock(redis: Redis) {
  return new Redlock([redis as never], {
    driftFactor: 0.01,
    retryCount: 10,
    retryDelay: 200,
    retryJitter: 200,
  });
}

async function loadCartItems(client: any, cartId: string, userId?: string): Promise<CartLineItem[]> {
  const queries = [
    { sql: `SELECT * FROM carts WHERE id = $1`, params: [cartId] },
    { sql: `SELECT * FROM carts WHERE cart_id = $1`, params: [cartId] },
  ];

  let cart: any = null;
  for (const q of queries) {
    try {
      const res = await client.query(q.sql, q.params);
      if (res.rowCount && res.rowCount > 0) {
        cart = res.rows[0];
        break;
      }
    } catch (e) {
      // continue
    }
  }

  if (cart) {
    if (userId && cart.user_id && cart.user_id !== userId) {
      throw new Error("Cart does not belong to the authenticated user");
    }
  }

  const cartItemQueries = [
    { sql: `SELECT * FROM cart_items WHERE cart_id = $1`, params: [cartId] },
    { sql: `SELECT ci.* FROM cart_items ci WHERE ci.cart_id = $1`, params: [cartId] },
    { sql: `SELECT * FROM cart_items WHERE id = $1`, params: [cartId] },
  ];

  for (const q of cartItemQueries) {
    try {
      const res = await client.query(q.sql, q.params);
      if (res.rowCount && res.rowCount > 0) {
        return res.rows;
      }
    } catch (e) {
      continue;
    }
  }

  if (cart && Array.isArray(cart.items)) {
    return cart.items;
  }
  if (cart && cart.line_items) {
    return Array.isArray(cart.line_items) ? cart.line_items : [];
  }

  throw new Error("Cart is empty or not found");
}

function toStroops(val: string | bigint | number | undefined): bigint {
  if (val === undefined || val === null) return BigInt(0);
  if (typeof val === "bigint") return val;
  if (typeof val === "number") {
    if (!Number.isFinite(val)) return BigInt(0);
    return BigInt(Math.round(val));
  }
  const s = String(val).trim();
  if (s === "") return BigInt(0);
  // assume it's already in stroops as string
  return BigInt(s);
}

export async function orchestrateMultiMerchantCheckout(
  request: MultiMerchantCheckoutRequest,
  userId?: string
): Promise<MultiMerchantCheckoutResponse> {
  if (!request.cartId || typeof request.cartId !== "string" || request.cartId.trim() === "") {
    throw new Error("cartId is required");
  }
  if (!request.shippingAddressId || typeof request.shippingAddressId !== "string" || request.shippingAddressId.trim() === "") {
    throw new Error("shippingAddressId is required");
  }
  if (!request.settlementCurrency || typeof request.settlementCurrency !== "string" || request.settlementCurrency.trim() === "") {
    throw new Error("settlementCurrency is required");
  }

  const pool = getPool();
  const redis = getRedis();
  const redlock = getRedlock(redis);

  const parentOrderId = randomUUID();
  const escrowPaymentAddress =
    process.env.ESCROW_PAYMENT_ADDRESS ?? process.env.ESCROW_ADDRESS ?? `escrow-${parentOrderId}`;

  const reservationExpiry = new Date(Date.now() + RESERVATION_TTL_MS).toISOString();

  const client = await pool.connect();
  let lock: any = null;
  try {
    await client.query("BEGIN");

    let cartItems: CartLineItem[] = [];
    try {
      cartItems = await loadCartItems(client, request.cartId, userId);
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    }

    if (!cartItems || cartItems.length === 0) {
      await client.query("ROLLBACK");
      throw new Error("Cart is empty or not found");
    }

    const itemsByMerchant = new Map<string, CartLineItem[]>();
    for (const item of cartItems) {
      if (!item || typeof item.quantity !== "number" || item.quantity <= 0 || !Number.isInteger(item.quantity)) {
        await client.query("ROLLBACK");
        throw new Error("Invalid cart item quantity");
      }
      const merchantId = item.merchant_id || (item as any).merchantId;
      if (!merchantId || typeof merchantId !== "string") {
        await client.query("ROLLBACK");
        throw new Error("Cart items must be associated with a merchant");
      }
      if (!itemsByMerchant.has(merchantId)) {
        itemsByMerchant.set(merchantId, []);
      }
      itemsByMerchant.get(merchantId)!.push({ ...item, merchant_id: merchantId });
    }

    const lockResources: string[] = [];
    for (const items of itemsByMerchant.values()) {
      for (const item of items) {
        const productId = item.product_id || (item as any).productId || (item as any).product_id || item.id;
        if (productId && item.merchant_id) {
          lockResources.push(`inventory:${item.merchant_id}:${productId}`);
        }
      }
    }
    const uniqueResources = [...new Set(lockResources)].sort();
    try {
      lock = await redlock.acquire(uniqueResources.length > 0 ? uniqueResources : ["multi-merchant-checkout-lock"], 10000);
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    }

    try {
      const reservationService = getInventoryReservationService();
      const reservationItems = cartItems
        .map((item) => {
          const merchantId = item.merchant_id || (item as any).merchantId;
          const productId = item.product_id || (item as any).productId || item.id;
          if (!merchantId || !productId) return null;
          return {
            merchantId,
            productId,
            quantity: item.quantity,
            orderId: parentOrderId,
          };
        })
        .filter((x): x is NonNullable<typeof x> => x !== null);

      if (reservationItems.length > 0) {
        await reservationService.reserve(reservationItems);
      }

      const childOrders: MultiMerchantCheckoutResponse["childOrders"] = [];
      let totalPayableStroops = BigInt(0);

      for (const [merchantId, items] of itemsByMerchant.entries()) {
        const childOrderId = randomUUID();
        let subtotalStroops = BigInt(0);

        for (const item of items) {
          const unitPriceVal =
            item.unit_price_stroops || item.price_stroops || (item as any).unitPriceStroops || (item as any).price || "0";
          const unitPrice = toStroops(unitPriceVal);
          subtotalStroops += unitPrice * BigInt(item.quantity);
        }

        const shippingStroops = BigInt(0);
        const taxStroops = BigInt(0);
        const totalStroops = subtotalStroops + shippingStroops + taxStroops;
        totalPayableStroops += totalStroops;

        const lineItems = items.map((item) => ({
          productId: item.product_id || (item as any).productId || item.id,
          quantity: item.quantity,
          unitPriceStroops: toStroops(
            item.unit_price_stroops || item.price_stroops || (item as any).unitPriceStroops || (item as any).price || "0"
          ).toString(),
        }));

        try {
          await client.query(
            `INSERT INTO orders (id, user_id, delegation_id, merchant_id, status, line_items, total_stroops, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())`,
            [
              childOrderId,
              userId ?? null,
              null,
              merchantId,
              "pending",
              JSON.stringify(lineItems),
              totalStroops.toString(),
            ]
          );
        } catch (err) {
          throw err;
        }

        childOrders.push({
          orderId: childOrderId,
          merchantId,
          subtotalStroops: subtotalStroops.toString(),
          shippingStroops: shippingStroops.toString(),
          taxStroops: taxStroops.toString(),
          totalStroops: totalStroops.toString(),
        });
      }

      try {
        await client.query(
          `INSERT INTO orders (id, user_id, delegation_id, merchant_id, status, line_items, total_stroops, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())`,
          [
            parentOrderId,
            userId ?? null,
            null,
            "multi-merchant",
            "pending",
            JSON.stringify(cartItems),
            totalPayableStroops.toString(),
          ]
        );
      } catch (err) {
        throw err;
      }

      await client.query("COMMIT");

      log.info("Multi-merchant checkout completed", {
        parentOrderId,
        childOrderCount: childOrders.length,
      });

      return {
        parentOrderId,
        childOrders,
        totalPayableStroops: totalPayableStroops.toString(),
        escrowPaymentAddress,
        reservationExpiry,
      };
    } catch (error) {
      try {
        if (lock) {
          if (typeof lock.unlock === "function") {
            await lock.unlock();
          } else if (typeof lock.release === "function") {
            await lock.release();
          }
        }
      } catch (releaseError) {
        log.warn("Failed to release lock", { error: (releaseError as Error).message });
      }
      throw error;
    }
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch (e) {
      // ignore
    }
    throw error;
  } finally {
    try {
      if (lock) {
        if (typeof lock.unlock === "function") {
          await lock.unlock().catch(() => {});
        } else if (typeof lock.release === "function") {
          await lock.release().catch(() => {});
        }
      }
    } catch (e) {
      // ignore
    }
    client.release();
    try {
      await redis.quit();
    } catch (e) {
      // ignore
    }
  }
}

export default orchestrateMultiMerchantCheckout;
