/**
 * Dispute response API routes (Issue #112).
 *
 * Endpoints for merchants to submit responses, carrier delivery receipts,
 * and optional partial refund counter-offers.
 */

import type { Route, RouteHandler } from "@delegolabs/utils";
import { json, createLogger, route } from "@delegolabs/utils";
import { submitMerchantResponse } from "@delegolabs/payments";
import { readJsonBody } from "../src/request.js";

const log = createLogger("gateway:disputes", process.env.LOG_LEVEL ?? "info");

/**
 * Request DTO for submitting a dispute response.
 */
export interface SubmitDisputeResponseDTO {
  disputeId: string;
  responseStatement: string;
  evidenceAttachmentUrls?: string[];
  partialRefundAmountStroops?: string;
}

/**
 * Validate a dispute response request.
 */
export function validateDisputeResponseRequest(
  request: SubmitDisputeResponseDTO,
): { valid: true } | { valid: false; error: string } {
  if (!request.disputeId || typeof request.disputeId !== "string") {
    return { valid: false, error: "disputeId is required" };
  }

  if (!request.responseStatement || typeof request.responseStatement !== "string") {
    return { valid: false, error: "responseStatement is required" };
  }

  if (request.evidenceAttachmentUrls !== undefined) {
    if (!Array.isArray(request.evidenceAttachmentUrls)) {
      return { valid: false, error: "evidenceAttachmentUrls must be an array" };
    }
    const allowedExtensions = [".jpg", ".jpeg", ".png", ".pdf"];
    for (const url of request.evidenceAttachmentUrls) {
      if (typeof url !== "string") {
        return { valid: false, error: "evidenceAttachmentUrls must contain only strings" };
      }
      const cleanUrl = url.split("?")[0].toLowerCase();
      const hasValidExt = allowedExtensions.some((ext) => cleanUrl.endsWith(ext));
      if (!hasValidExt) {
        return {
          valid: false,
          error: `Evidence attachment URL "${url}" does not have an allowed extension (${allowedExtensions.join(", ")})`,
        };
      }
    }
  }

  if (request.partialRefundAmountStroops !== undefined) {
    if (!/^\d+$/.test(request.partialRefundAmountStroops)) {
      return { valid: false, error: "partialRefundAmountStroops must be a positive integer string" };
    }
  }

  return { valid: true };
}

/**
 * Handle dispute response submission.
 *
 * POST /api/v1/merchant/disputes/:disputeId/response
 */
export const submitDisputeResponseHandler: RouteHandler = async (req, res, params) => {
  const disputeId = params.disputeId;
  if (!disputeId) {
    json(res, 400, {
      data: null,
      error: { code: "MISSING_DISPUTE_ID", message: "disputeId path parameter is required" },
    });
    return;
  }

  // Read and parse request body
  let body: unknown;
  try {
    body = await readJsonBody(req);
  } catch (err) {
    json(res, 400, {
      data: null,
      error: { code: "INVALID_JSON", message: "Invalid JSON body" },
    });
    return;
  }

  if (!body || typeof body !== "object") {
    json(res, 400, {
      data: null,
      error: { code: "INVALID_REQUEST", message: "Request body is required" },
    });
    return;
  }

  const request = body as SubmitDisputeResponseDTO;

  // Validate request
  const validation = validateDisputeResponseRequest(request);
  if (!validation.valid) {
    json(res, 400, {
      data: null,
      error: { code: "VALIDATION_ERROR", message: validation.error },
    });
    return;
  }

  // Validate dispute ID matches path
  if (request.disputeId !== disputeId) {
    json(res, 400, {
      data: null,
      error: { code: "DISPUTE_ID_MISMATCH", message: "disputeId in body must match path parameter" },
    });
    return;
  }

  // Verify caller is the merchant linked to the disputed escrow
  const authHeader = req.headers["authorization"];
  const callerAddress = extractAddressFromAuth(authHeader);
  if (!callerAddress) {
    json(res, 401, {
      data: null,
      error: { code: "AUTH_REQUIRED", message: "Authorization header required" },
    });
    return;
  }

  // TODO: Verify callerAddress is the merchant for this escrow
  // This would require looking up the escrow to get the seller address

  try {
    const dispute = await submitMerchantResponse(
      request.disputeId,
      callerAddress,
      request.responseStatement,
      request.evidenceAttachmentUrls,
      request.partialRefundAmountStroops,
    );

    json(res, 200, {
      data: {
        disputeId: dispute.id,
        status: dispute.status,
        escrowId: dispute.escrowId,
        updatedAt: dispute.updatedAt,
      },
      error: null,
    });
  } catch (err) {
    log.error("Failed to submit dispute response", {
      disputeId,
      error: err instanceof Error ? err.message : String(err),
    });

    if (err instanceof Error) {
      if (err.message.includes("DisputeNotFoundError")) {
        json(res, 404, {
          data: null,
          error: { code: "DISPUTE_NOT_FOUND", message: err.message },
        });
        return;
      }

      if (err.message.includes("InvalidStateTransitionError") || err.message.includes("transition")) {
        json(res, 400, {
          data: null,
          error: { code: "INVALID_STATE_TRANSITION", message: err.message },
        });
        return;
      }

      if (err.message.includes("Only the merchant can submit")) {
        json(res, 403, {
          data: null,
          error: { code: "FORBIDDEN", message: "Only the merchant can submit a response for this dispute" },
        });
        return;
      }
    }

    json(res, 500, {
      data: null,
      error: { code: "SUBMIT_RESPONSE_FAILED", message: err instanceof Error ? err.message : "Failed to submit dispute response" },
    });
  }
};

/**
 * Extract Stellar address from authorization header.
 * Expected format: "Bearer <stellar_address>"
 */
function extractAddressFromAuth(authHeader: string | undefined): string | null {
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return null;
  }
  return authHeader.substring(7).trim() || null;
}

/**
 * Register dispute routes.
 */
export function registerDisputeRoutes(): Route[] {
  return [
    route("POST", "/api/v1/merchant/disputes/:disputeId/response", submitDisputeResponseHandler),
  ];
}
