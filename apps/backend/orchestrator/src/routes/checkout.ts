/**
 * Multi-merchant checkout routes
 */
import type { IncomingMessage, ServerResponse } from "node:http";
import { json, route } from "@delegolabs/utils";
import { orchestrateMultiMerchantCheckout, type MultiMerchantCheckoutRequest } from "../checkout.js";

async function readJsonBody(req: IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
    });
    req.on("end", () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

export async function multiMerchantCheckoutHandler(req: IncomingMessage, res: ServerResponse) {
  try {
    let body: MultiMerchantCheckoutRequest;
    try {
      body = (await readJsonBody(req)) as MultiMerchantCheckoutRequest;
    } catch (e) {
      json(res, 400, {
        data: null,
        error: {
          code: "VALIDATION_ERROR",
          message: "Malformed JSON body",
        },
      });
      return;
    }

    if (!body || typeof body !== "object") {
      json(res, 400, {
        data: null,
        error: {
          code: "VALIDATION_ERROR",
          message: "Invalid request body",
        },
      });
      return;
    }

    if (typeof body.cartId !== "string" || body.cartId.trim() === "") {
      json(res, 400, {
        data: null,
        error: {
          code: "VALIDATION_ERROR",
          message: "cartId is required",
        },
      });
      return;
    }
    if (typeof body.shippingAddressId !== "string" || body.shippingAddressId.trim() === "") {
      json(res, 400, {
        data: null,
        error: {
          code: "VALIDATION_ERROR",
          message: "shippingAddressId is required",
        },
      });
      return;
    }
    if (typeof body.settlementCurrency !== "string" || body.settlementCurrency.trim() === "") {
      json(res, 400, {
        data: null,
        error: {
          code: "VALIDATION_ERROR",
          message: "settlementCurrency is required",
        },
      });
      return;
    }

    const authenticatedUserId = (req as typeof req & { userId?: string }).userId;
    if (!authenticatedUserId) {
      json(res, 401, {
        data: null,
        error: { code: "UNAUTHORIZED", message: "Authentication required" },
      });
      return;
    }

    const result = await orchestrateMultiMerchantCheckout(body, authenticatedUserId);

    json(res, 200, {
      data: result,
      error: null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Internal server error";
    if (message.includes("Insufficient stock")) {
      json(res, 409, {
        data: null,
        error: {
          code: "INSUFFICIENT_STOCK",
          message: "Insufficient stock",
        },
      });
      return;
    }
    if (message.includes("Cart does not belong") || message.includes("not found") || message.includes("empty")) {
      json(res, 404, {
        data: null,
        error: {
          code: "NOT_FOUND",
          message,
        },
      });
      return;
    }
    if (message.includes("required") || message.includes("Invalid") || message.includes("quantity")) {
      json(res, 400, {
        data: null,
        error: {
          code: "VALIDATION_ERROR",
          message,
        },
      });
      return;
    }
    json(res, 500, {
      data: null,
      error: {
        code: "INTERNAL_ERROR",
        message: "Internal server error",
      },
    });
  }
}

export function createMultiMerchantCheckoutRoutes() {
  return [
    route("POST", "/api/v1/orders/multi-merchant/checkout", multiMerchantCheckoutHandler),
  ];
}
