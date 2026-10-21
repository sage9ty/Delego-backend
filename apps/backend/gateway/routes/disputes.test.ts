import { describe, it, expect, beforeEach, vi } from "vitest";
import { validateDisputeResponseRequest } from "./disputes.js";

describe("validateDisputeResponseRequest", () => {
  it("validates a correct request", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "I received the goods in good condition",
      evidenceAttachmentUrls: ["https://example.com/receipt.pdf"],
      partialRefundAmountStroops: "1000",
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(true);
  });

  it("rejects missing disputeId", () => {
    const request = {
      disputeId: "",
      responseStatement: "Response",
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(false);
    expect(result.error).toBe("disputeId is required");
  });

  it("rejects missing responseStatement", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "",
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(false);
    expect(result.error).toBe("responseStatement is required");
  });

  it("rejects invalid partialRefundAmountStroops format", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "Response",
      partialRefundAmountStroops: "abc",
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(false);
    expect(result.error).toBe("partialRefundAmountStroops must be a positive integer string");
  });

  it("rejects negative partialRefundAmountStroops", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "Response",
      partialRefundAmountStroops: "-100",
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(false);
    expect(result.error).toBe("partialRefundAmountStroops must be a positive integer string");
  });

  it("accepts evidenceAttachmentUrls as optional", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "Response",
      partialRefundAmountStroops: "1000",
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(true);
  });

  it("rejects invalid evidenceAttachmentUrls type", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "Response",
      evidenceAttachmentUrls: "not-array" as any,
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(false);
    expect(result.error).toBe("evidenceAttachmentUrls must be an array");
  });

  it("rejects non-string evidenceAttachmentUrls", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "Response",
      evidenceAttachmentUrls: [123] as any,
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(false);
    expect(result.error).toBe("evidenceAttachmentUrls must contain only strings");
  });

  it("rejects evidence attachment URLs with disallowed or executable extensions", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "Response",
      evidenceAttachmentUrls: ["https://example.com/malicious.exe"],
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(false);
    expect(result.error).toContain("does not have an allowed extension");
  });

  it("accepts valid image and PDF evidence attachments", () => {
    const request = {
      disputeId: "dispute-123",
      responseStatement: "Response",
      evidenceAttachmentUrls: [
        "https://storage.delego.io/evidence/invoice.pdf",
        "https://storage.delego.io/evidence/damaged_item.png",
        "https://storage.delego.io/evidence/delivery_photo.jpg",
      ],
    };

    const result = validateDisputeResponseRequest(request);
    expect(result.valid).toBe(true);
  });
});
