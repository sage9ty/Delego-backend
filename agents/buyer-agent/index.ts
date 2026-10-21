import type { AgentDefinition, AgentRunContext, AgentRunResult } from "@delegolabs/types";

export const buyerAgentDefinition: AgentDefinition = {
  id: "buyer-agent",
  role: "buyer",
  name: "Buyer Agent",
  description: "Searches catalog and initiates purchases on behalf of users",
  version: "0.0.1",
};

/**
 * Run the buyer agent for a given context.
 * TODO: Integrate Agent runtime and tool execution
 */
export async function runBuyerAgent(
  _context: AgentRunContext,
  _prompt: string
): Promise<AgentRunResult> {
  return {
    runId: "placeholder",
    status: "awaiting_approval",
    output: null,
    orderId: null,
  };
}
