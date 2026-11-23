import type { Env } from './shortLinks';
import { handle } from './shortLinks';
import { handleLive, handleLiveLink, type LiveEnv } from './live';
import { movedRedirect } from './moved';
import { handleUsage, type UsageEnv } from './usage';

// The Worker's entry module (wrangler.jsonc "main"). It exports the handler
// and the Durable Object classes of the live location (live.ts) and of the
// usage counts (usage.ts), and nothing else: the Workers runtime reads every
// named export of this module as an entry point, so constants and helpers
// live in their own files.

export { LiveBudget, LiveRide } from './live';
export { UsageStats } from './usage';

export default {
  fetch: async (request: Request, env: Env & LiveEnv & UsageEnv): Promise<Response> =>
    movedRedirect(request) ??
    (await handleUsage(request, env)) ??
    (await handleLive(request, env)) ??
    (await handleLiveLink(request, env)) ??
    handle(request, env),
};
