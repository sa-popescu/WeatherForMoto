import type { Env } from './shortLinks';
import { handle } from './shortLinks';
import { movedRedirect } from './moved';

// The Worker's entry module (wrangler.jsonc "main"). It exports the handler
// and nothing else: the Workers runtime reads every named export of this
// module as an entry point, so constants and helpers live in their own files.

export default {
  fetch: (request: Request, env: Env): Promise<Response> | Response => movedRedirect(request) ?? handle(request, env),
};
