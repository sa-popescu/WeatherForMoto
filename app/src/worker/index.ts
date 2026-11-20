import { handle } from './shortLinks';

// The Worker's entry module (wrangler.jsonc "main"). It exports the handler
// and nothing else: the Workers runtime reads every named export of this
// module as an entry point, so constants and helpers live in shortLinks.ts.

export default { fetch: handle };
