import type { CORE } from '../../../i18n/core';
import type { Tier } from '../../../lib/scoring';

type CoreStrings = { [K in keyof (typeof CORE)['ro']]: string };

/** Localized tier word (IDEAL / OK / ATENȚIE / EVITĂ), "–" without a score. */
export function tierWord(tier: Tier | null, core: CoreStrings): string {
  if (!tier) return '–';
  const words: Record<Tier, string> = { ideal: core.tierIdeal, ok: core.tierOk, atentie: core.tierAtentie, evita: core.tierEvita };
  return words[tier];
}
