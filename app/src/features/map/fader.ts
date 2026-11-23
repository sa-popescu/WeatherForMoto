// Moving from one moment of the band to the next on the map: the new picture
// fades in over the one on screen, which stays fully visible underneath until
// the fade is done, and only then goes. Nothing is ever blank in between: the
// old way swapped one layer's opacity off and the other's on, and MapLibre's
// own 300 ms paint transition then faded the new one in from nothing (a
// flicker on every move of the cursor). Without motion (the system setting)
// the swap is instant. Map-free, so it can be tested: the hooks pass in the
// few map calls it needs (weatherSlots.ts).
//
// Where the band hands over from one layer to another (the observed radar to
// the rain ahead, the satellite clouds to the clouds ahead), the two layers
// sit in different slots, the one ahead on top. A layer `under` the handover
// stays fully visible a fade longer when it hides, so the one on top fades in
// over it, and comes back at once when the band returns to it, under the one
// on top fading out. The layer on top fades in and out.

export interface LayerOps {
  exists: (id: string) => boolean;
  /** Sets the layer's opacity, fading over `ms` (0: at once). */
  setOpacity: (id: string, value: number, ms: number) => void;
  /** Puts the layer above the others of its slot, so it fades in on top of them. */
  raise: (id: string) => void;
}

export interface FaderOptions {
  /** This layer sits under the one that takes over where the band hands over (see above). */
  under?: boolean;
  /** Told when the layers left underneath are hidden, so the caller can take them off. */
  onSettled?: () => void;
}

export class Fader {
  private shown: string | null = null;
  /** Layers still visible under the one fading in, or on their way out; they go when the fade is done. */
  private readonly leaving = new Set<string>();
  private timer: ReturnType<typeof setTimeout> | null = null;
  private readonly under: boolean;
  private readonly onSettled: () => void;

  constructor(
    private readonly ops: LayerOps,
    private readonly fadeMs: number,
    options: FaderOptions = {},
  ) {
    this.under = options.under ?? false;
    this.onSettled = options.onSettled ?? (() => undefined);
  }

  /** The layer on screen, or null. */
  get current(): string | null {
    return this.shown;
  }

  /** True for a layer that must stay on the map until the fade is done. */
  isLeaving(id: string): boolean {
    return this.leaving.has(id);
  }

  /** Shows `id` at `opacity`: at once if it is already the one shown, else fading in over the one before. */
  show(id: string, opacity: number): void {
    if (this.shown === id) {
      this.ops.setOpacity(id, opacity, 0);
      return;
    }
    const previous = this.shown;
    this.shown = id;
    this.leaving.delete(id);
    if (previous !== null && this.ops.exists(previous)) this.leaving.add(previous);
    this.ops.raise(id);
    // Back from a handover, a layer under the other one is simply there again.
    const ms = previous === null && this.under ? 0 : this.fadeMs;
    this.ops.setOpacity(id, opacity, ms);
    this.settleAfter(ms);
  }

  /** Nothing to show from this layer for now: it goes, the way the handover needs (see above). */
  hide(): void {
    const leaving = this.shown;
    if (leaving === null) return;
    this.shown = null;
    if (!this.ops.exists(leaving)) return;
    this.leaving.add(leaving);
    if (!this.under) this.ops.setOpacity(leaving, 0, this.fadeMs);
    this.settleAfter(this.fadeMs);
  }

  /** Forgets everything (the layers are being taken off). */
  reset(): void {
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    this.shown = null;
    this.leaving.clear();
  }

  private settleAfter(ms: number): void {
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    if (ms <= 0) this.settle();
    else this.timer = setTimeout(() => this.settle(), ms);
  }

  private settle(): void {
    this.timer = null;
    for (const id of this.leaving) if (id !== this.shown && this.ops.exists(id)) this.ops.setOpacity(id, 0, 0);
    this.leaving.clear();
    this.onSettled();
  }
}
