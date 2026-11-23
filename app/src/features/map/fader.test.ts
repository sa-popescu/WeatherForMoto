import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Fader, type LayerOps } from './fader';

/** A pretend map: each layer's opacity and how it got there, and the stacking order. */
function fakeMap(ids: string[]) {
  const opacity = new Map(ids.map((id) => [id, 0]));
  const lastFadeMs = new Map<string, number>();
  const order = [...ids];
  const ops: LayerOps = {
    exists: (id) => opacity.has(id),
    setOpacity: (id, value, ms) => {
      opacity.set(id, value);
      lastFadeMs.set(id, ms);
    },
    raise: (id) => {
      order.splice(order.indexOf(id), 1);
      order.push(id);
    },
  };
  return { ops, opacity, lastFadeMs, order };
}

describe('moving between pictures on the map', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('fades the new picture in on top while the old one stays, then takes the old one away', () => {
    const map = fakeMap(['a', 'b']);
    const settled = vi.fn();
    const fader = new Fader(map.ops, 250, { onSettled: settled });
    fader.show('a', 0.7);
    vi.advanceTimersByTime(250);

    fader.show('b', 0.7);
    // Mid-fade: the old picture is still fully there, under the new one.
    expect(map.opacity.get('a')).toBe(0.7);
    expect(map.opacity.get('b')).toBe(0.7);
    expect(map.lastFadeMs.get('b')).toBe(250);
    expect(map.order.at(-1)).toBe('b');
    expect(fader.isLeaving('a')).toBe(true);

    vi.advanceTimersByTime(250);
    expect(map.opacity.get('a')).toBe(0);
    expect(map.lastFadeMs.get('a')).toBe(0);
    expect(fader.isLeaving('a')).toBe(false);
    expect(settled).toHaveBeenCalled();
  });

  it('keeps every picture passed on the way until the last one has faded in', () => {
    const map = fakeMap(['a', 'b', 'c']);
    const fader = new Fader(map.ops, 250);
    fader.show('a', 0.7);
    fader.show('b', 0.7);
    vi.advanceTimersByTime(100);
    fader.show('c', 0.7);
    vi.advanceTimersByTime(200);
    // The first timer was replaced: nothing went blank before c was in.
    expect([...map.opacity.values()].filter((v) => v > 0).length).toBeGreaterThan(1);
    vi.advanceTimersByTime(50);
    expect(map.opacity.get('a')).toBe(0);
    expect(map.opacity.get('b')).toBe(0);
    expect(map.opacity.get('c')).toBe(0.7);
  });

  it('changes the opacity of the picture on screen at once, and swaps at once without motion', () => {
    const map = fakeMap(['a', 'b']);
    const fader = new Fader(map.ops, 250);
    fader.show('a', 0.7);
    fader.show('a', 0.4);
    expect(map.opacity.get('a')).toBe(0.4);
    expect(map.lastFadeMs.get('a')).toBe(0);

    const still = fakeMap(['a', 'b']);
    const instant = new Fader(still.ops, 0);
    instant.show('a', 0.7);
    instant.show('b', 0.7);
    expect(still.opacity.get('a')).toBe(0);
    expect(still.opacity.get('b')).toBe(0.7);
  });

  it('hands over at the band\'s boundary without a gap: the layer below waits, the one on top fades', () => {
    // The observed radar (below) hides: it stays fully on while the rain ahead fades in over it.
    const radar = fakeMap(['r']);
    const below = new Fader(radar.ops, 250, { under: true });
    below.show('r', 0.7);
    vi.advanceTimersByTime(250);
    below.hide();
    expect(radar.opacity.get('r')).toBe(0.7);
    vi.advanceTimersByTime(250);
    expect(radar.opacity.get('r')).toBe(0);
    // Back to it: there at once, under the rain ahead fading out.
    below.show('r', 0.7);
    expect(radar.lastFadeMs.get('r')).toBe(0);

    // The rain ahead (on top) fades out over the radar.
    const ahead = fakeMap(['f']);
    const above = new Fader(ahead.ops, 250);
    above.show('f', 0.7);
    vi.advanceTimersByTime(250);
    above.hide();
    expect(ahead.opacity.get('f')).toBe(0);
    expect(ahead.lastFadeMs.get('f')).toBe(250);
    expect(above.isLeaving('f')).toBe(true);
  });
});
