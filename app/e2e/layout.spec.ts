import { expect, test, type Page } from '@playwright/test';
import { fileURLToPath } from 'node:url';

// Every tab, at every size in playwright.config.ts, on the same weather (a
// real answer of the API, saved in fixtures/), at the moment it was fetched.
// Nothing leaves the test: the API answers from the fixtures, and every
// other outside request (map tiles, radar, geocoding) is refused, so a map
// stays empty but the layout around it is the real one.

const FIXTURES = fileURLToPath(new URL('./fixtures/', import.meta.url));
/** When fixtures/weather.json was fetched: 23 November 2026, 12:00 in Bucharest. */
const FIXTURE_TIME = new Date('2026-11-23T10:00:00Z');

const SCREENS = ['drum', 'traseu', 'acum', 'harta', 'ture', 'eu'] as const;

async function prepare(page: Page): Promise<void> {
  await page.clock.setFixedTime(FIXTURE_TIME);
  await page.addInitScript(() => {
    // The install invitation is checked on its own below; here it stays away.
    localStorage.setItem('motonorInstallInviteV1', JSON.stringify({ shown: 2, lastAt: Date.now() }));
  });
  await page.route('**/*', async (route) => {
    const url = new URL(route.request().url());
    if (url.hostname === '127.0.0.1') return route.continue();
    if (url.hostname.endsWith('.run.app')) {
      if (url.pathname === '/weather') return route.fulfill({ path: `${FIXTURES}weather.json`, contentType: 'application/json' });
      if (url.pathname === '/meta/scoring') return route.fulfill({ path: `${FIXTURES}scoring.json`, contentType: 'application/json' });
      return route.fulfill({ status: 503, contentType: 'application/json', body: '{"detail":"not in the layout checks"}' });
    }
    return route.abort();
  });
}

interface Problem {
  kind: string;
  detail: string;
}

/**
 * What a person would see as broken: text off the screen, text over other
 * text, the page scrolling sideways, and in the wide layout a row that hides
 * part of itself. Only what shows counts: text clipped on purpose (a scrolling
 * row on a phone, an ellipsis) by the part that shows, and every area that
 * scrolls up and down is checked at each step down to its end, the way a
 * person scrolling it would see it. `scope` keeps the text checks to one card
 * that floats over the rest on purpose.
 */
async function layoutProblems(page: Page, scope?: string): Promise<Problem[]> {
  return page.evaluate((scope) => {
    const found = new Map<string, { kind: string; detail: string }>();
    const report = (kind: string, detail: string): void => {
      found.set(`${kind}|${detail}`, { kind, detail });
    };
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const wide = window.matchMedia('(min-width: 1024px), (orientation: landscape) and (max-height: 500px) and (min-width: 600px)').matches;

    type Box = { left: number; top: number; right: number; bottom: number };
    const intersect = (a: Box, b: Box): Box | null => {
      const box = { left: Math.max(a.left, b.left), top: Math.max(a.top, b.top), right: Math.min(a.right, b.right), bottom: Math.min(a.bottom, b.bottom) };
      return box.right - box.left > 0.5 && box.bottom - box.top > 0.5 ? box : null;
    };
    const describe = (el: Element): string => {
      const cls = typeof el.className === 'string' && el.className ? `.${el.className.trim().split(/\s+/).join('.')}` : '';
      return `${el.tagName.toLowerCase()}${cls}`;
    };

    const scrolls = (value: string): boolean => value === 'auto' || value === 'scroll';
    /** The part of a box that shows: inside the screen and inside every ancestor that clips. */
    const shown = (el: Element, box: Box): Box | null => {
      let visible: Box | null = intersect(box, { left: 0, top: 0, right: vw, bottom: vh });
      for (let parent = el.parentElement; parent && visible; parent = parent.parentElement) {
        const style = getComputedStyle(parent);
        if (style.overflowX !== 'visible' || style.overflowY !== 'visible') visible = intersect(visible, parent.getBoundingClientRect());
      }
      return visible;
    };

    /** The checks on what shows right now. */
    const check = (): void => {
      // Every piece of text on screen, as the boxes of its lines.
      const texts: Array<{ el: Element; box: Box; full: Box; text: string }> = [];
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
      while (walker.nextNode()) {
        const node = walker.currentNode;
        const text = (node.textContent ?? '').trim();
        const el = node.parentElement;
        if (!text || !el) continue;
        if (scope && !el.closest(scope)) continue;
        if (el.closest('.maplibregl-map, svg, script, style, noscript, .visually-hidden, [hidden]')) continue;
        const style = getComputedStyle(el);
        if (style.visibility === 'hidden' || Number(style.opacity) === 0) continue;
        const range = document.createRange();
        range.selectNodeContents(node);
        for (const rect of Array.from(range.getClientRects())) {
          if (rect.width < 1 || rect.height < 1) continue;
          const full = { left: rect.left, top: rect.top, right: rect.right, bottom: rect.bottom };
          const box = shown(el, full);
          if (box) texts.push({ el, box, full, text: text.slice(0, 40) });
        }
      }

      // Text cut by the screen's edge (not by a container that clips on purpose).
      for (const t of texts) {
        if (t.full.right > vw + 1 && t.box.right >= vw - 1) report('text off the screen', `"${t.text}" in ${describe(t.el)}`);
      }

      // Text over other text. A line's box is taller than its letters (the
      // font's leading), so two lines stacked close together touch boxes
      // without touching letters: it takes a third of the shorter line.
      for (let i = 0; i < texts.length; i += 1) {
        for (let j = i + 1; j < texts.length; j += 1) {
          const a = texts[i];
          const b = texts[j];
          if (a.el === b.el || a.el.contains(b.el) || b.el.contains(a.el)) continue;
          const both = intersect(a.box, b.box);
          const shorter = Math.min(a.box.bottom - a.box.top, b.box.bottom - b.box.top);
          if (both && both.right - both.left > 2 && both.bottom - both.top > shorter / 3) {
            report('text over text', `"${a.text}" (${describe(a.el)}) and "${b.text}" (${describe(b.el)})`);
          }
        }
      }
    };

    if (document.documentElement.scrollWidth > vw + 1) {
      report('page wider than the screen', `${document.documentElement.scrollWidth} px > ${vw} px`);
    }

    // At the top, then down every area that scrolls, a screen at a time.
    check();
    const scrollers = Array.from(document.querySelectorAll('body *')).filter((el): el is HTMLElement => {
      if (!(el instanceof HTMLElement) || el.closest('[hidden], .maplibregl-map')) return false;
      return scrolls(getComputedStyle(el).overflowY) && el.scrollHeight > el.clientHeight + 1 && el.clientHeight > 0;
    });
    for (const scroller of scrollers) {
      const step = Math.max(80, Math.floor(scroller.clientHeight * 0.8));
      const end = scroller.scrollHeight - scroller.clientHeight;
      for (let y = step; y < end + step; y += step) {
        scroller.scrollTop = Math.min(y, end);
        check();
      }
      scroller.scrollTop = 0;
    }

    // In the wide layout a mouse cannot swipe: a row that scrolls sideways hides part of itself.
    if (wide && !scope) {
      for (const el of Array.from(document.querySelectorAll('body *'))) {
        const style = getComputedStyle(el);
        if (!scrolls(style.overflowX)) continue;
        if (el.closest('[hidden], .maplibregl-map, .table-wrap')) continue;
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) continue;
        if (el.scrollWidth > el.clientWidth + 1) {
          report('row cut off in the wide layout', `${describe(el)}: ${el.scrollWidth} px in ${el.clientWidth} px`);
        }
      }
    }
    return [...found.values()];
  }, scope);
}

for (const screen of SCREENS) {
  test(`#/${screen} keeps its text on screen and apart`, async ({ page }) => {
    const errors: string[] = [];
    page.on('pageerror', (err) => errors.push(err.message));
    await prepare(page);
    await page.goto(`/#/${screen}`);
    await page.locator('.bottom-nav').waitFor();
    // The forecast, once it is there, is what most of the screens are made of.
    if (screen === 'acum') await page.locator('.now-days').first().waitFor();
    await page.waitForTimeout(800);
    expect(await layoutProblems(page)).toEqual([]);
    expect(errors).toEqual([]);
  });
}

test('the forecast, all 14 days and the day hours, stays in place', async ({ page }) => {
  await prepare(page);
  await page.goto('/#/acum');
  await page.locator('.now-days').first().waitFor();
  const more = page.getByRole('button', { name: /14 zile/i });
  if (await more.count()) await more.first().click();
  await page.waitForTimeout(500);
  expect(await layoutProblems(page)).toEqual([]);
});

test('the install invitation fits the screen', async ({ page }) => {
  await prepare(page);
  await page.addInitScript(() => localStorage.removeItem('motonorInstallInviteV1'));
  await page.goto('/#/drum');
  await page.locator('.install-invite').waitFor({ timeout: 6_000 });
  // It floats over the screen on purpose: only its own text is checked.
  expect(await layoutProblems(page, '.install-invite')).toEqual([]);
});
