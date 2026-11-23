import { expect, test } from '@playwright/test';

// The anonymous usage counts (src/lib/usage.ts): the built app collects the
// parts a visit used and sends them in one message when the page is hidden,
// and nothing when the browser asks not to be tracked. The Worker's side is
// covered by src/worker/usage.test.ts.

/** What the app reported, with every outside request refused. */
async function reports(page: import('@playwright/test').Page): Promise<string[]> {
  const bodies: string[] = [];
  await page.route('**/*', async (route) => {
    const url = new URL(route.request().url());
    if (url.hostname !== '127.0.0.1') return route.abort();
    if (url.pathname === '/usage') {
      bodies.push(route.request().postData() ?? '');
      return route.fulfill({ status: 204 });
    }
    return route.continue();
  });
  return bodies;
}

async function hide(page: import('@playwright/test').Page): Promise<void> {
  await page.evaluate(() => {
    Object.defineProperty(document, 'visibilityState', { value: 'hidden', configurable: true });
    document.dispatchEvent(new Event('visibilitychange'));
  });
}

test('a visit reports the views it opened, once, when the page is hidden', async ({ page }, info) => {
  test.skip(info.project.name !== 'desktop', 'one size is enough');
  const bodies = await reports(page);
  await page.goto('/#/acum');
  await page.goto('/#/ture');
  await page.locator('.bottom-nav').waitFor();
  expect(bodies).toHaveLength(0);
  await hide(page);
  await expect.poll(() => bodies.length).toBe(1);
  const sent = (JSON.parse(bodies[0]) as { e: string[] }).e;
  expect(sent).toEqual(expect.arrayContaining(['visit', 'view:acum', 'view:ture']));
  // Hidden again with nothing new: nothing more to send.
  await hide(page);
  await page.waitForTimeout(300);
  expect(bodies).toHaveLength(1);
});

test('a browser that asks not to be tracked sends nothing', async ({ page }, info) => {
  test.skip(info.project.name !== 'desktop', 'one size is enough');
  await page.addInitScript(() => Object.defineProperty(navigator, 'globalPrivacyControl', { value: true }));
  const bodies = await reports(page);
  await page.goto('/#/drum');
  await page.locator('.bottom-nav').waitFor();
  await hide(page);
  await page.waitForTimeout(300);
  expect(bodies).toHaveLength(0);
});
