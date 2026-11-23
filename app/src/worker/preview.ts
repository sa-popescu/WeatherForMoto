// What a chat app shows for a shared link (WhatsApp, Telegram, Signal,
// iMessage, Facebook, Slack, Discord...). Their preview robots read the
// page's Open Graph tags and never run the app, and a link to the app's hash
// routes (/#/...) shows them only the home page. So the Worker answers those
// robots itself on the two shared paths: a short route link (/s/<id>) gets the
// route's ends and departure, a live location link (/l/<id>) the ride's
// destination and arrival. People get the same redirect as before.
// Nothing here is stored or logged.

import type { LiveState } from './live';

export type PreviewLang = 'ro' | 'en';

export interface Preview {
  title: string;
  description: string;
  /** Where the page sends whoever opens it, absolute. */
  target: string;
}

/** The robots that fetch a link to show its preview. Signal's sends WhatsApp's name. */
const PREVIEW_ROBOTS =
  /facebookexternalhit|Facebot|Twitterbot|WhatsApp|TelegramBot|Slackbot|Discordbot|LinkedInBot|SkypeUriPreview|redditbot|Applebot|Embedly|Pinterest|vkShare|Viber|Mastodon|Iframely|Googlebot|bingbot/i;

export function isPreviewRobot(request: Request): boolean {
  return PREVIEW_ROBOTS.test(request.headers.get('User-Agent') ?? '');
}

const TEXT = {
  ro: {
    siteTitle: 'Motonor: vremea și drumul pentru motocicliști',
    routeTitle: 'Traseu {from} → {to}',
    routeTitleSome: 'Traseu cu {n} opriri',
    routeStops: ', cu {n} opriri pe drum',
    routeStop: ', cu o oprire pe drum',
    routeLead: 'Vremea pe tot drumul, la ora la care treci: scor moto, ploaie și rafale în fiecare punct.',
    routeDeparture: 'Plecare {when}.',
    routeGone: 'Traseu expirat',
    routeGoneLead: 'Linkul scurt a expirat după 90 de zile. Motonor: vremea și drumul pentru motocicliști.',
    liveTitle: 'Tură în direct',
    liveTitleTo: 'Tură în direct spre {dest}',
    liveLead: 'Vezi pe hartă unde e motociclistul, actualizat la 30 de secunde. Linkul se închide la sfârșitul turei.',
    liveEta: 'Sosire estimată la {time}.',
    liveGone: 'Tura s-a încheiat',
    liveGoneLead: 'Locația live nu mai e disponibilă: tura s-a încheiat sau linkul a expirat.',
    open: 'Deschide în Motonor',
    months: ['ian', 'feb', 'mar', 'apr', 'mai', 'iun', 'iul', 'aug', 'sep', 'oct', 'nov', 'dec'],
    locale: 'ro_RO',
  },
  en: {
    siteTitle: 'Motonor: weather and roads for motorcyclists',
    routeTitle: 'Route {from} → {to}',
    routeTitleSome: 'Route with {n} stops',
    routeStops: ', with {n} stops on the way',
    routeStop: ', with one stop on the way',
    routeLead: 'The weather all along the road, at the time you pass: moto score, rain and gusts at every point.',
    routeDeparture: 'Leaving {when}.',
    routeGone: 'Route expired',
    routeGoneLead: 'The short link expired after 90 days. Motonor: weather and roads for motorcyclists.',
    liveTitle: 'Live ride',
    liveTitleTo: 'Live ride to {dest}',
    liveLead: 'See on the map where the rider is, updated every 30 seconds. The link closes when the ride ends.',
    liveEta: 'Estimated arrival at {time}.',
    liveGone: 'The ride has ended',
    liveGoneLead: 'The live location is no longer available: the ride ended or the link expired.',
    open: 'Open in Motonor',
    months: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    locale: 'en_GB',
  },
} as const;

function fill(template: string, values: Record<string, string | number>): string {
  return template.replace(/\{(\w+)\}/g, (_, key: string) => String(values[key] ?? ''));
}

function escapeHtml(text: string): string {
  return text.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c] ?? c);
}

/** A name from a link, kept to a sensible length. */
function clip(text: string, max = 60): string {
  const clean = text.replace(/\s+/g, ' ').trim();
  return clean.length > max ? `${clean.slice(0, max - 1)}…` : clean;
}

/** "2026-11-22T08:00" (local time, as routeLink.ts writes it) as "22 nov, 08:00". */
function departureText(value: string | null, lang: PreviewLang): string | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/.exec(value ?? '');
  if (!match) return null;
  const month = TEXT[lang].months[Number(match[2]) - 1];
  return month ? `${Number(match[3])} ${month}, ${match[4]}:${match[5]}` : null;
}

/** A route link's query (routeLink.ts): its ends and departure, or the expired link's text. */
export function routePreview(query: string, target: string, lang: PreviewLang = 'ro'): Preview {
  const t = TEXT[lang];
  const params = new URLSearchParams(query);
  if (params.get('r') !== '1') return { title: t.routeGone, description: t.routeGoneLead, target };
  // Each stop is "lat~lon~name"; a stop taken from the GPS may have no name.
  const names = params.getAll('p').map((stop) => clip(stop.split('~')[2] ?? ''));
  const from = names[0];
  const to = names[names.length - 1];
  const between = names.length - 2;
  let title: string;
  if (names.length >= 2 && from && to) {
    title = fill(t.routeTitle, { from, to }) + (between === 1 ? t.routeStop : between > 1 ? fill(t.routeStops, { n: between }) : '');
  } else {
    title = fill(t.routeTitleSome, { n: names.length });
  }
  const when = departureText(params.get('t'), lang);
  const description = when ? `${t.routeLead} ${fill(t.routeDeparture, { when })}` : t.routeLead;
  return { title, description, target };
}

/** A shared ride (live.ts): where to and when, or that it is over. */
export function livePreview(state: LiveState | null, target: string, lang: PreviewLang = 'ro', now = Date.now()): Preview {
  const t = TEXT[lang];
  if (!state || state.expiresAt <= now) return { title: t.liveGone, description: t.liveGoneLead, target };
  const dest = state.pos?.dest ? clip(state.pos.dest) : null;
  const title = dest ? fill(t.liveTitleTo, { dest }) : t.liveTitle;
  let description: string = t.liveLead;
  const eta = state.pos?.etaMs;
  if (eta && eta > now) {
    const time = new Intl.DateTimeFormat(lang === 'en' ? 'en-GB' : 'ro-RO', {
      hour: '2-digit',
      minute: '2-digit',
      timeZone: 'Europe/Bucharest',
    }).format(eta);
    description = `${fill(t.liveEta, { time })} ${t.liveLead}`;
  }
  return { title, description, target };
}

/**
 * The page the robots read: the preview's tags, and a redirect for anyone else
 * who lands on it. `shared` is the link as it was sent, for og:url.
 */
export function previewPage(preview: Preview, shared: URL, lang: PreviewLang = 'ro'): Response {
  const t = TEXT[lang];
  const origin = shared.origin;
  const title = escapeHtml(preview.title);
  const description = escapeHtml(preview.description);
  const target = escapeHtml(preview.target);
  const canonical = escapeHtml(`${origin}${shared.pathname}`);
  const image = `${origin}/og-image.jpg`;
  const html = `<!doctype html>
<html lang="${lang}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title}</title>
<meta name="description" content="${description}">
<meta name="robots" content="noindex">
<meta property="og:type" content="website">
<meta property="og:site_name" content="Motonor">
<meta property="og:locale" content="${t.locale}">
<meta property="og:title" content="${title}">
<meta property="og:description" content="${description}">
<meta property="og:url" content="${canonical}">
<meta property="og:image" content="${image}">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:image:alt" content="${escapeHtml(t.siteTitle)}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${title}">
<meta name="twitter:description" content="${description}">
<meta name="twitter:image" content="${image}">
<meta http-equiv="refresh" content="0; url=${target}">
</head>
<body><p><a href="${target}">${escapeHtml(t.open)}</a></p></body>
</html>
`;
  return new Response(html, {
    status: 200,
    headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex' },
  });
}

/** The language a shared link asks for: "?lang=en" from an app set to English. */
export function previewLang(url: URL): PreviewLang {
  return url.searchParams.get('lang') === 'en' ? 'en' : 'ro';
}
