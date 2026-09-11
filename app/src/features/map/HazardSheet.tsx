import { fmtNumber } from '../../lib/format';
import { distanceKm } from '../../lib/geo';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import type { Hazard } from '../../lib/types';
import { usePlace } from '../../state/place';
import { cx } from '../../ui/primitives';
import { Sheet } from '../../ui/Sheet';
import { HazardGlyph } from './hazardGlyphs';
import { hazardLabelKey, hazardTypeOf, parseApiTime, severityKey, severityTone } from './hazards';
import { MAP_STRINGS } from './strings';
import { agoText, clockText, inText } from './texts';

// Details of one reported hazard. The description is user text, rendered as
// plain text only (React escapes it).

interface Props {
  hazard: Hazard | null;
  onClose: () => void;
}

export function HazardSheet({ hazard, onClose }: Props) {
  const s = useStrings(MAP_STRINGS);
  const lang = useLang();
  const { place } = usePlace();
  if (!hazard) return null;

  const type = hazardTypeOf(hazard.hazard_type);
  const tone = severityTone(hazard.severity);
  const now = Date.now();
  const created = parseApiTime(hazard.created_at);
  const expires = parseApiTime(hazard.expires_at);
  const km = distanceKm(place, hazard);

  const reported = Number.isFinite(created) ? agoText(s, now - created) : '–';
  let expiry = '–';
  if (Number.isFinite(expires)) {
    expiry = expires <= now ? s.expired : fmt(s.atTime, { rel: inText(s, expires - now), time: clockText(expires, lang) });
  }

  return (
    <Sheet open onClose={onClose} title={s[hazardLabelKey(type)]}>
      <div className="map-hz">
        <span className={cx('map-hz__badge', `map-tone--${tone}`)}>
          <HazardGlyph type={type} size={28} />
        </span>
        <div className="map-hz__sev">
          <span className="eyebrow">{s.severity}</span>
          <span className={cx('map-hz__sev-word num', `map-tone--${tone}`)}>{s[severityKey(hazard.severity)]}</span>
        </div>
      </div>
      <p className="map-hz__desc">{hazard.description}</p>
      <dl className="map-facts">
        <div className="map-facts__row">
          <dt>{s.factDistance}</dt>
          <dd>{fmt(s.distanceFrom, { km: fmtNumber(km, lang, km < 10 ? 1 : 0), place: place.name })}</dd>
        </div>
        <div className="map-facts__row">
          <dt>{s.factReported}</dt>
          <dd>{reported}</dd>
        </div>
        <div className="map-facts__row">
          <dt>{s.factExpires}</dt>
          <dd>{expiry}</dd>
        </div>
      </dl>
    </Sheet>
  );
}
