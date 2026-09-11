import { useId } from 'react';
import { fmt, useStrings } from '../../lib/i18n';
import type { HazardType } from '../../lib/types';
import { Chip, cx, Segmented } from '../../ui/primitives';
import { HazardGlyph } from './hazardGlyphs';
import {
  DESCRIPTION_MAX,
  DURATIONS_H,
  HAZARD_TYPES,
  hazardLabelKey,
  isDescriptionValid,
  SEVERITIES,
  severityKey,
  severityTone,
  type DurationH,
  type Severity,
} from './hazards';
import { MAP_STRINGS } from './strings';

// Fields of the hazard report: type, severity in words, description with a
// counter, and how long the report stays on the map.

export interface ReportDraft {
  type: HazardType | null;
  severity: Severity;
  description: string;
  ttl: DurationH;
}

export const EMPTY_DRAFT: ReportDraft = { type: null, severity: 3, description: '', ttl: 6 };

function toDuration(value: string): DurationH {
  return DURATIONS_H.find((hours) => String(hours) === value) ?? EMPTY_DRAFT.ttl;
}

interface Props {
  draft: ReportDraft;
  onChange: (change: Partial<ReportDraft>) => void;
  showErrors: boolean;
}

export function ReportFields({ draft, onChange, showErrors }: Props) {
  const s = useStrings(MAP_STRINGS);
  const descId = useId();
  const hintId = useId();
  const typeErrorId = useId();
  const typeMissing = showErrors && draft.type === null;
  const descBad = showErrors && !isDescriptionValid(draft.description);

  return (
    <>
      <fieldset className="map-fieldset" aria-describedby={typeMissing ? typeErrorId : undefined}>
        <legend className="field__label">{s.reportType}</legend>
        <div className="map-choices">
          {HAZARD_TYPES.map((type) => (
            <Chip key={type} selected={draft.type === type} className="map-choice" onClick={() => onChange({ type })}>
              <HazardGlyph type={type} size={20} />
              {s[hazardLabelKey(type)]}
            </Chip>
          ))}
        </div>
        {typeMissing && (
          <span id={typeErrorId} className="field__hint field__hint--error">
            {s.reportTypeMissing}
          </span>
        )}
      </fieldset>

      <fieldset className="map-fieldset">
        <legend className="field__label">{s.severity}</legend>
        <div className="map-choices">
          {SEVERITIES.map((level) => (
            <Chip key={level} selected={draft.severity === level} className="map-choice" onClick={() => onChange({ severity: level })}>
              <span className={cx('map-choice__dot', `map-tone--${severityTone(level)}`)} aria-hidden="true" />
              {s[severityKey(level)]}
            </Chip>
          ))}
        </div>
      </fieldset>

      <div className={cx('field', descBad && 'field--error')}>
        <label className="field__label" htmlFor={descId}>
          {s.reportDesc}
        </label>
        <textarea
          id={descId}
          className="field__input map-textarea"
          rows={3}
          maxLength={DESCRIPTION_MAX}
          value={draft.description}
          aria-invalid={descBad || undefined}
          aria-describedby={hintId}
          onChange={(event) => onChange({ description: event.target.value })}
        />
        <div className="map-desc-foot">
          <span id={hintId} className={cx('field__hint', descBad && 'field__hint--error')}>
            {descBad ? s.reportDescShort : s.reportDescHint}
          </span>
          <span className={cx('map-counter num', draft.description.length > DESCRIPTION_MAX - 20 && 'map-counter--near')}>
            {draft.description.length}/{DESCRIPTION_MAX}
          </span>
        </div>
      </div>

      <div className="field">
        <span className="field__label" aria-hidden="true">
          {s.reportDuration}
        </span>
        <Segmented
          label={s.reportDuration}
          options={DURATIONS_H.map((hours) => ({ value: String(hours), label: fmt(s.reportHours, { n: hours }) }))}
          value={String(draft.ttl)}
          onChange={(value) => onChange({ ttl: toDuration(value) })}
        />
      </div>
    </>
  );
}
