import { CORE } from '../../../i18n/core';
import { useStrings } from '../../../lib/i18n';
import { Skeleton } from '../../../ui/primitives';

/** First-load placeholder with the same rhythm as the real screen. */
export function NowSkeleton() {
  const core = useStrings(CORE);
  return (
    <div className="now-skeleton" aria-busy="true">
      <p className="visually-hidden" role="status">
        {core.loading}
      </p>
      <div className="now-skeleton__hero">
        <Skeleton width={120} height={80} radius={18} />
        <Skeleton width="40%" height={18} />
        <Skeleton width="70%" height={28} />
        <Skeleton width="min(340px, 100%)" height={44} radius={999} />
      </div>
      <Skeleton height={262} radius={22} />
      <Skeleton height={236} radius={22} />
      <div className="now-skeleton__row">
        <Skeleton height={112} radius={18} />
        <Skeleton height={112} radius={18} />
        <Skeleton height={112} radius={18} />
      </div>
    </div>
  );
}
