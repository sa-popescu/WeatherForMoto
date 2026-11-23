import { fmt } from '../../lib/i18n';
import type { Avoidable, Level, RouteOptions, RouteType } from './routeOptions';
import type { RouteOptionsText } from './routeOptionsStrings';

// The words for the route options: the name of each choice, and the one line
// that sums them up on the buttons that open the sheet. Pure, so it is tested.

type Key = keyof RouteOptionsText;

const TYPE_KEY: Record<RouteType, Key> = {
  fastest: 'typeFastest',
  short: 'typeShort',
  shortest: 'typeShortest',
  eco: 'typeEco',
  thrilling: 'typeThrilling',
};

const AVOID_KEY: Record<Avoidable, Key> = {
  motorways: 'avoidMotorways',
  tollRoads: 'avoidTollRoads',
  ferries: 'avoidFerries',
  unpavedRoads: 'avoidUnpavedRoads',
  tunnels: 'avoidTunnels',
  borderCrossings: 'avoidBorderCrossings',
};

const AVOID_SHORT_KEY: Record<Avoidable, Key> = {
  motorways: 'sumMotorways',
  tollRoads: 'sumTollRoads',
  ferries: 'sumFerries',
  unpavedRoads: 'sumUnpavedRoads',
  tunnels: 'sumTunnels',
  borderCrossings: 'sumBorderCrossings',
};

const CURVES_KEY: Record<Level, Key> = { low: 'curvesLow', normal: 'curvesNormal', high: 'curvesHigh' };
const HILLS_KEY: Record<Level, Key> = { low: 'hillsLow', normal: 'hillsNormal', high: 'hillsHigh' };

export const typeName = (type: RouteType, s: RouteOptionsText): string => s[TYPE_KEY[type]];
export const typeDescription = (type: RouteType, s: RouteOptionsText): string => s[`${TYPE_KEY[type]}Desc` as Key];
export const avoidName = (what: Avoidable, s: RouteOptionsText): string => s[AVOID_KEY[what]];
export const curvesName = (level: Level, s: RouteOptionsText): string => s[CURVES_KEY[level]];
export const hillsName = (level: Level, s: RouteOptionsText): string => s[HILLS_KEY[level]];

/** "Cel mai rapid · fără neasfaltate", "Cu curbe · curbe: multe · fără autostrăzi, taxe · max 45 km/h". */
export function optionsSummary(options: RouteOptions, s: RouteOptionsText): string {
  const parts = [typeName(options.type, s)];
  if (options.type === 'thrilling' && options.windingness !== 'normal') {
    parts.push(fmt(s.sumCurves, { level: curvesName(options.windingness, s).toLowerCase() }));
  }
  if (options.avoid.length > 0) parts.push(fmt(s.sumWithout, { what: options.avoid.map((a) => s[AVOID_SHORT_KEY[a]]).join(', ') }));
  if (options.topSpeedKmh !== null) parts.push(fmt(s.sumSpeed, { v: options.topSpeedKmh }));
  return parts.join(' · ');
}
