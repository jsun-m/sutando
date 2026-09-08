/**
 * Core supervisor state → obs events, as a composable collector Normalizer.
 * The monitor (src/core-input-watch.py) POSTs `{kind:'core.state', from, to,
 * detail, gate, session}` to `/ingest/core-state` on every supervisor state
 * transition; this validates the shape and hands it to the pure mapCoreState,
 * which names the transition (`core.auth.login_required`, `core.session.crashed`,
 * `core.gateway.down`, ...). Registered on the same collector as the CC and
 * realtime sources (boot.ts). Produces events only — no usage records.
 */

import { AbstractNormalizer, type NormalizeContext, type NormalizeResult } from './collector/normalizer.js';
import { mapCoreState, type RawCoreState } from './core-state-map.js';

export const CORE_STATE_SOURCE = 'core-state';

export class CoreStateNormalizer extends AbstractNormalizer<RawCoreState> {
	readonly source = CORE_STATE_SOURCE;

	/** Accept only a well-formed transition; anything else is dropped, never thrown. */
	decode(payload: unknown): RawCoreState | null {
		if (!payload || typeof payload !== 'object') return null;
		const p = payload as Record<string, unknown>;
		if (p.kind !== 'core.state') return null;
		if (typeof p.to !== 'string' || !p.to) return null;
		if (typeof p.session !== 'string' || !p.session) return null;
		if (p.from !== undefined && p.from !== null && typeof p.from !== 'string') return null;
		if (p.ts !== undefined && typeof p.ts !== 'number') return null;
		return p as unknown as RawCoreState;
	}

	map(p: RawCoreState, ctx: NormalizeContext): NormalizeResult {
		return mapCoreState(p, { node: ctx.node, receivedAt: ctx.receivedAt });
	}
}
