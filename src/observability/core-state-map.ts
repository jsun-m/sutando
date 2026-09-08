/**
 * Core supervisor state transitions → obs events (pure map). The monitor
 * (src/core-input-watch.py) POSTs one `{kind:'core.state', from, to, ...}`
 * record per transition of the detached core's supervisor state; this turns it
 * into exactly one ObsEvent whose kind names the thing an operator filters on:
 *
 *   core.auth.login_required   to=logged-out, or a login gate       (denied)
 *   core.auth.recovered        login-required → running/idle-ready  (ok)
 *   core.session.crashed       to=crashed                           (error)
 *   core.session.hung          to=hung                              (error)
 *   core.session.unobserved    to=unobserved                        (ok)
 *   core.gateway.auth_rejected to=gateway-down + credential refused (denied)
 *   core.gateway.down          to=gateway-down                      (error)
 *   core.gateway.up            gateway-down → running/idle-ready    (ok)
 *   core.gate.human/known      to=blocked-human / blocked-known     (ok)
 *   core.session.running/idle  to=running / idle-ready              (ok)
 *   core.state.changed         anything else                        (ok)
 *
 * Every event carries data.{from,to,detail,gate} so `core.*` is also queryable
 * as one stream. trace_id is derived from the tmux session name, never minted,
 * so a session's transitions correlate. Pure: no clock, no I/O.
 */

import type { ObsEvent, Actor, Outcome } from './events.js';
import type { NormalizeResult } from './collector/normalizer.js';

/** Raw payload the monitor POSTs to /ingest/core-state. */
export interface RawCoreState {
	kind: 'core.state';
	session: string;
	to: string;
	from?: string | null;
	detail?: string;
	gate?: string;
	gateway_auth_rejected?: boolean;
	ts?: number;
}

export interface CoreStateMapContext {
	node: string;
	receivedAt: number;
}

export const CORE_STATE_OBS_SOURCE = 'core-supervisor';

const CORE_ACTOR: Actor = { user_id: 'core', channel: 'core-supervisor', access_tier: 'owner', tenant_id: null };

const LIVE_STATES = new Set(['running', 'idle-ready']);

function loginRequired(state: string | null | undefined, gate?: string): boolean {
	if (!state) return false;
	return state === 'logged-out' || (state.startsWith('blocked') && gate === 'login');
}

/** Kind + outcome for one transition. Order = precedence: auth first, then
 *  liveness, then gateway, then gates, then plain liveness states. */
export function coreStateKind(p: Pick<RawCoreState, 'from' | 'to' | 'gate' | 'gateway_auth_rejected'>): {
	kind: string;
	outcome: Outcome;
} {
	const { from, to, gate } = p;
	if (loginRequired(to, gate)) return { kind: 'core.auth.login_required', outcome: 'denied' };
	if (loginRequired(from, undefined) && LIVE_STATES.has(to)) return { kind: 'core.auth.recovered', outcome: 'ok' };
	if (to === 'crashed') return { kind: 'core.session.crashed', outcome: 'error' };
	if (to === 'hung') return { kind: 'core.session.hung', outcome: 'error' };
	if (to === 'unobserved') return { kind: 'core.session.unobserved', outcome: 'ok' };
	if (to === 'gateway-down') {
		return p.gateway_auth_rejected
			? { kind: 'core.gateway.auth_rejected', outcome: 'denied' }
			: { kind: 'core.gateway.down', outcome: 'error' };
	}
	if (from === 'gateway-down' && LIVE_STATES.has(to)) return { kind: 'core.gateway.up', outcome: 'ok' };
	if (to === 'blocked-human') return { kind: 'core.gate.human', outcome: 'ok' };
	if (to === 'blocked-known') return { kind: 'core.gate.known', outcome: 'ok' };
	if (to === 'running') return { kind: 'core.session.running', outcome: 'ok' };
	if (to === 'idle-ready') return { kind: 'core.session.idle', outcome: 'ok' };
	return { kind: 'core.state.changed', outcome: 'ok' };
}

function compact<T extends Record<string, unknown>>(o: T): T {
	const out: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(o)) if (v !== undefined) out[k] = v;
	return out as T;
}

export function mapCoreState(p: RawCoreState, ctx: CoreStateMapContext): NormalizeResult {
	const { kind, outcome } = coreStateKind(p);
	const ev: ObsEvent = {
		schema: 1,
		ts: p.ts ?? ctx.receivedAt,
		trace_id: `core-sess:${p.session}`,
		node: ctx.node,
		source: CORE_STATE_OBS_SOURCE,
		actor: CORE_ACTOR,
		kind,
		outcome,
		data: compact({
			from: p.from ?? null,
			to: p.to,
			detail: p.detail,
			gate: p.gate,
			gateway_auth_rejected: p.gateway_auth_rejected,
			session: p.session,
		}),
	};
	return { events: [ev], usage: [] };
}
