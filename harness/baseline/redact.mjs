// redact.mjs — the ONE place a manifest is scrubbed before it can reach a
// public repo. collect-manifest.mjs applies it at write time; test/redact.test.mjs
// proves it offline. Council asked for the test after the 2026-09-10 baseline
// was rewritten in place post-verification (Aster 70d3fd97): the rule now
// lives where a test can reach it, not inline in the collector.
//
// What leaves a manifest:   <ws>       the workspace root
//                           <home>     any other /Users/<x>, /home/<x>, /c/Users/<x>
//                           sha256[0:12] of a hostname, never the hostname
// What is NOT redacted, on purpose: ssh aliases (air, m1, …), droplet IPs and
// /opt paths (already public in axona-docs), key FILENAMES (not key material).
import { createHash } from 'node:crypto';

const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Build a redactor bound to a workspace root. */
export function makeRedactor(ws) {
  const wsRe = new RegExp(esc(ws), 'g');
  return (s) => String(s).replace(wsRe, '<ws>').replace(/(\/Users|\/home|\/c\/Users)\/[^/\s'"]+/g, '<home>');
}

/** A hostname is reported as 12 hex chars of its sha256 — enough to tell two
 *  collectors apart in a diff, not enough to name a machine. */
export const hostTag = (hostname) => createHash('sha256').update(hostname).digest('hex').slice(0, 12);

/** Names that must never appear in a public artifact. Used by the test to scan
 *  real output; extend it when a new host or account joins the fleet. */
export const FORBIDDEN = [/\/Users\/[a-z]/i, /\/home\/[a-z]/i, /\/c\/Users\/[a-z]/i, /MacBook-Pro/i, /\.local\b/];
export const scan = (text) => FORBIDDEN.filter((re) => re.test(text)).map(String);
