// smoke_std_message.mjs — the canonical pub/sub message convention (std/message).
// Every reference app publishes makeMessage() and renders readMessage(); this
// pins the contract so cross-app interop can't silently drift back to
// "[object Object]".
import { makeMessage, readMessage, readSender, MESSAGE_FORMAT } from '../std/message.js';

let passed = 0, failed = 0;
const check = (l, c, x = '') => { if (c) { console.log(`  ✓ ${l}`); passed++; } else { console.log(`  ✗ ${l} ${x}`); failed++; } };

console.log('std/message — canonical pub/sub message convention');

// makeMessage
const m = makeMessage('hello, world', { node: 'abcd1234' });
check('makeMessage stamps the format version', m.v === MESSAGE_FORMAT);
check('makeMessage carries text', m.text === 'hello, world');
check('makeMessage merges app extras', m.node === 'abcd1234');
check('makeMessage coerces non-string text', makeMessage(42).text === '42');
check('makeMessage handles null', makeMessage(null).text === '');

// readMessage — tolerant across every shape an app might receive
check('reads canonical { v, text }',        readMessage(makeMessage('hi')) === 'hi');
check('reads a bare string (legacy/demo)',  readMessage('plain string') === 'plain string');
check('reads a { message } object',         readMessage({ message: 'via message field' }) === 'via message field');
check('reads { text } without v',           readMessage({ text: 'body' }) === 'body');
check('arbitrary object → JSON, never [object Object]', (() => {
  const out = readMessage({ a: 1, b: 2 });
  return out !== '[object Object]' && out.includes('"a"');
})());
check('null/undefined → empty string',      readMessage(null) === '' && readMessage(undefined) === '');
check('number → string',                    readMessage(7) === '7');

// the exact bug this convention prevents: minimal's object rendered by another app
check('minimal {text,node} renders as the text (not [object Object])',
  readMessage(makeMessage('Hello from minimal', { node: 'ff00' })) === 'Hello from minimal');

// readSender
check('readSender prefers the authenticated signerPubkey',
  readSender({ signerPubkey: 'deadbeefcafe', message: makeMessage('x') }, 8) === 'deadbeef');
check('readSender falls back to a body node hint',
  readSender({ message: { text: 'x', node: 'abcd1234ef' } }, 8) === 'abcd1234');
check('readSender → (unknown) with nothing to show', readSender({}) === '(unknown)');

console.log(`\nResult: ${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
