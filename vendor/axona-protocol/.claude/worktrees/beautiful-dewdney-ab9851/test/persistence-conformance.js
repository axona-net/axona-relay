// =====================================================================
// persistence-conformance.js — adapter conformance test suite.
//
// Re-usable across PersistenceAdapter implementations:
//   - InMemoryPersistence  (this file's own runner exercises it)
//   - IndexedDBPersistence (run in a browser context)
//   - FilePersistence      (run in Node with a temp dir)
//
// Each impl runs the same scenarios; differences in semantics
// (atomicity, durability) are validated in impl-specific smoke tests
// in addition to this baseline.
// =====================================================================

export async function runConformance(makeAdapter, { check, name }) {
  console.log(`\n=== PersistenceAdapter conformance: ${name} ===`);

  // ─── basic load/save/delete ──────────────────────────────────────
  {
    console.log('\n── basic load / save / delete ──');
    const a = await makeAdapter();
    check(`${name}: load missing key → undefined`,
      (await a.load('missing')) === undefined);

    await a.save('greeting', 'hello');
    check(`${name}: save then load → same value`,
      (await a.load('greeting')) === 'hello');

    await a.save('greeting', 'updated');
    check(`${name}: overwrite via save`,
      (await a.load('greeting')) === 'updated');

    await a.delete('greeting');
    check(`${name}: load after delete → undefined`,
      (await a.load('greeting')) === undefined);

    await a.delete('never-existed');     // no throw
    check(`${name}: delete missing is no-op`, true);
    await a.close();
  }

  // ─── JSON-friendly values (object / array / number / bool / null) ─
  {
    console.log('\n── value types ──');
    const a = await makeAdapter();
    const cases = [
      ['obj',    { a: 1, b: 'two', c: [3, 4] }],
      ['arr',    [1, 2, 3, { nested: true }]],
      ['num',    42.5],
      ['bool',   true],
      ['null',   null],
      ['string', 'plain string'],
    ];
    for (const [k, v] of cases) await a.save(k, v);
    for (const [k, v] of cases) {
      const loaded = await a.load(k);
      check(`${name}: ${k} round-trips`,
        JSON.stringify(loaded) === JSON.stringify(v));
    }
    await a.close();
  }

  // ─── load returns a structured clone, not a live reference ───────
  {
    console.log('\n── isolation: returned values do not alias storage ──');
    const a = await makeAdapter();
    const original = { count: 1, items: ['a'] };
    await a.save('thing', original);

    // Mutating the original after save() must not affect loaded values.
    original.count = 999;
    original.items.push('b');
    const loaded1 = await a.load('thing');
    check(`${name}: post-save mutation does not leak`,
      loaded1.count === 1 && loaded1.items.length === 1);

    // Mutating a loaded value must not affect future loads.
    loaded1.count = 7;
    loaded1.items.push('c');
    const loaded2 = await a.load('thing');
    check(`${name}: loaded-value mutation does not leak`,
      loaded2.count === 1 && loaded2.items.length === 1);

    await a.close();
  }

  // ─── transaction: atomic read-modify-write ───────────────────────
  {
    console.log('\n── transaction ──');
    const a = await makeAdapter();
    await a.save('balance', 100);
    await a.save('outstanding', 0);

    await a.transaction(['balance', 'outstanding'], (entries) => {
      const newBalance     = entries.balance - 10;
      const newOutstanding = entries.outstanding + 10;
      return { balance: newBalance, outstanding: newOutstanding };
    });

    check(`${name}: tx applied first write`,
      (await a.load('balance')) === 90);
    check(`${name}: tx applied second write`,
      (await a.load('outstanding')) === 10);

    // Deleting via transaction: returning `undefined` for a key.
    await a.transaction(['balance'], () => ({ balance: undefined }));
    check(`${name}: tx-undefined deletes key`,
      (await a.load('balance')) === undefined);

    // Returning nothing from fn() = no-op.
    await a.save('untouched', 'value');
    await a.transaction(['untouched'], () => undefined);
    check(`${name}: tx returning undefined is a no-op`,
      (await a.load('untouched')) === 'value');

    await a.close();
  }

  // ─── transaction failure handling ────────────────────────────────
  {
    console.log('\n── transaction failure ──');
    const a = await makeAdapter();
    await a.save('counter', 0);

    let txError = null;
    try {
      await a.transaction(['counter'], () => {
        throw new Error('user logic failed');
      });
    } catch (err) { txError = err; }
    check(`${name}: tx propagates fn() errors`,
      txError !== null && txError.message === 'user logic failed');

    // Storage must be unchanged after a failing transaction.
    check(`${name}: failing tx does not corrupt state`,
      (await a.load('counter')) === 0);

    // A subsequent transaction must still work.
    await a.transaction(['counter'], (e) => ({ counter: e.counter + 1 }));
    check(`${name}: next tx after failure still works`,
      (await a.load('counter')) === 1);

    await a.close();
  }

  // ─── transaction serialisation ───────────────────────────────────
  {
    console.log('\n── transaction serialisation ──');
    const a = await makeAdapter();
    await a.save('counter', 0);

    // 100 concurrent increments. If transactions don't serialise the
    // read-modify-write, the final counter will be < 100.
    const promises = [];
    for (let i = 0; i < 100; i++) {
      promises.push(
        a.transaction(['counter'], (e) => ({ counter: (e.counter ?? 0) + 1 }))
      );
    }
    await Promise.all(promises);
    check(`${name}: 100 concurrent transactions all applied`,
      (await a.load('counter')) === 100);

    await a.close();
  }

  // ─── close idempotency ───────────────────────────────────────────
  {
    console.log('\n── close ──');
    const a = await makeAdapter();
    await a.save('x', 1);
    await a.close();

    let threw = false;
    try { await a.save('x', 2); } catch { threw = true; }
    check(`${name}: save after close throws`, threw);

    let threw2 = false;
    try { await a.close(); } catch { threw2 = true; }
    check(`${name}: second close is idempotent`, !threw2);
  }

  // ─── input validation ────────────────────────────────────────────
  {
    console.log('\n── input validation ──');
    const a = await makeAdapter();
    let threw = false;
    try { await a.load(42); } catch { threw = true; }
    check(`${name}: load rejects non-string key`, threw);

    threw = false;
    try { await a.save(null, 'v'); } catch { threw = true; }
    check(`${name}: save rejects non-string key`, threw);

    threw = false;
    try { await a.transaction('not-an-array', () => ({})); } catch { threw = true; }
    check(`${name}: transaction rejects non-array keys`, threw);

    threw = false;
    try { await a.transaction(['k'], 'not-a-fn'); } catch { threw = true; }
    check(`${name}: transaction rejects non-function fn`, threw);

    await a.close();
  }
}
