<!-- @axona/protocol — the security-critical kernel. Please run through the checklist before merge. -->

## Summary

<!-- What changed and why. -->

## Checklist

- [ ] **Security-relevant change?** If this touches authentication, crypto, channel binding, key handling, access control, or anything that changes *what the protocol protects* — add/update an entry in [`axona-docs/SECURITY-CHANGELOG.md`](https://github.com/axona-net/axona-docs/blob/main/SECURITY-CHANGELOG.md). Resolved items only: describe **what's now protected**, not how the old gap was exploitable, and **never enumerate still-open findings** (those stay in the private red-team register). Flip the finding's status in that register too, so the two records agree.
- [ ] Tests pass (`npm test`); new behavior has coverage. Mesh/auth changes: also run `npm run test:mesh` (real-WebRTC harness).
- [ ] Version bumped for a shippable change — both `package.json` and `KERNEL_VERSION` in `src/transport/handshake.js`.
- [ ] Consumers re-vendored if the kernel changed (axona-peer `./scripts/sync-protocol.sh`, dht-sim `./scripts/sync-vendor-kernel.sh`).
- [ ] Flag-day implications (mixed-version interop) called out in the summary if the wire/handshake/CBV changed.
