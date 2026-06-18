# Axona Relay — desktop app (macOS)

A menu-bar app that runs an Axona relay with no terminal. It wraps the same
relay core as `node ../src/index.js` (shared by reference — `../src` + `../vendor`),
shows a green-light connection indicator, a mesh-connections graph, node/network
health, lets you switch between **Main** and **Testnet**, and can start at login.

The terminal relay is unchanged — this subproject is purely additive.

## Develop

```bash
cd desktop
npm install          # electron + electron-builder; node-datachannel/ws (prebuilt N-API 8 — no rebuild)
npm start            # runs the app (unsigned)
npm run smoke        # asserts the native node-datachannel binary loads under Electron
```

The relay runs in the Electron main process (Phase 0/1). Closing the window
hides to the menu bar (the relay keeps running); **Quit** from the tray stops it
and releases WebRTC.

## Build a distributable

```bash
npm run pack         # unsigned .app in dist/ (open via right-click → Open)
npm run dist         # signed + notarized .dmg/.zip (Developer ID) — see below
```

### Signing + notarization credentials (release builds only)

`npm run dist` signs with your Apple **Developer ID Application** cert and
notarizes via **notarytool**. electron-builder reads these from the environment
or login keychain at build time — export them before running:

```bash
# Signing identity — either the exported cert…
export CSC_LINK=/path/to/DeveloperIDApplication.p12
export CSC_KEY_PASSWORD='…'
#   …or rely on the cert already in your login keychain (omit CSC_LINK).

# Notarization — App Store Connect API key (recommended):
export APPLE_API_KEY=/path/to/AuthKey_XXXX.p8
export APPLE_API_KEY_ID=XXXXXXXXXX
export APPLE_API_ISSUER=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
#   …or Apple ID + app-specific password:
# export APPLE_ID='you@example.com'
# export APPLE_APP_SPECIFIC_PASSWORD='xxxx-xxxx-xxxx-xxxx'
# export APPLE_TEAM_ID='XXXXXXXXXX'
```

electron-builder signs (hardened runtime, the entitlements in
`build/entitlements.mac.plist`), submits to notarytool, waits, and staples the
ticket. Verify: `spctl -a -vvv "dist/mac-arm64/Axona Relay.app"`.

## Packaging notes

- **No native rebuild.** `node-datachannel` ships a prebuilt **N-API 8** binary,
  which is ABI-stable across Node and Electron — `@electron/rebuild` is *not*
  used. `npm run smoke` guards that the binary still loads.
- The binary is loaded by a hardcoded relative `require`, so the whole package
  stays outside the asar: `asarUnpack: ["**/node_modules/node-datachannel/**"]`.
- The relay core (`../src`, `../vendor`) is copied into `Resources/app-src` via
  `extraResources`; `src/main/resolve-relay.js` resolves it dev-vs-packaged.
- `build/icon.icns` is the Axona ant logo (generated from
  `axona-docs/images/Axona Ant.png` via `sips` + `iconutil`); electron-builder
  picks it up automatically. It shows on the `.app` in Finder and in the DMG (the
  app runs menu-bar-only, so there's no Dock icon).
