# sse-coordinator live demo

A zero-dependency demo: one Node server emits a named SSE stream, the page uses
[`sse-coordinator`](https://www.npmjs.com/package/sse-coordinator) to share a
single connection across tabs.

## Run it locally (recommended)

```bash
cd examples/demo
node server.js
# open http://localhost:3000 in 2+ tabs
```

One tab becomes the **LEADER** (owns the single `EventSource`); the rest are
**followers** fed over BroadcastChannel. Close the leader tab and watch a
follower get promoted instantly.

`localhost` is the most reliable way to see this, because leader election only
works **across tabs on the same origin** (BroadcastChannel and the Web Locks API
are origin-scoped).

## Run it online (StackBlitz)

[![Open in StackBlitz](https://developer.stackblitz.com/img/open_in_stackblitz.svg)](https://stackblitz.com/github/john-athan/sse-coordinator/tree/main/examples/demo)

⚠️ **One gotcha:** to see cross-tab coordination you must open multiple tabs on
the **same preview origin** — *not* the StackBlitz project URL twice.

1. Wait for the preview to boot.
2. Click the preview's **"Open in New Window"** (popout) button. You'll get a URL
   like `https://xxxx--3000.local-credentialless.webcontainer-api.io`.
3. Copy that URL and paste it into a **second normal browser tab**.
4. Both tabs now share one origin → one leader, the rest followers.

Opening the `stackblitz.com/...` project link in two tabs will **not** work: each
project tab spins up its own WebContainer with its own origin, so every tab
elects itself leader. (WebContainer preview origins are also cross-origin
isolated, which can make Web Locks behave oddly — `localhost` avoids all of this.)

No build step, no install — the page imports the library from
[esm.sh](https://esm.sh/sse-coordinator) and the server uses only Node built-ins.
