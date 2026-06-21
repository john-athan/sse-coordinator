# sse-coordinator live demo

A zero-dependency demo: one Node server emits a named SSE stream, the page uses
[`sse-coordinator`](https://www.npmjs.com/package/sse-coordinator) to share a
single connection across tabs.

## Run it online

[![Open in StackBlitz](https://developer.stackblitz.com/img/open_in_stackblitz.svg)](https://stackblitz.com/github/john-athan/sse-coordinator/tree/main/examples/demo)

Once it boots, open the preview in **2+ tabs** (use "open in new tab"). One tab
becomes the **LEADER**; the rest are followers fed over BroadcastChannel. Close
the leader and watch a follower take over instantly.

## Run it locally

```bash
cd examples/demo
node server.js
# open http://localhost:3000 in several tabs
```

No build step, no install — the page imports the library from
[esm.sh](https://esm.sh/sse-coordinator) and the server uses only Node built-ins.
