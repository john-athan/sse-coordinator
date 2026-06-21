// Zero-dependency demo server: serves the page and an SSE stream that emits
// NAMED events ("tick"), because sse-coordinator subscribes to named event
// types only. Run with `node server.js`, then open the page in several tabs.
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;

let counter = 0;

const server = createServer(async (req, res) => {
  if (req.url === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    res.write('retry: 2000\n\n');

    const interval = setInterval(() => {
      counter += 1;
      const payload = JSON.stringify({ count: counter, at: new Date().toISOString() });
      // Named event type "tick" + an id so stream resume / lastEventId works.
      res.write(`event: tick\nid: ${counter}\ndata: ${payload}\n\n`);
    }, 1500);

    req.on('close', () => clearInterval(interval));
    return;
  }

  // Serve the single-page demo.
  try {
    const html = await readFile(join(__dirname, 'index.html'));
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
  } catch {
    res.writeHead(404);
    res.end('Not found');
  }
});

server.listen(PORT, () => {
  console.log(`Demo running at http://localhost:${PORT}`);
  console.log('Open it in 2+ tabs and watch one become LEADER.');
});
