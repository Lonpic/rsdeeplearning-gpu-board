import http from 'node:http';
import fs from 'node:fs/promises';
import { timingSafeEqual } from 'node:crypto';

const hosts = new Map([['a100', 2], ['rtx4090', 5], ['v100-1', 2], ['v100-2', 2]]);
const snapshots = new Map();
const token = process.env.INGEST_TOKEN || '';
if (token.length < 32) throw new Error('INGEST_TOKEN must be at least 32 characters');
const html = await fs.readFile(new URL('./index.html', import.meta.url), 'utf8');
const port = Number(process.env.PORT || 10000);

function headers(type) {
  return {
    'content-type': type,
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
    'referrer-policy': 'no-referrer',
  };
}
function json(res, status, body) {
  res.writeHead(status, headers('application/json; charset=utf-8'));
  res.end(JSON.stringify(body));
}
function authorized(value) {
  const actual = Buffer.from(value || '');
  const expected = Buffer.from('Bearer ' + token);
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}
function validSnapshot(s, now) {
  const count = hosts.get(s?.host);
  if (!count || !Number.isFinite(s.captured_at) || !Number.isFinite(s.received_at)) return false;
  if (Math.abs(now - s.captured_at) > 300000 || now - s.received_at > 120000 || s.received_at > now + 10000) return false;
  if (s.error !== null) return typeof s.error === 'string' && s.error.length <= 200 && s.gpus === null;
  if (!Array.isArray(s.gpus) || s.gpus.length !== count) return false;
  const seen = new Set();
  for (const gpu of s.gpus) {
    if (!Number.isInteger(gpu?.index) || gpu.index < 0 || gpu.index >= count || seen.has(gpu.index) || !Array.isArray(gpu.occupants) || gpu.occupants.length > 30) return false;
    seen.add(gpu.index);
    for (const p of gpu.occupants) {
      if (!p || typeof p.user !== 'string' || !/^[A-Za-z0-9_.-]{1,32}$/.test(p.user) || !Number.isFinite(p.started_at) || p.started_at < 1577836800000 || p.started_at > now + 120000) return false;
    }
  }
  return true;
}
async function ingest(req, res) {
  if (!authorized(req.headers.authorization)) return json(res, 401, { error: 'unauthorized' });
  let body = '';
  try {
    for await (const chunk of req) {
      body += chunk;
      if (body.length > 20000) return json(res, 413, { error: 'too large' });
    }
    const payload = JSON.parse(body);
    if (!Array.isArray(payload.hosts) || payload.hosts.length > hosts.size) return json(res, 400, { error: 'bad hosts' });
    const now = Date.now();
    const seen = new Set();
    for (const s of payload.hosts) {
      if (seen.has(s?.host) || !validSnapshot(s, now)) return json(res, 400, { error: 'invalid snapshot' });
      seen.add(s.host);
    }
    for (const s of payload.hosts) {
      if ((snapshots.get(s.host)?.received_at || 0) < s.received_at) snapshots.set(s.host, s);
    }
    return json(res, 200, { ok: true, updated: [...seen] });
  } catch {
    return json(res, 400, { error: 'bad request' });
  }
}

http.createServer(async (req, res) => {
  const route = new URL(req.url || '/', 'http://localhost').pathname;
  if (req.method === 'GET' && route === '/health') return json(res, 200, { ok: true });
  if (req.method === 'GET' && route === '/api/status') return json(res, 200, { hosts: [...snapshots.values()] });
  if (req.method === 'POST' && route === '/api/ingest') return ingest(req, res);
  if (req.method === 'GET' && route === '/') {
    res.writeHead(200, {
      ...headers('text/html; charset=utf-8'),
      'content-security-policy': "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
    });
    return res.end(html);
  }
  return json(res, 404, { error: 'not found' });
}).listen(port, '0.0.0.0', () => console.log(`Listening on ${port}`));
