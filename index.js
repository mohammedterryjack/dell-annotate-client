'use strict';
// Shared client for the Dell's dell-annotate service (DELL_ANNOTATE, LAN only, accepts only oldmac).
// Used by vision-agent (Vision bot) and splat-agent (3D bot) so both queue, wait and pull the same way.
//   const DA = require('../dell-annotate-client')({ envFile })   // envFile holds DELL_LLM_API_KEY (read per request)
// Rules: a stage that is done/queued/running for a job is never POSTed again (done -> its stored result; queued/running
// -> waited on); only a new or failed stage is POSTed. GPU stages queue FIFO on the Dell and are never cancelled here.
// Waiting longer than waitMs throws DA.Pending (not a failure: call again to keep waiting). Files are streamed to disk
// (vggt_depth.npz is ~180 MB; buffering it whole OOM-killed vision-agent's 600 MB cgroup on 19 Nov 11:27 and 20 Nov
// 10:03). An interrupted copy resumes on the next service start (pull markers, resumePulls).
const fs = require('fs'), path = require('path');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');

class Pending extends Error {}
const JOB_RE = /^[0-9a-f]{16}$/;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const short = o => JSON.stringify(o).slice(0, 3000);
const VGGT_MEDIA = ['vggt.mp4', 'vggt_preview.jpg', 'vggt_cameras.json'];

module.exports = function client(o) {
  const base = o.base || process.env.DELL_ANNOTATE || (() => { throw new Error('DELL_ANNOTATE is not set (pass base or set the env)'); })();
  const waitMs = o.waitMs || 30 * 60 * 1000;   // per tool call (Hermes allows 40 min)
  const pollMs = o.pollMs || 3000;
  function key() {
    const m = /^DELL_LLM_API_KEY=(.+)$/m.exec(fs.readFileSync(o.envFile, 'utf8'));
    if (!m || m[1].trim().length < 16) throw new Error(`DELL_LLM_API_KEY is missing in ${o.envFile}`);
    return m[1].trim();
  }
  async function dell(method, p, body, headers = {}) {
    const r = await fetch(base + p, { method, body, headers: { Authorization: 'Bearer ' + key(), ...headers }, ...(body instanceof Readable ? { duplex: 'half' } : {}) });
    const t = await r.text();
    let j; try { j = JSON.parse(t); } catch { throw new Error(`dell-annotate ${method} ${p}: HTTP ${r.status} ${t.slice(0, 300)}`); }
    if (!r.ok) throw Object.assign(new Error(j.error || `dell-annotate ${method} ${p}: HTTP ${r.status}`), { status: r.status });
    return j;
  }
  const checkId = id => { if (!JOB_RE.test(id || '')) throw new Error('job_id must be the 16-hex id from start_job'); return id; };
  async function runStage(id, stage, params) {
    checkId(id);
    const t0 = Date.now();
    const cur = ((await dell('GET', `/v1/jobs/${id}`)).stages || {})[stage];
    let s;
    if (cur && ['done', 'running', 'queued'].includes(cur.state)) s = cur;
    else { const r = await dell('POST', `/v1/jobs/${id}/stages/${stage}`, JSON.stringify(params || {}), { 'Content-Type': 'application/json' }); s = { state: r.state, position: r.position }; }
    for (;;) {
      if (s.state === 'done') return s;
      if (s.state !== 'running' && s.state !== 'queued') throw new Error(`${stage} ${s.state}: ${s.error || 'no detail'}`);
      if (Date.now() - t0 > waitMs)
        throw new Pending(`${stage} is still ${s.state}${s.state === 'queued' ? ` (position ${s.position} in the Dell queue, behind ${s.ahead || 'other jobs'})` : ''} after ${Math.round(waitMs / 60000)} min. ` +
          `This is not a failure: call the same tool again with the same job_id to keep waiting (it will not run twice).`);
      await sleep(pollMs);
      s = (await dell('GET', `/v1/jobs/${id}`)).stages[stage];
    }
  }
  // Copy files from the Dell into dir, resumably. A marker <dir>/.pull-<stage>.json (job, stage, files) is written first;
  // files whose local size already equals the Dell's size (GET /files) are skipped; each file is streamed to <name>.part
  // and renamed when its size matches; then job.json is written and the marker removed. A service that dies mid-copy
  // finds the marker on start (resumePulls) and finishes it. A size mismatch or HTTP error throws: no partial result.
  async function pull(id, names, dir, stage) {
    checkId(id);
    if (!/^[a-z0-9_]+$/.test(stage || '')) throw new Error('pull needs the stage name');
    for (const n of names) if (!/^(keyframes\/)?[A-Za-z0-9_.-]+$/.test(n)) throw new Error(`refusing odd file name ${n}`);
    fs.mkdirSync(dir, { recursive: true });
    const marker = path.join(dir, `.pull-${stage}.json`);
    fs.writeFileSync(marker, JSON.stringify({ job: id, stage, files: names, started: new Date().toISOString() }));
    const sizes = Object.fromEntries((await dell('GET', `/v1/jobs/${id}/files`)).files.map(f => [f.name, f.bytes]));
    const got = [], skipped = [];
    for (const n of names) {
      if (!(n in sizes)) throw new Error(`${n} is not among job ${id}'s files on the Dell`);
      const f = path.join(dir, n);
      if (fs.existsSync(f) && fs.statSync(f).size === sizes[n]) { skipped.push(n); continue; }
      const r = await fetch(`${base}/v1/jobs/${id}/files/${n}`, { headers: { Authorization: 'Bearer ' + key() } });
      if (!r.ok) throw new Error(`could not fetch ${n}: HTTP ${r.status}`);
      fs.mkdirSync(path.dirname(f), { recursive: true });
      await pipeline(Readable.fromWeb(r.body), fs.createWriteStream(f + '.part'));
      const have = fs.statSync(f + '.part').size;
      if (have !== sizes[n]) throw new Error(`${n}: got ${have} bytes, the Dell has ${sizes[n]}`);
      fs.renameSync(f + '.part', f);
      got.push(n);
    }
    fs.writeFileSync(path.join(dir, 'job.json'), JSON.stringify(await dell('GET', `/v1/jobs/${id}`), null, 1));
    fs.unlinkSync(marker);
    return { got, skipped };
  }
  // On service start: finish every copy a previous run left half done (markers in <root>/<job>/.pull-*.json).
  // Runs in the background, one job after another; every failure is logged loudly and the marker is kept.
  function resumePulls(root, log) {
    const found = [];
    for (const id of fs.existsSync(root) ? fs.readdirSync(root) : []) {
      if (!JOB_RE.test(id)) continue;
      for (const m of fs.readdirSync(path.join(root, id)).filter(n => /^\.pull-[a-z0-9_]+\.json$/.test(n)))
        found.push({ dir: path.join(root, id), marker: m });
    }
    if (found.length) log('resuming', found.length, 'interrupted copies:', found.map(f => path.basename(f.dir) + '/' + f.marker).join(' '));
    return (async () => {
      for (const { dir, marker } of found) {
        let m;
        try {
          m = JSON.parse(fs.readFileSync(path.join(dir, marker), 'utf8'));
          const r = await pull(m.job, m.files, dir, m.stage);
          log('RESUMED copy', m.job, m.stage, 'fetched', r.got.join(',') || '-', 'already had', r.skipped.join(',') || '-');
        } catch (e) {
          console.error(new Date().toISOString(), 'RESUME FAILED', dir, marker, e.message);
        }
      }
    })();
  }
  // upload a clip (file path on this machine or http(s) URL); returns the Dell's {job, clip}
  async function startJob(a) {
    if (!!a.path === !!a.url) throw new Error('give exactly one of path or url');
    let body, size;
    if (a.path) { const st = fs.statSync(a.path); if (!st.isFile()) throw new Error(`${a.path} is not a file`); size = st.size; body = Readable.from(fs.createReadStream(a.path)); }
    else { const r = await fetch(a.url); if (!r.ok) throw new Error(`download failed: HTTP ${r.status}`); body = Buffer.from(await r.arrayBuffer()); size = body.length; }
    const q = new URLSearchParams({ name: a.name || path.basename(a.path || new URL(a.url).pathname) || 'clip' });
    if (a.start != null) q.set('start', a.start); if (a.duration != null) q.set('duration', a.duration);
    return dell('POST', `/v1/jobs?${q}`, body, { 'Content-Type': 'application/octet-stream', 'Content-Length': String(size) });
  }
  // VGGT-Omega on a job: run (or reuse) the queued vggt stage, pull its outputs into dir, save the job record.
  // Returns { stage, job, dir }. max_frames: frames per shot (Dell default 60, cap 100); conf_percentile default 20.
  async function vggt(id, a, dir) {
    if (a.max_frames != null && !(Number.isInteger(a.max_frames) && a.max_frames >= 1 && a.max_frames <= 100)) throw new Error('max_frames must be an integer 1..100');
    if (a.conf_percentile != null && !(a.conf_percentile >= 0 && a.conf_percentile <= 99)) throw new Error('conf_percentile must be 0..99');
    const s = await runStage(id, 'vggt', { max_frames: a.max_frames, conf_percentile: a.conf_percentile });
    await pull(id, s.result.files, dir, 'vggt');   // cameras json, depth npz, per-shot ply, vggt.mp4, vggt_preview.jpg (+ job.json)
    return { stage: s, dir };
  }
  return { key, dell, runStage, pull, resumePulls, startJob, vggt, Pending, JOB_RE, short, VGGT_MEDIA };
};
module.exports.Pending = Pending;
