# dell-annotate-client

A tiny shared Node module (no dependencies) for talking to `dell-annotate`, a video-annotation and 3D
reconstruction service on a LAN GPU box. It is used by
[vision-agent](https://github.com/mohammedterryjack/vision-agent) and
[splat-agent](https://github.com/mohammedterryjack/splat-agent), so both queue, wait and pull files the same way.
It is a library, not an MCP server on its own.

## Behaviour

- `runStage`: a stage that is already done returns its stored result, a queued or running one is waited on,
  and only a new or failed stage is POSTed. GPU stages queue FIFO on the server and are never cancelled here.
- Waiting longer than `waitMs` throws `DA.Pending` (not a failure: call again to keep waiting).
- Pulls stream to disk. A marker `<job>/.pull-<stage>.json` is written before each copy, files with matching
  sizes are skipped, and an interrupted copy resumes on the next service start (`resumePulls`); a resume that
  fails is logged loudly as `RESUME FAILED`.

## Use

```js
const DA = require(path.join(os.homedir(), 'Code', 'dell-annotate-client'))({
  base: process.env.DELL_ANNOTATE,   // e.g. http://<gpu-box>:8093
  envFile: '/path/to/.env',          // holds DELL_LLM_API_KEY, read per request
  waitMs: 12 * 60 * 1000,
});
```

Install by cloning into `~/Code/dell-annotate-client` (the agents require it from there).

## Configuration

| Variable / option | Default | Meaning |
|---|---|---|
| `DELL_ANNOTATE` / `base` | (required) | dell-annotate base URL |
| `envFile` | (required) | file containing `DELL_LLM_API_KEY=...`; the key is never stored in code |
