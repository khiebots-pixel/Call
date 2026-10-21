// api.ts
import { loginWithAuthToken } from "jsr:@evex/linejs@3.5.0";
import type { Device } from "jsr:@evex/linejs@3.5.0/base";
import {
  decodeWavSync,
  opusCodecFactory,
  packetizeNativeGroupOpusPairs,
  PlanetTransport,
  resampleLinear,
  type PlanetTransportOpts,
} from "jsr:@evex/linejs@3.5.0/call";

type MediaKeyMode = NonNullable<PlanetTransportOpts["mediaKeyMode"]>;

const PORT       = Number(Deno.env.get("PORT") || 8080);
const SAMPLE_RATE = 48_000;
const FRAME_MS    = 20;
const FRAME_SAMPLES = 960;

type JobStatus = "pending" | "running" | "done" | "failed" | "stopped";
type Job = {
  id: string; kind: "group" | "one"; status: JobStatus;
  startedAt: number; endedAt?: number;
  sent: number; error?: string; stopRequested: boolean;
};

const jobs = new Map<string, Job>();
const stops = new Map<string, () => void>();

Deno.serve({ port: PORT }, async (req) => {
  const url = new URL(req.url);
  try {
    if (req.method === "POST" && url.pathname === "/group/join") {
      const body = await req.json();
      const job = newJob("group");
      jobs.set(job.id, job);
      runGroup(job, body).catch((e) => fail(job, e));
      return Response.json({ jobId: job.id }, { status: 202 });
    }
    if (req.method === "POST" && url.pathname === "/one/call") {
      const body = await req.json();
      const job = newJob("one");
      jobs.set(job.id, job);
      runOne(job, body).catch((e) => fail(job, e));
      return Response.json({ jobId: job.id }, { status: 202 });
    }
    if (req.method === "GET"  && url.pathname.startsWith("/jobs/")) {
      const j = jobs.get(url.pathname.slice(6));
      return j ? Response.json(j) : Response.json({ error: "not found" }, { status: 404 });
    }
    if (req.method === "DELETE" && url.pathname.startsWith("/jobs/")) {
      stops.get(url.pathname.slice(6))?.();
      return Response.json({ ok: true });
    }
    if (req.method === "GET" && url.pathname === "/health") {
      return Response.json({ ok: true, jobs: jobs.size });
    }
    return Response.json({ error: "not found" }, { status: 404 });
  } catch (e) {
    return Response.json({ error: msg(e) }, { status: 500 });
  }
});

function newJob(kind: "group" | "one"): Job {
  return {
    id: crypto.randomUUID(), kind, status: "pending",
    startedAt: Date.now(), sent: 0, stopRequested: false,
  };
}
function fail(job: Job, e: unknown) {
  job.status = "failed"; job.error = msg(e); job.endedAt = Date.now();
}

// ---------------- Group ----------------
async function runGroup(job: Job, b: any) {
  const authToken = b.authToken || Deno.env.get("LINE_AUTH_TOKEN")?.trim();
  if (!authToken) throw new Error("authToken required");
  const chatMid = String(b.chatMid || "");
  if (!chatMid) throw new Error("chatMid required");

  const wavPath     = b.wavPath || "./output.wav";
  const device      = (b.device || "ANDROID") as Device;
  const version     = b.version || undefined;
  const timeoutMs   = Number(b.timeoutMs ?? 10_000);
  const gain        = Number(b.gain ?? 1);
  const repeatCount = Math.max(1, Math.floor(Number(b.repeatCount ?? 1)));
  const silent      = Boolean(b.silent);
  const mediaKeyMode = (b.mediaKeyMode || "audio-secret-sender") as MediaKeyMode;
  const deviceInfo  = b.deviceInfo || `ANDROID\t${version || "26.6.2"}\tAndroid OS\t16`;

  job.status = "running";
  let transport: PlanetTransport | undefined;
  try {
    const samples = await loadWav(wavPath, gain);
    const client  = await loginWithAuthToken(authToken, { device, version });
    const localMid = client.base.profile?.mid;
    if (!localMid) throw new Error("LINE MID unavailable");

    const state  = await client.call.getGroupCall(chatMid).catch(() => undefined);
    const online = Boolean((state as any)?.online);
    const route  = await client.call.acquireGroupRoute({
      chatMid, mediaType: "AUDIO",
      isInitialHost: !online, capabilities: ["AUDIO", "VIDEO"],
    } as never);

    transport = new PlanetTransport({ localMid, timeoutMs, mediaKeyMode, deviceInfo });
    stops.set(job.id, () => { job.stopRequested = true; });
    await transport.connect({ route });
    const joined = await transport.joinGroupDetailed({ roomId: chatMid });
    if (!joined.mediaReady) throw new Error("group joined, media not ready");

    if (silent) {
      while (!job.stopRequested) await sleep(1000);
      job.status = "stopped";
    } else {
      job.sent = await streamGroup(transport, samples, repeatCount, job);
      job.status = job.stopRequested ? "stopped" : "done";
    }
  } catch (e) {
    job.status = "failed"; job.error = msg(e);
  } finally {
    job.endedAt = Date.now(); stops.delete(job.id);
    await transport?.close().catch(() => {});
  }
}

async function streamGroup(tp: PlanetTransport, samples: Int16Array, repeatCount: number, job: Job): Promise<number> {
  const codec   = await opusCodecFactory();
  const encoder = codec.newEncoder({
    sampleRate: SAMPLE_RATE, channels: 1, frameDurationMs: FRAME_MS,
    signal: "music", vbr: false,
  });
  const prefix = new Uint8Array([0x00]);
  const packets: Uint8Array[] = [];
  const frameActivity: boolean[] = [];
  try {
    const padded = Math.ceil(samples.length / (FRAME_SAMPLES * 2)) * (FRAME_SAMPLES * 2);
    for (let r = 0; r < repeatCount; r++) {
      for (let off = 0; off < padded; off += FRAME_SAMPLES) {
        const frame = new Int16Array(FRAME_SAMPLES);
        frame.set(samples.subarray(off, off + FRAME_SAMPLES));
        const opus = encoder.encode({ samples: frame, sampleRate: SAMPLE_RATE, channels: 1 });
        if (!opus) throw new Error("encoder produced empty frame");
        const p = new Uint8Array(opus.length + 1);
        p[0] = prefix[0]; p.set(opus, 1);
        packets.push(p);
        frameActivity.push(frame.some((s) => s !== 0));
      }
    }
    if (packets.length % 2 !== 0) throw new Error("incomplete Opus pair");
    const payloads = packetizeNativeGroupOpusPairs(packets, {
      inputPrefixBytes: prefix.length, frameActivity,
    });
    let sent = 0; const started = performance.now();
    for (const p of payloads) {
      if (job.stopRequested) break;
      const wait = started + sent * FRAME_MS * 2 - performance.now();
      if (wait > 0) await sleep(wait);
      await tp.send(p, { timestampStep: FRAME_SAMPLES * 2 });
      sent++;
    }
    return sent;
  } finally { encoder.close?.(); }
}

// ---------------- 1:1 ----------------
async function runOne(job: Job, b: any) {
  const authToken = b.authToken || Deno.env.get("LINE_AUTH_TOKEN")?.trim();
  if (!authToken) throw new Error("authToken required");
  const to = String(b.to || "");
  if (!to) throw new Error("to required");

  const wavPath        = b.wavPath || "./output.wav";
  const device         = (b.device || "ANDROID") as Device;
  const version        = b.version || undefined;
  const timeoutMs      = Number(b.timeoutMs ?? 10_000);
  const answerTimeout  = Number(b.answerTimeoutMs ?? 60_000);
  const gain           = Number(b.gain ?? 1);
  const opusSignal     = (b.opusSignal || "music") as "auto" | "voice" | "music";
  const opusBitrate    = Number(b.opusBitrate ?? 32000);
  const opusVbr        = Boolean(b.opusVbr);
  const mediaKeyMode   = (b.mediaKeyMode || "audio-reverse-stage") as MediaKeyMode;
  const deviceInfo     = b.deviceInfo || `ANDROID\t${version || "26.8.0"}\tAndroid OS\t13`;

  job.status = "running";
  let transport: PlanetTransport | undefined;
  try {
    const samples = await loadWav(wavPath, gain);
    const client  = await loginWithAuthToken(authToken, { device, version });
    const localMid = client.base.profile?.mid;
    if (!localMid) throw new Error("LINE MID unavailable");

    const route = await client.call.acquireRoute({
      to, callType: "AUDIO", fromEnvInfo: { devname: "Android" },
    });
    transport = new PlanetTransport({ localMid, timeoutMs, mediaKeyMode, deviceInfo });
    stops.set(job.id, () => { job.stopRequested = true; });
    await transport.connect({ route });
    await transport.inviteDetailed({ to });
    const ans = await transport.waitForAnswerDetailed({ timeoutMs: answerTimeout, autoConnRsp: true });
    if (!ans.mediaReady) throw new Error("call answered, media not ready");

    job.sent = await streamOne(transport, samples, job, { opusSignal, opusBitrate, opusVbr });
    job.status = job.stopRequested ? "stopped" : "done";
  } catch (e) {
    job.status = "failed"; job.error = msg(e);
  } finally {
    job.endedAt = Date.now(); stops.delete(job.id);
    await transport?.close().catch(() => {});
  }
}

async function streamOne(
  tp: PlanetTransport, samples: Int16Array, job: Job,
  opt: { opusSignal: string; opusBitrate: number; opusVbr: boolean },
): Promise<number> {
  const codec   = await opusCodecFactory();
  const encoder = codec.newEncoder({
    sampleRate: SAMPLE_RATE, channels: 1, frameDurationMs: FRAME_MS,
    signal: opt.opusSignal as "auto" | "voice" | "music",
    bitrate: opt.opusBitrate, bandwidth: "fullband", vbr: opt.opusVbr,
  });
  let sent = 0; const started = performance.now();
  try {
    for (let off = 0; off < samples.length; off += FRAME_SAMPLES) {
      if (job.stopRequested) break;
      const idx = off / FRAME_SAMPLES;
      const wait = started + idx * FRAME_MS - performance.now();
      if (wait > 0) await sleep(wait);
      const frame = new Int16Array(FRAME_SAMPLES);
      frame.set(samples.subarray(off, off + FRAME_SAMPLES));
      const opus = encoder.encode({ samples: frame, sampleRate: SAMPLE_RATE, channels: 1 });
      if (opus) {
        const p = new Uint8Array(opus.length + 1); p[0] = 0x00; p.set(opus, 1);
        await tp.send(p, { timestampStep: FRAME_SAMPLES });
        sent++;
      }
    }
    return sent;
  } finally { encoder.close?.(); }
}

// ---------------- helpers (نفس ملفاتك) ----------------
async function loadWav(path: string, gain: number): Promise<Int16Array> {
  const wav = decodeWavSync(await Deno.readFile(path));
  let s = wav.samples;
  if (wav.sampleRate !== SAMPLE_RATE) s = resampleLinear(s, wav.sampleRate, SAMPLE_RATE, wav.channels);
  if (wav.channels > 1) s = downmix(s, wav.channels);
  if (gain !== 1) s = applyGain(s, gain);
  if (!s.length) throw new Error("WAV empty");
  return s;
}
function downmix(s: Int16Array, ch: number) {
  const n = Math.floor(s.length / ch), o = new Int16Array(n);
  for (let f = 0; f < n; f++) {
    let sum = 0;
    for (let c = 0; c < ch; c++) sum += s[f * ch + c] || 0;
    o[f] = Math.max(-32768, Math.min(32767, Math.round(sum / ch)));
  }
  return o;
}
function applyGain(s: Int16Array, g: number) {
  const o = new Int16Array(s.length);
  for (let i = 0; i < s.length; i++) o[i] = Math.max(-32768, Math.min(32767, Math.round(s[i] * g)));
  return o;
}
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const msg = (e: unknown) => e instanceof Error ? e.message : String(e);
