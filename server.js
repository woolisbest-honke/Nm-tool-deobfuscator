const http = require("http");
const https = require("https");
const { URL } = require("url");
const env = process.env;
const numberOrDefault = (value, fallback) => {
  const parsedNumber = Number(value);
  return value !== undefined && value !== "" && Number.isFinite(parsedNumber)
    ? parsedNumber
    : fallback;
};
const DEFAULT_WORKERS = [
  "https://api-tool.nemu0001.workers.dev",
  "https://api-tool.nemu01.workers.dev",
  "https://api-tool.myproxy0109.workers.dev",
  "https://api-tool.nemu0000.workers.dev",
];
const MAX_ATTEMPTS = Math.max(1, numberOrDefault(env.MAX_ATTEMPTS, 3));
const HEADER_TIMEOUT_MS = numberOrDefault(env.HEADER_TIMEOUT_MS, 15000);
const MAX_REQUEST_BODY_BYTES =
  numberOrDefault(env.MAX_BODY_MB, 4) * 1024 * 1024;
const CACHE_ENABLED = String(env.CACHE || "").toLowerCase() !== "off";
const MAX_CACHE_BYTES = numberOrDefault(env.CACHE_MB, 64) * 1024 * 1024;
const MAX_CACHE_ITEM_BYTES =
  numberOrDefault(env.CACHE_MAX_ITEM_MB, 4) * 1024 * 1024;
const DEFAULT_CACHE_TTL_MS =
  numberOrDefault(env.CACHE_DEFAULT_TTL_S, 900) * 1000;
const MAX_CACHE_TTL_MS = numberOrDefault(env.CACHE_MAX_TTL_S, 21600) * 1000;
const REDIS_URL = (
  env.UPSTASH_REDIS_REST_URL ||
  env.KV_REST_API_URL ||
  ""
).replace(/\/+$/, "");
const REDIS_TOKEN = env.UPSTASH_REDIS_REST_TOKEN || env.KV_REST_API_TOKEN || "";
const INSPECTABLE_STATUS_CODES = new Set([429, 500, 502, 503, 504]);
const MAX_ERROR_BODY_BYTES = 65536;
const SAFE_RETRY_ERRORS = new Set(["ENOTFOUND", "ECONNREFUSED", "EAI_AGAIN"]);
const workerUrlList =
  env.CF_WORKER_URLS && env.CF_WORKER_URLS.trim()
    ? env.CF_WORKER_URLS
    : DEFAULT_WORKERS.join(",");
const RESET_OFFSET_MS =
  (((numberOrDefault(env.RESET_UTC_HOUR, 0) % 24) + 24) % 24) * 3600000;
const getResetDay = () => Math.floor((Date.now() - RESET_OFFSET_MS) / 86400000);
const getNextResetTime = () => (getResetDay() + 1) * 86400000 + RESET_OFFSET_MS;
const formatJst = (timestampMs) =>
  new Date(timestampMs + 32400000)
    .toISOString()
    .replace("T", " ")
    .slice(0, 16) + " JST";
const workers = [];
for (const workerUrl of workerUrlList
  .split(/[\s,]+/)
  .map((value) => value.trim().replace(/\/+$/, ""))
  .filter(Boolean)) {
  try {
    const parsedUrl = new URL(workerUrl);
    if (parsedUrl.protocol !== "https:" && parsedUrl.protocol !== "http:") {
      throw new Error("protocol");
    }
    workers.push({
      id: workers.length + 1,
      url: parsedUrl,
      used: 0,
      day: getResetDay(),
      blockedUntil: 0,
      reason: "",
      fails: 0,
    });
  } catch {
    console.warn("[config] 無効な Worker URL を無視: " + workerUrl);
  }
}
function resetDailyUsageIfNeeded(worker) {
  const day = getResetDay();
  if (worker.day !== day) {
    worker.day = day;
    worker.used = 0;
    if (worker.reason === "quota") {
      worker.blockedUntil = 0;
      worker.reason = "";
      worker.fails = 0;
    }
  }
}
function blockWorker(worker, durationMs, reason) {
  worker.blockedUntil = Date.now() + durationMs;
  worker.reason = reason;
  console.log(
    "[worker#" +
      worker.id +
      "] 停止 " +
      reason +
      " → " +
      new Date(worker.blockedUntil).toISOString() +
      " (" +
      formatJst(worker.blockedUntil) +
      ")",
  );
}
function markWorkerOk(worker) {
  worker.fails = 0;
}
function recordWorkerFailure(worker, reason) {
  worker.fails++;
  if (worker.fails >= 2) {
    blockWorker(
      worker,
      Math.min(15000 * Math.pow(2, worker.fails - 2), 300000),
      reason,
    );
  }
}
async function applyWorkerError(worker, kind) {
  if (kind === "quota") {
    blockWorker(worker, getNextResetTime() - Date.now() + 30000, "quota");
    worker.fails = 0;
    await shareWorkerBlock(worker);
  } else if (kind === "rate") {
    blockWorker(worker, 60000, "rate");
    await shareWorkerBlock(worker);
  } else if (kind === "workererr") {
    recordWorkerFailure(worker, "worker-error");
  }
}
function selectWorker(excludedIds) {
  const now = Date.now();
  const candidates = [];
  for (const worker of workers) {
    resetDailyUsageIfNeeded(worker);
    if (excludedIds && excludedIds.has(worker.id)) {
      continue;
    }
    if (worker.blockedUntil > now) {
      continue;
    }
    candidates.push(worker);
  }
  if (!candidates.length) {
    return null;
  }
  candidates.sort(() => Math.random() - 0.5);
  candidates.sort((left, right) => left.used - right.used);
  return candidates[0];
}
function getRetryAfterSeconds() {
  const now = Date.now();
  let waitMs = Infinity;
  for (const worker of workers) {
    if (worker.blockedUntil > now) {
      waitMs = Math.min(waitMs, worker.blockedUntil - now);
    }
  }
  return Number.isFinite(waitMs)
    ? Math.max(1, Math.min(3600, Math.ceil(waitMs / 1000)))
    : 30;
}
const QUOTA_RE =
  /error\s*code:?\s*1027|error\s*1027|daily request limit|exceeded[^.]{0,40}(free|daily)[^.]{0,40}(limit|tier|quota)/i;
const RATE_RE =
  /error\s*code:?\s*1015|error\s*1015|you are being rate limited/i;
const WORKER_ERROR_RE =
  /error\s*code:?\s*110[12]|error\s*110[12]|worker threw exception|exceeded resource limits/i;
function classifyWorkerError(statusCode, body) {
  if (!INSPECTABLE_STATUS_CODES.has(statusCode)) {
    return null;
  }
  const text = body.toString("utf8", 0, 4096);
  if (QUOTA_RE.test(text)) {
    return "quota";
  }
  if (RATE_RE.test(text)) {
    return "rate";
  }
  if (WORKER_ERROR_RE.test(text)) {
    return "workererr";
  }
  return null;
}
const getSharedBlockKey = (worker) => "wproxy:block:" + worker.url.host;
async function redisRest(commandPath) {
  if (!REDIS_URL) {
    return null;
  }
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 1500);
    const response = await fetch(REDIS_URL + commandPath, {
      headers: { Authorization: "Bearer " + REDIS_TOKEN },
      signal: controller.signal,
    });
    clearTimeout(timer);
    return await response.json();
  } catch {
    return null;
  }
}
async function shareWorkerBlock(worker) {
  if (!REDIS_URL) {
    return;
  }
  const ttlSeconds = Math.max(
    1,
    Math.ceil((worker.blockedUntil - Date.now()) / 1000),
  );
  await redisRest(
    "/set/" +
      encodeURIComponent(getSharedBlockKey(worker)) +
      "/" +
      worker.blockedUntil +
      "/EX/" +
      ttlSeconds,
  );
}
let lastSyncAt = 0;
async function syncSharedBlocks() {
  if (!REDIS_URL || !workers.length || Date.now() - lastSyncAt < 8000) {
    return;
  }
  lastSyncAt = Date.now();
  const response = await redisRest(
    "/mget/" +
      workers
        .map((worker) => encodeURIComponent(getSharedBlockKey(worker)))
        .join("/"),
  );
  if (!response || !Array.isArray(response.result)) {
    return;
  }
  const now = Date.now();
  workers.forEach((worker, index) => {
    const blockedUntil = Number(response.result[index]);
    if (Number.isFinite(blockedUntil) && blockedUntil > now) {
      if (worker.blockedUntil < blockedUntil) {
        worker.blockedUntil = blockedUntil;
        worker.reason = "shared";
      }
    } else if (worker.reason === "shared") {
      worker.blockedUntil = 0;
      worker.reason = "";
    }
  });
}
const cache = new Map();
let cacheBytes = 0;
let cacheHits = 0;
let cacheMisses = 0;
function getCachedResponse(key) {
  const entry = cache.get(key);
  if (!entry) {
    return null;
  }
  if (entry.exp < Date.now()) {
    cache.delete(key);
    cacheBytes -= entry.size;
    return null;
  }
  cache.delete(key);
  cache.set(key, entry);
  return entry;
}
function storeCachedResponse(key, entry) {
  const previousEntry = cache.get(key);
  if (previousEntry) {
    cacheBytes -= previousEntry.size;
    cache.delete(key);
  }
  entry.size = entry.body.length + 1024;
  cache.set(key, entry);
  cacheBytes += entry.size;
  while (cacheBytes > MAX_CACHE_BYTES && cache.size) {
    const [oldestKey, oldestEntry] = cache.entries().next().value;
    cache.delete(oldestKey);
    cacheBytes -= oldestEntry.size;
  }
}
const STATIC_CONTENT_TYPE_RE =
  /^(image\/|font\/|text\/css|(application|text)\/(x-)?javascript|application\/(x-)?font|application\/vnd\.ms-fontobject)/i;
function getCacheTtl(cacheControl) {
  const match =
    /(?:^|,)\s*s-maxage=(\d+)/i.exec(cacheControl) ||
    /(?:^|,)\s*max-age=(\d+)/i.exec(cacheControl);
  if (!match) {
    return DEFAULT_CACHE_TTL_MS;
  }
  return Math.min(Number(match[1]) * 1000, MAX_CACHE_TTL_MS);
}
function getCookie(cookieHeader, cookieName) {
  const match = new RegExp("(?:^|;\\s*)" + cookieName + "=([^;]*)").exec(
    cookieHeader || "",
  );
  return match ? match[1] : "";
}
const REQUEST_HEADERS_TO_DROP = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-connection",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "content-length",
  "expect",
  "x-forwarded-for",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
  "x-real-ip",
  "forwarded",
  "via",
  "true-client-ip",
  "cdn-loop",
  "x-request-start",
]);
const RESPONSE_HEADERS_TO_DROP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-connection",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);
const shouldDropRequestHeader = (name) =>
  REQUEST_HEADERS_TO_DROP.has(name) ||
  name.startsWith("cf-") ||
  name.startsWith("x-vercel") ||
  name.startsWith("x-render") ||
  name.startsWith("rndr");
const getPublicHost = (req) =>
  String(req.headers["x-forwarded-host"] || req.headers.host || "")
    .split(",")[0]
    .trim();
function buildUpstreamHeaders(req, bodyLength) {
  const headers = {};
  for (const name of Object.keys(req.headers)) {
    if (!shouldDropRequestHeader(name)) {
      headers[name] = req.headers[name];
    }
  }
  headers["x-forwarded-host"] = getPublicHost(req);
  headers["x-forwarded-proto"] = "https";
  if (bodyLength > 0 || !/^(GET|HEAD)$/.test(req.method)) {
    headers["content-length"] = String(bodyLength);
  }
  return headers;
}
const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 128,
  maxFreeSockets: 32,
  scheduling: "lifo",
});
const httpAgent = new http.Agent({
  keepAlive: true,
  maxSockets: 128,
  maxFreeSockets: 32,
  scheduling: "lifo",
});
function requestWorker(worker, requestState) {
  return new Promise((resolve) => {
    const isHttp = worker.url.protocol === "http:";
    let settled = false;
    let headerTimer = null;
    const finish = (result) => {
      if (!settled) {
        settled = true;
        clearTimeout(headerTimer);
        resolve(result);
      }
    };
    const upstreamRequest = (isHttp ? http : https).request(
      {
        hostname: worker.url.hostname,
        port: worker.url.port || (isHttp ? 80 : 443),
        path: requestState.path,
        method: requestState.method,
        headers: Object.assign({}, requestState.headers, {
          host: worker.url.host,
        }),
        agent: isHttp ? httpAgent : httpsAgent,
      },
      (upstreamResponse) => {
        clearTimeout(headerTimer);
        const statusCode = upstreamResponse.statusCode;
        const contentLength = Number(
          upstreamResponse.headers["content-length"],
        );
        if (
          INSPECTABLE_STATUS_CODES.has(statusCode) &&
          Number.isFinite(contentLength) &&
          contentLength <= MAX_ERROR_BODY_BYTES
        ) {
          const chunks = [];
          upstreamResponse.on("data", (chunk) => chunks.push(chunk));
          upstreamResponse.on("end", () => {
            const body = Buffer.concat(chunks);
            finish({
              upRes: upstreamResponse,
              body,
              kind: classifyWorkerError(statusCode, body),
            });
          });
          upstreamResponse.on("error", () => finish({ error: "ECONNRESET" }));
          return;
        }
        finish({ upRes: upstreamResponse });
      },
    );
    headerTimer = setTimeout(() => {
      upstreamRequest.destroy(
        Object.assign(new Error("header timeout"), { code: "EHDRTIMEOUT" }),
      );
    }, HEADER_TIMEOUT_MS);
    upstreamRequest.on("error", (error) =>
      finish({ error: (error && error.code) || "ERROR" }),
    );
    requestState.cancel = () => {
      try {
        upstreamRequest.destroy();
      } catch {}
    };
    if (requestState.body && requestState.body.length) {
      upstreamRequest.end(requestState.body);
    } else {
      upstreamRequest.end();
    }
  });
}
function sendText(res, statusCode, text, extraHeaders) {
  if (res.headersSent) {
    try {
      res.end();
    } catch {}
    return;
  }
  const body = Buffer.from(text, "utf8");
  res.writeHead(
    statusCode,
    Object.assign(
      {
        "content-type": "text/plain; charset=utf-8",
        "content-length": String(body.length),
        "cache-control": "no-store",
      },
      extraHeaders || {},
    ),
  );
  res.end(body);
}
function sendBusy(res) {
  if (res.headersSent) {
    try {
      res.end();
    } catch {}
    return;
  }
  const html =
    '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>混み合っています</title><body style="font-family:system-ui,sans-serif;background:#0b0b14;color:#e5e7eb;display:grid;place-items:center;height:100vh;margin:0"><div style="text-align:center;padding:16px"><h2>ただいま混み合っています</h2><p>しばらくしてからもう一度開いてください。</p></div></body>';
  const body = Buffer.from(html, "utf8");
  res.writeHead(503, {
    "content-type": "text/html; charset=utf-8",
    "content-length": String(body.length),
    "retry-after": String(getRetryAfterSeconds()),
    "cache-control": "no-store",
  });
  res.end(body);
}
function forwardUpstreamResponse(req, res, result, cacheKey) {
  const upstreamResponse = result.upRes;
  const statusCode = upstreamResponse.statusCode;
  const headers = {};
  for (const name of Object.keys(upstreamResponse.headers)) {
    if (!RESPONSE_HEADERS_TO_DROP.has(name)) {
      headers[name] = upstreamResponse.headers[name];
    }
  }
  const contentType = String(headers["content-type"] || "");
  const originalCacheControl = String(headers["cache-control"] || "");
  if (
    !originalCacheControl &&
    statusCode === 200 &&
    (STATIC_CONTENT_TYPE_RE.test(contentType) || /x-icon/i.test(contentType))
  ) {
    headers["cache-control"] =
      /[-_.][A-Za-z0-9_-]{8,}\.(?:js|css|woff2?|png|jpe?g|webp|avif|svg)(?:[?#]|$)/.test(
        req.url || "",
      )
        ? "public, max-age=604800, immutable"
        : "public, max-age=3600";
  }
  if (result.body) {
    res.writeHead(statusCode, headers);
    res.end(req.method === "HEAD" ? undefined : result.body);
    return;
  }
  let cacheCapture = null;
  if (
    cacheKey &&
    statusCode === 200 &&
    !headers["set-cookie"] &&
    STATIC_CONTENT_TYPE_RE.test(contentType) &&
    !/no-store|private/i.test(originalCacheControl)
  ) {
    const ttlMs = getCacheTtl(headers["cache-control"] || "");
    const contentLength = Number(headers["content-length"]);
    if (
      ttlMs > 0 &&
      !(Number.isFinite(contentLength) && contentLength > MAX_CACHE_ITEM_BYTES)
    ) {
      cacheCapture = { chunks: [], size: 0, ttl: ttlMs, ok: true };
    }
  }
  res.writeHead(statusCode, headers);
  if (req.method === "HEAD" || statusCode === 204 || statusCode === 304) {
    upstreamResponse.resume();
    res.end();
    return;
  }
  upstreamResponse.pipe(res);
  if (cacheCapture) {
    upstreamResponse.on("data", (chunk) => {
      if (!cacheCapture.ok) {
        return;
      }
      cacheCapture.size += chunk.length;
      if (cacheCapture.size > MAX_CACHE_ITEM_BYTES) {
        cacheCapture.ok = false;
        cacheCapture.chunks = null;
      } else {
        cacheCapture.chunks.push(chunk);
      }
    });
    upstreamResponse.on("end", () => {
      if (cacheCapture.ok && !upstreamResponse.aborted) {
        storeCachedResponse(cacheKey, {
          status: statusCode,
          headers,
          body: Buffer.concat(cacheCapture.chunks),
          at: Date.now(),
          exp: Date.now() + cacheCapture.ttl,
        });
      }
    });
  }
  upstreamResponse.on("error", () => {
    try {
      res.destroy();
    } catch {}
  });
  upstreamResponse.on("aborted", () => {
    try {
      res.destroy();
    } catch {}
  });
}
function serveCachedResponse(req, res, entry) {
  const headers = Object.assign({}, entry.headers, {
    "x-proxy-cache": "HIT",
    age: String(Math.floor((Date.now() - entry.at) / 1000)),
  });
  const ifNoneMatch = req.headers["if-none-match"];
  if (ifNoneMatch && entry.headers.etag && ifNoneMatch === entry.headers.etag) {
    const notModifiedHeaders = {
      etag: entry.headers.etag,
      "x-proxy-cache": "HIT",
    };
    if (entry.headers["cache-control"]) {
      notModifiedHeaders["cache-control"] = entry.headers["cache-control"];
    }
    res.writeHead(304, notModifiedHeaders);
    res.end();
    return;
  }
  const ifModifiedSince = req.headers["if-modified-since"];
  if (
    !ifNoneMatch &&
    ifModifiedSince &&
    entry.headers["last-modified"] &&
    ifModifiedSince === entry.headers["last-modified"]
  ) {
    const notModifiedHeaders = {
      "last-modified": entry.headers["last-modified"],
      "x-proxy-cache": "HIT",
    };
    if (entry.headers["cache-control"]) {
      notModifiedHeaders["cache-control"] = entry.headers["cache-control"];
    }
    res.writeHead(304, notModifiedHeaders);
    res.end();
    return;
  }
  headers["content-length"] = String(entry.body.length);
  res.writeHead(entry.status, headers);
  res.end(req.method === "HEAD" ? undefined : entry.body);
}
function readRequestBody(req, maxBytes) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let totalBytes = 0;
    let tooLarge = false;
    req.on("data", (chunk) => {
      if (tooLarge) {
        return;
      }
      totalBytes += chunk.length;
      if (totalBytes > maxBytes) {
        tooLarge = true;
        chunks.length = 0;
      } else {
        chunks.push(chunk);
      }
    });
    req.on("end", () =>
      tooLarge
        ? reject(Object.assign(new Error("too large"), { code: "E2BIG" }))
        : resolve(Buffer.concat(chunks)),
    );
    req.on("error", reject);
    req.on("aborted", () => reject(new Error("aborted")));
  });
}
async function handleRequest(req, res) {
  const requestPath = req.url || "/";
  if (requestPath === "/_proxy/health" || requestPath === "/_render/health") {
    return sendText(res, 200, "ok");
  }
  if (requestPath === "/robots.txt") {
    return sendText(res, 200, "User-agent: *\nDisallow: /\n", {
      "cache-control": "public, max-age=86400",
    });
  }
  if (
    /^\/apple-touch-icon(-precomposed|-\d+x\d+)*\.png(\?|$)/.test(requestPath)
  ) {
    res.writeHead(204, { "cache-control": "public, max-age=86400" });
    return res.end();
  }
  if (requestPath.startsWith("/_proxy/status")) {
    if (env.STATUS_TOKEN) {
      let token = "";
      try {
        token =
          new URL(requestPath, "http://x").searchParams.get("token") || "";
      } catch {}
      if (token !== env.STATUS_TOKEN) {
        return sendText(res, 403, "forbidden");
      }
    }
    await syncSharedBlocks();
    const now = Date.now();
    const statusData = {
      note: "このインスタンスから見た状態です（Vercel はインスタンスが複数あります）",
      time: new Date().toISOString(),
      timeJST: formatJst(Date.now()),
      sharedState: REDIS_URL ? "redis" : "none",
      workers: workers.map((worker) => ({
        id: worker.id,
        host: worker.url.host.replace(/^(.{4}).*(.{12})$/, "$1…$2"),
        usedOnThisInstance: worker.used,
        state:
          worker.blockedUntil > now ? "blocked(" + worker.reason + ")" : "ok",
        blockedUntil:
          worker.blockedUntil > now
            ? new Date(worker.blockedUntil).toISOString()
            : null,
        blockedUntilJST:
          worker.blockedUntil > now ? formatJst(worker.blockedUntil) : null,
        consecutiveFails: worker.fails,
      })),
      cache: {
        items: cache.size,
        mb: +(cacheBytes / 1048576).toFixed(1),
        hits: cacheHits,
        misses: cacheMisses,
      },
    };
    const statusBody = Buffer.from(JSON.stringify(statusData, null, 2));
    res.writeHead(200, {
      "content-type": "application/json; charset=utf-8",
      "content-length": String(statusBody.length),
      "cache-control": "no-store",
    });
    return res.end(statusBody);
  }
  if (requestPath[0] !== "/") {
    return sendText(res, 400, "bad request");
  }
  if (
    requestPath === "/favicon.ico" &&
    String(env.FAVICON).toLowerCase() !== "proxy"
  ) {
    res.writeHead(204);
    return res.end();
  }
  if (!workers.length) {
    return sendText(res, 500, "No worker configured");
  }
  const isRetryableMethod = /^(GET|HEAD|OPTIONS)$/.test(req.method);
  let cacheKey = null;
  if (
    CACHE_ENABLED &&
    req.method === "GET" &&
    !req.headers.range &&
    !req.headers.authorization
  ) {
    cacheKey =
      getPublicHost(req) +
      "|" +
      getCookie(req.headers.cookie, "gw_site") +
      "|" +
      (req.headers["accept-encoding"] || "") +
      "|" +
      requestPath;
    const cachedEntry = getCachedResponse(cacheKey);
    if (cachedEntry) {
      cacheHits++;
      return serveCachedResponse(req, res, cachedEntry);
    }
    cacheMisses++;
  }
  let body = Buffer.alloc(0);
  if (!isRetryableMethod || req.method === "OPTIONS") {
    try {
      body = await readRequestBody(req, MAX_REQUEST_BODY_BYTES);
    } catch (error) {
      return sendText(
        res,
        error.code === "E2BIG" ? 413 : 400,
        error.code === "E2BIG" ? "payload too large" : "bad request",
      );
    }
  }
  const requestState = {
    method: req.method,
    path: requestPath,
    headers: buildUpstreamHeaders(req, body.length),
    body,
    cancel: null,
    dead: false,
  };
  res.on("close", () => {
    if (!res.writableEnded) {
      requestState.dead = true;
      if (requestState.cancel) {
        requestState.cancel();
      }
    }
  });
  await syncSharedBlocks();
  const triedWorkerIds = new Set();
  let lastWorkerError = null;
  for (let attemptIndex = 0; attemptIndex < MAX_ATTEMPTS; attemptIndex++) {
    if (requestState.dead) {
      return;
    }
    const worker = selectWorker(triedWorkerIds);
    if (!worker) {
      break;
    }
    triedWorkerIds.add(worker.id);
    worker.used++;
    const result = await requestWorker(worker, requestState);
    if (requestState.dead) {
      if (result.upRes) {
        result.upRes.destroy();
      }
      return;
    }
    if (result.error) {
      recordWorkerFailure(worker, result.error);
      if (isRetryableMethod || SAFE_RETRY_ERRORS.has(result.error)) {
        continue;
      }
      return sendText(res, 502, "Proxy Error");
    }
    if (result.kind === "quota" || result.kind === "rate") {
      await applyWorkerError(worker, result.kind);
      continue;
    }
    if (result.kind === "workererr") {
      await applyWorkerError(worker, result.kind);
      lastWorkerError = result;
      if (isRetryableMethod) {
        continue;
      }
      return forwardUpstreamResponse(req, res, result, null);
    }
    markWorkerOk(worker);
    return forwardUpstreamResponse(req, res, result, cacheKey);
  }
  if (lastWorkerError) {
    return forwardUpstreamResponse(req, res, lastWorkerError, null);
  }
  return sendBusy(res);
}
const requestListener = (req, res) => {
  handleRequest(req, res).catch((error) => {
    console.error("[handle]", error && error.message);
    if (!res.headersSent) {
      sendText(res, 502, "Proxy Error");
    } else {
      try {
        res.destroy();
      } catch {}
    }
  });
};
module.exports = requestListener;
module.exports.config = { api: { bodyParser: false } };
module.exports._internals = { workers, cache };
if (require.main === module) {
  const PORT = numberOrDefault(env.PORT, 10000);
  const server = http.createServer(requestListener);
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;
  server.listen(PORT, "0.0.0.0", () => {
    console.log(
      "[proxy] listening on :" +
        PORT +
        " / workers=" +
        workers.length +
        " / reset=UTC " +
        RESET_OFFSET_MS / 3600000 +
        ":00 (日本時間 " +
        ((RESET_OFFSET_MS / 3600000 + 9) % 24) +
        ":00)",
    );
  });
  server.on("clientError", (error, socket) => {
    try {
      socket.destroy();
    } catch {}
  });
  const stop = () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  process.on("unhandledRejection", (error) =>
    console.error("[unhandledRejection]", error && error.message),
  );
  process.on("uncaughtException", (error) =>
    console.error("[uncaughtException]", error && error.message),
  );
  if (env.KEEP_ALIVE === "1" && env.RENDER_EXTERNAL_URL) {
    setInterval(() => {
      https
        .get(
          env.RENDER_EXTERNAL_URL.replace(/\/+$/, "") + "/_proxy/health",
          (response) => response.resume(),
        )
        .on("error", () => {});
    }, 600000).unref();
  }
}
