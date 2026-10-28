const http = require("http");
const https = require("https");
const { URL } = require("url");
const env = process.env;
const parseNumber = (local_392, local_201) => {
  const local_446 = {
    eTJFR: function (local_134, local_601) {
      return local_134 !== local_601;
    },
  };
  const local_553 = Number(local_392);
  if (
    local_446.eTJFR(local_392, undefined) &&
    local_446.eTJFR(local_392, "") &&
    Number.isFinite(local_553)
  ) {
    return local_553;
  } else {
    return local_201;
  }
};
const DEFAULT_WORKERS = [
  "https://api-tool.nemu0001.workers.dev",
  "https://api-tool.nemu01.workers.dev",
  "https://api-tool.myproxy0109.workers.dev",
  "https://api-tool.nemu0000.workers.dev",
];
const MAX_ATTEMPTS = Math.max(1, parseNumber(env.MAX_ATTEMPTS, 3));
const HEADER_TIMEOUT = parseNumber(env.HEADER_TIMEOUT_MS, 15000);
const MAX_BODY = parseNumber(env.MAX_BODY_MB, 4) * 1024 * 1024;
const CACHE_ON = String(env.CACHE || "").toLowerCase() !== "off";
const CACHE_BYTES = parseNumber(env.CACHE_MB, 64) * 1024 * 1024;
const CACHE_ITEM_MAX = parseNumber(env.CACHE_MAX_ITEM_MB, 4) * 1024 * 1024;
const CACHE_DEFAULT_TTL = parseNumber(env.CACHE_DEFAULT_TTL_S, 900) * 1000;
const CACHE_MAX_TTL = parseNumber(env.CACHE_MAX_TTL_S, 21600) * 1000;
const REDIS_URL = (
  env.UPSTASH_REDIS_REST_URL ||
  env.KV_REST_API_URL ||
  ""
).replace(/\/+$/, "");
const REDIS_TOKEN = env.UPSTASH_REDIS_REST_TOKEN || env.KV_REST_API_TOKEN || "";
const PEEK_STATUS = new Set([429, 500, 502, 503, 504]);
const SAFE_ERR = new Set(["ENOTFOUND", "ECONNREFUSED", "EAI_AGAIN"]);
const rawList =
  env.CF_WORKER_URLS && env.CF_WORKER_URLS.trim()
    ? env.CF_WORKER_URLS
    : DEFAULT_WORKERS.join(",");
const RESET_OFFSET =
  (((parseNumber(env.RESET_UTC_HOUR, 0) % 24) + 24) % 24) * 3600000;
const nextUtcMidnight = () =>
  (Math.floor((Date.now() - RESET_OFFSET) / 86400000) + 1) * 86400000 +
  RESET_OFFSET;
const jst = (local_506) =>
  new Date(local_506 + 32400000).toISOString().replace("T", " ").slice(0, 16) +
  " JST";
const workers = [];
for (const s of rawList
  .split(/[\s,]+/)
  .map((local_449) => local_449.trim().replace(/\/+$/, ""))
  .filter(Boolean)) {
  try {
    const u = new URL(s);
    if (u.protocol !== "https:" && u.protocol !== "http:") {
      throw new Error("protocol");
    }
    workers.push({
      id: workers.length + 1,
      url: u,
      used: 0,
      day: Math.floor((Date.now() - RESET_OFFSET) / 86400000),
      blockedUntil: 0,
      reason: "",
      fails: 0,
    });
  } catch (a0_0x5a7e06) {
    console.warn("[config] 無効な Worker URL を無視: " + s);
  }
}
function resetWorkerDailyUsage(worker) {
  const local_577 = {
    qiFNO: function (local_212, local_196) {
      return local_212 !== local_196;
    },
    pwSuj: function (local_638, local_526) {
      return local_638 === local_526;
    },
    LyaLH: "quota",
    ZcsQY: "OvnCe",
    ngiah: "HLAxZ",
  };
  const local_377 = Math.floor((Date.now() - RESET_OFFSET) / 86400000);
  if (local_577.qiFNO(worker.day, local_377)) {
    worker.day = local_377;
    worker.used = 0;
    if (local_577.pwSuj(worker.reason, "quota")) {
      if (local_577.pwSuj(local_577.ZcsQY, local_577.ngiah)) {
        local_357.end();
      } else {
        worker.blockedUntil = 0;
        worker.reason = "";
        worker.fails = 0;
      }
    }
  }
}
function blockWorker(worker, durationMs, reason) {
  const local_448 = {
    qauvx: function (local_605, local_567) {
      return local_605 + local_567;
    },
    OskGG: function (local_547, local_458) {
      return local_547 + local_458;
    },
    XosED: function (local_200, local_457) {
      return local_200 + local_457;
    },
    Uqrgr: function (local_355, local_452) {
      return local_355 + local_452;
    },
    PPnNd: function (local_120, local_533) {
      return local_120 + local_533;
    },
    htozH: "[worker#",
    YjECA: function (local_582, local_232) {
      return local_582(local_232);
    },
  };
  worker.blockedUntil = Date.now() + durationMs;
  worker.reason = reason;
  console.log(
    local_448.qauvx(
      local_448.OskGG(
        local_448.XosED(
          local_448.OskGG(
            local_448.Uqrgr(
              local_448.XosED(local_448.htozH, worker.id) + "] 停止 ",
              reason,
            ) + " → ",
            new Date(worker.blockedUntil).toISOString(),
          ),
          " (",
        ),
        local_448.YjECA(jst, worker.blockedUntil),
      ),
      ")",
    ),
  );
}
function markWorkerHealthy(worker) {
  worker.fails = 0;
}
function recordWorkerFailure(worker, reason) {
  const local_548 = {
    iIdBz: function (local_400, local_114) {
      return local_400 >= local_114;
    },
    NlpfQ: function (local_163, local_618, local_050, local_407) {
      return local_163(local_618, local_050, local_407);
    },
    xwHRX: function (local_549, local_541) {
      return local_549 - local_541;
    },
  };
  worker.fails++;
  if (local_548.iIdBz(worker.fails, 2)) {
    local_548.NlpfQ(
      blockWorker,
      worker,
      Math.min(Math.pow(2, local_548.xwHRX(worker.fails, 2)) * 15000, 300000),
      reason,
    );
  }
}
async function applyWorkerFailurePolicy(worker, failureKind) {
  const local_388 = {
    uRQww: "public, max-age=86400",
    SOMHH: function (local_580, local_034) {
      return local_580 === local_034;
    },
    oGguo: "quota",
    TDjVf: function (local_432, local_173, local_204, local_422) {
      return local_432(local_173, local_204, local_422);
    },
    nBllr: function (local_552, local_626) {
      return local_552 - local_626;
    },
    nQXWp: function (local_403) {
      return local_403();
    },
    LvaaJ: function (local_104, local_064) {
      return local_104(local_064);
    },
    tSqxR: "rate",
    PPQHk: function (local_486, local_606) {
      return local_486 !== local_606;
    },
    xtJIH: "wKPnQ",
    lThWK: "RDKKw",
    lihsO: "workererr",
    LGksH: function (local_093, local_226, local_521) {
      return local_093(local_226, local_521);
    },
    CGkEp: "worker-error",
  };
  if (local_388.SOMHH(failureKind, local_388.oGguo)) {
    local_388.TDjVf(
      blockWorker,
      worker,
      local_388.nBllr(local_388.nQXWp(nextUtcMidnight), Date.now()) + 30000,
      local_388.oGguo,
    );
    worker.fails = 0;
    await shareWorkerBlock(worker);
  } else if (local_388.SOMHH(failureKind, local_388.tSqxR)) {
    if (local_388.PPQHk(local_388.xtJIH, local_388.lThWK)) {
      blockWorker(worker, 60000, local_388.tSqxR);
      await shareWorkerBlock(worker);
    } else {
      local_299.writeHead(204, {
        "cache-control": local_388.uRQww,
      });
      return local_230.end();
    }
  } else if (local_388.SOMHH(failureKind, local_388.lihsO)) {
    local_388.LGksH(recordWorkerFailure, worker, local_388.CGkEp);
  }
}
function chooseAvailableWorker(excludedWorkerIds) {
  const local_022 = {
    qgvBq: function (local_406, local_411) {
      return local_406 !== local_411;
    },
    mltQO: "VYTce",
    rTuUG: "rtyzG",
    sYrnW: function (local_599, local_216) {
      return local_599(local_216);
    },
    OwMmU: function (local_545, local_233) {
      return local_545 > local_233;
    },
  };
  const local_575 = Date.now();
  const local_015 = [];
  for (const local_068 of workers) {
    if (local_022.mltQO !== local_022.rTuUG) {
      local_022.sYrnW(resetWorkerDailyUsage, local_068);
      if (excludedWorkerIds && excludedWorkerIds.has(local_068.id)) {
        continue;
      }
      if (local_068.blockedUntil > local_575) {
        continue;
      }
      local_015.push(local_068);
    } else {
      local_386.destroy();
    }
  }
  if (!local_015.length) {
    return null;
  }
  local_015.sort(() => Math.random() - 0.5);
  local_015.sort((local_507, local_105) => local_507.used - local_105.used);
  return local_015[0];
}
function getRetryDelaySeconds() {
  const local_594 = {
    Kjchp: function (local_417, local_534) {
      return local_417 - local_534;
    },
    xBiIz: function (local_600, local_096) {
      return local_600 / local_096;
    },
  };
  const local_464 = Date.now();
  let local_183 = Infinity;
  for (const local_518 of workers) {
    if (local_518.blockedUntil > local_464) {
      local_183 = Math.min(
        local_183,
        local_594.Kjchp(local_518.blockedUntil, local_464),
      );
    }
  }
  if (Number.isFinite(local_183)) {
    return Math.max(
      1,
      Math.min(3600, Math.ceil(local_594.xBiIz(local_183, 1000))),
    );
  } else {
    return 30;
  }
}
const QUOTA_RE =
  /error\s*code:?\s*1027|error\s*1027|daily request limit|exceeded[^.]{0,40}(free|daily)[^.]{0,40}(limit|tier|quota)/i;
const RATE_RE =
  /error\s*code:?\s*1015|error\s*1015|you are being rate limited/i;
const WERR_RE =
  /error\s*code:?\s*110[12]|error\s*110[12]|worker threw exception|exceeded resource limits/i;
function classifyWorkerResponse(statusCode, body) {
  const local_360 = {
    DxXAa: "quota",
    ffZVJ: "rate",
    qnvsv: "workererr",
  };
  if (!PEEK_STATUS.has(statusCode)) {
    return null;
  }
  const local_554 = body.toString("utf8", 0, 4096);
  if (QUOTA_RE.test(local_554)) {
    return local_360.DxXAa;
  }
  if (RATE_RE.test(local_554)) {
    return local_360.ffZVJ;
  }
  if (WERR_RE.test(local_554)) {
    return local_360.qnvsv;
  }
  return null;
}
const shareKey = (local_376) => "wproxy:blockWorker:" + local_376.url.host;
async function redisRequest(commandPath) {
  const local_345 = {
    rGiFr: "shared",
    bocAp: "HIT",
    mzCdX: function (local_488, local_520) {
      return local_488(local_520);
    },
    foXIt: function (local_310, local_060) {
      return local_310 / local_060;
    },
    IOlzU: "if-none-match",
    VowvM: function (local_419, local_572) {
      return local_419 === local_572;
    },
    FJIfy: "cache-control",
    ySSXu: "if-modified-since",
    sgWmU: "last-modified",
    qZYrc: function (local_119, local_118) {
      return local_119 === local_118;
    },
    BSGSy: "content-length",
    nDBgu: function (local_385, local_354) {
      return local_385(local_354);
    },
    cnzqX: function (local_124, local_390) {
      return local_124 === local_390;
    },
    tQNCm: "nhRUp",
    bwNgk: "EXWWX",
    heouY: function (local_113, local_260, local_524) {
      return local_113(local_260, local_524);
    },
    cXken: function (local_211, local_109, local_587) {
      return local_211(local_109, local_587);
    },
    APHHa: function (local_272, local_188) {
      return local_272 + local_188;
    },
    VwzFr: function (local_542, local_614) {
      return local_542 + local_614;
    },
    ysyOq: "Bearer ",
    rlIuU: "guBCg",
    nOPNq: "VNLzq",
  };
  if (!REDIS_URL) {
    return null;
  }
  try {
    if (local_345.tQNCm === local_345.bwNgk) {
      local_203.blockedUntil = local_156;
      local_623.reason = local_345.rGiFr;
    } else {
      const local_415 = new AbortController();
      const local_009 = local_345.heouY(
        setTimeout,
        () => local_415.abort(),
        1500,
      );
      const local_030 = await local_345.cXken(
        fetch,
        local_345.APHHa(REDIS_URL, commandPath),
        {
          headers: {
            Authorization: local_345.VwzFr(local_345.ysyOq, REDIS_TOKEN),
          },
          signal: local_415.signal,
        },
      );
      local_345.mzCdX(clearTimeout, local_009);
      return await local_030.json();
    }
  } catch (local_179) {
    if (local_345.qZYrc(local_345.rlIuU, local_345.nOPNq)) {
      const local_234 = local_471.assign({}, local_007.headers, {
        "x-proxy-cache": local_345.bocAp,
        age: local_345.mzCdX(
          local_437,
          local_071.floor(
            local_345.foXIt(local_032.now() - local_634.at, 1000),
          ),
        ),
      });
      const local_111 = local_475.headers[local_345.IOlzU];
      if (
        local_111 &&
        local_462.headers.etag &&
        local_111 === local_610.headers.etag
      ) {
        const local_291 = {
          etag: local_408.headers.etag,
          "x-proxy-cache": local_345.bocAp,
        };
        if (local_501.headers[local_345.FJIfy]) {
          local_291[local_345.FJIfy] = local_076.headers[local_345.FJIfy];
        }
        local_242.writeHead(304, local_291);
        local_017.end();
        return;
      }
      const local_380 = local_044.headers[local_345.ySSXu];
      if (
        !local_111 &&
        local_380 &&
        local_192.headers[local_345.sgWmU] &&
        local_345.qZYrc(local_380, local_445.headers["last-modified"])
      ) {
        const local_027 = {
          "last-modified": local_454.headers["last-modified"],
          "x-proxy-cache": "HIT",
        };
        if (local_551.headers[local_345.FJIfy]) {
          local_027[local_345.FJIfy] = local_365.headers[local_345.FJIfy];
        }
        local_297.writeHead(304, local_027);
        local_444.end();
        return;
      }
      local_234[local_345.BSGSy] = local_305(local_159.body.length);
      local_287.writeHead(local_073.status, local_234);
      local_046.end(
        local_345.cnzqX(local_278.method, "HEAD") ? local_611 : local_370.body,
      );
    } else {
      return null;
    }
  }
}
async function shareWorkerBlock(worker) {
  const local_189 = {
    hnsfv: function (local_143, local_447) {
      return local_143 / local_447;
    },
    DmJme: function (local_304, local_512) {
      return local_304 - local_512;
    },
    YifRN: function (local_248, local_369) {
      return local_248(local_369);
    },
    ZYSww: function (local_513, local_121) {
      return local_513 + local_121;
    },
    oWzIc: function (local_363, local_326) {
      return local_363 + local_326;
    },
    TgnhJ: "/set/",
    NciVR: function (local_092, local_094) {
      return local_092(local_094);
    },
    zCSZZ: "/EX/",
  };
  if (!REDIS_URL) {
    return;
  }
  const local_177 = Math.max(
    1,
    Math.ceil(local_189.DmJme(worker.blockedUntil, Date.now()) / 1000),
  );
  await redisRequest(
    local_189.ZYSww(
      local_189.ZYSww(
        local_189.oWzIc(
          local_189.TgnhJ,
          local_189.YifRN(
            encodeURIComponent,
            local_189.NciVR(shareKey, worker),
          ),
        ) +
          "/" +
          worker.blockedUntil,
        local_189.zCSZZ,
      ),
      local_177,
    ),
  );
}
let lastSync = 0;
async function syncSharedWorkerBlocks() {
  const local_275 = {
    nJfOl: function (local_089, local_536) {
      return local_089 === local_536;
    },
    bBJgS: "quota",
    ApVTq: function (local_529, local_245) {
      return local_529 > local_245;
    },
    tXlXk: function (local_219, local_562) {
      return local_219 < local_562;
    },
    xYBNs: function (local_559, local_086) {
      return local_559 !== local_086;
    },
    YAfXC: "EFszS",
    bZkgK: "WCKQE",
    sEWIG: function (local_126, local_133) {
      return local_126 === local_133;
    },
    wqOYx: "shared",
    xrKnS: function (local_090, local_427) {
      return local_090 - local_427;
    },
    egJkm: function (local_566, local_130) {
      return local_566 + local_130;
    },
    cjeQn: "/mget/",
  };
  if (
    !REDIS_URL ||
    !workers.length ||
    local_275.xrKnS(Date.now(), lastSync) < 8000
  ) {
    return;
  }
  lastSync = Date.now();
  const local_602 = await redisRequest(
    local_275.egJkm(
      local_275.cjeQn,
      workers
        .map((local_257) =>
          encodeURIComponent("wproxy:blockWorker:" + local_257.url.host),
        )
        .join("/"),
    ),
  );
  if (!local_602 || !Array.isArray(local_602.result)) {
    return;
  }
  const local_292 = Date.now();
  workers.forEach((local_237, local_578) => {
    const local_045 = Number(local_602.result[local_578]);
    if (Number.isFinite(local_045) && local_275.ApVTq(local_045, local_292)) {
      if (local_275.tXlXk(local_237.blockedUntil, local_045)) {
        if (local_275.xYBNs(local_275.YAfXC, local_275.bZkgK)) {
          local_237.blockedUntil = local_045;
          local_237.reason = "shared";
        } else {
          local_267.day = local_231;
          local_312.used = 0;
          if (BOCSrP.nJfOl(local_253.reason, BOCSrP.bBJgS)) {
            local_254.blockedUntil = 0;
            local_214.reason = "";
            local_285.fails = 0;
          }
        }
      }
    } else if (local_275.sEWIG(local_237.reason, local_275.wqOYx)) {
      local_237.blockedUntil = 0;
      local_237.reason = "";
    }
  });
}
const cache = new Map();
let cacheBytes = 0;
let cacheHits = 0;
let cacheMiss = 0;
function getCachedResponse(key) {
  const local_482 = cache.get(key);
  if (!local_482) {
    return null;
  }
  if (local_482.exp < Date.now()) {
    cache.delete(key);
    cacheBytes -= local_482.size;
    return null;
  }
  cache.delete(key);
  cache.set(key, local_482);
  return local_482;
}
function storeCachedResponse(key, entry) {
  const local_067 = {
    XmPoD: function (local_031, local_393) {
      return local_031 !== local_393;
    },
    KATtd: "koBqI",
    FVnne: "VzbCT",
    LLYYu: function (local_186, local_535) {
      return local_186 + local_535;
    },
  };
  const local_083 = cache.get(key);
  if (local_083) {
    if (local_067.XmPoD(local_067.KATtd, local_067.FVnne)) {
      cacheBytes -= local_083.size;
      cache.delete(key);
    } else {
      try {
        local_508.destroy();
      } catch (local_328) {}
    }
  }
  entry.size = local_067.LLYYu(entry.body.length, 1024);
  cache.set(key, entry);
  cacheBytes += entry.size;
  while (cacheBytes > CACHE_BYTES && cache.size) {
    const [local_531, local_047] = cache.entries().next().value;
    cache.delete(local_531);
    cacheBytes -= local_047.size;
  }
}
const STATIC_CT =
  /^(image\/|font\/|text\/css|(application|text)\/(x-)?javascript|application\/(x-)?font|application\/vnd\.ms-fontobject)/i;
function getCacheTtl(headers) {
  const local_088 = {
    GSSQF: function (local_337, local_250) {
      return local_337(local_250);
    },
  };
  const local_039 =
    /(?:^|,)\s*s-maxage=(\d+)/i.exec(headers) ||
    /(?:^|,)\s*max-age=(\d+)/i.exec(headers);
  if (!local_039) {
    return CACHE_DEFAULT_TTL;
  }
  return Math.min(local_088.GSSQF(Number, local_039[1]) * 1000, CACHE_MAX_TTL);
}
function getCookieKey(cookieHeader, cookieName) {
  const local_165 = {
    nGRfr: function (local_261, local_069) {
      return local_261 + local_069;
    },
    BIJSf: "(?:^|;\\s*)",
    QFnBs: function (local_180, local_375) {
      return local_180 || local_375;
    },
  };
  const local_493 = new RegExp(
    local_165.nGRfr(local_165.BIJSf, cookieName) + "=([^;]*)",
  ).exec(local_165.QFnBs(cookieHeader, ""));
  if (local_493) {
    return local_493[1];
  } else {
    return "";
  }
}
const REQ_DROP = new Set([
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
const RES_DROP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-connection",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);
const shouldDropRequestHeader = (local_401) =>
  REQ_DROP.has(local_401) ||
  local_401.startsWith("cf-") ||
  local_401.startsWith("x-vercel") ||
  local_401.startsWith("x-render") ||
  local_401.startsWith("rndr");
const getPublicHost = (local_368) =>
  String(local_368.headers["x-forwarded-host"] || local_368.headers.host || "")
    .split(",")[0]
    .trim();
function buildUpstreamHeaders(request, bodyLength) {
  const local_502 = {
    sFtPy: function (local_517, local_364) {
      return local_517(local_364);
    },
    ATJHm: "x-forwarded-host",
    jOnZa: function (local_283, local_042) {
      return local_283(local_042);
    },
    uNNGd: "x-forwarded-proto",
    vXdJg: "https",
    WaSuO: function (local_504, local_136) {
      return local_504 > local_136;
    },
    irrpl: "content-length",
    DorPU: function (local_340, local_469) {
      return local_340(local_469);
    },
  };
  const local_603 = {};
  for (const local_590 of Object.keys(request.headers)) {
    if (!local_502.sFtPy(shouldDropRequestHeader, local_590)) {
      local_603[local_590] = request.headers[local_590];
    }
  }
  local_603[local_502.ATJHm] = String(
    request.headers["x-forwarded-host"] || request.headers.host || "",
  )
    .split(",")[0]
    .trim();
  local_603[local_502.uNNGd] = local_502.vXdJg;
  if (local_502.WaSuO(bodyLength, 0) || !/^(GET|HEAD)$/.test(request.method)) {
    local_603[local_502.irrpl] = String(bodyLength);
  }
  return local_603;
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
function requestWorker(worker, proxyRequest) {
  const local_619 = {
    pyzNe: function (local_494, local_244) {
      return local_494 > local_244;
    },
    gLJGC: function (local_621, local_056) {
      return local_621 === local_056;
    },
    TynuE: "asDPb",
    gHQJN: function (local_025, local_498) {
      return local_025(local_498);
    },
    AtpdE: function (local_333, local_435, local_510) {
      return local_333(local_435, local_510);
    },
    nwoDP: "HIT",
    pdZgW: "cache-control",
    DSAtj: "wPIuK",
    kLokE: "bsWSf",
    ZeVhP: "WlKLT",
    nUFIV: "yRPoj",
    isKZg: function (local_236, local_012) {
      return local_236(local_012);
    },
    ADaLd: "content-length",
    RXwau: function (local_006, local_581) {
      return local_006 <= local_581;
    },
    kawRy: "yKOXk",
    jQEdz: "end",
    JLUlK: "error",
    ZNorC: "HKNog",
    QgeEp: "header timeout",
    znUSn: "EHDRTIMEOUT",
    TDKPW: function (local_172, local_270) {
      return local_172 + local_270;
    },
    zjptz: function (local_135, local_256) {
      return local_135 + local_256;
    },
    MBFcN: function (local_206, local_258) {
      return local_206 + local_258;
    },
    RjUGK: function (local_040, local_336) {
      return local_040 + local_336;
    },
    tpiEO: function (local_451, local_268) {
      return local_451 / local_268;
    },
    OqfyX: ":00)",
    UpIJN: "mMkal",
    BHZeM: function (local_592, local_155) {
      return local_592 === local_155;
    },
    kRnaU: "http:",
  };
  return new Promise((local_087) => {
    const local_416 = {
      fAXzq: function (local_277, local_038, local_497) {
        return local_619.AtpdE(local_277, local_038, local_497);
      },
      pzPmY: local_619.nwoDP,
      CigTr: local_619.pdZgW,
      wwSbF: function (local_306, local_210) {
        return local_619.gLJGC(local_306, local_210);
      },
      wgofK: local_619.DSAtj,
      JrlwO: local_619.kLokE,
      PlInK: function (local_220, local_043) {
        return local_220(local_043);
      },
      iovsk: function (local_595, local_182) {
        return local_595(local_182);
      },
      LnTDz: function (local_167, local_426, local_631) {
        return local_167(local_426, local_631);
      },
      wJFcA: function (local_429, local_146) {
        return local_429 !== local_146;
      },
      Atxdh: local_619.ZeVhP,
      rKHqD: local_619.nUFIV,
      uovST: function (local_202, local_622) {
        return local_619.isKZg(local_202, local_622);
      },
      wNAkq: local_619.ADaLd,
      YddqD: function (local_353, local_085) {
        return local_619.RXwau(local_353, local_085);
      },
      muQNk: "yKOXk",
      fpntu: "data",
      QtVYF: local_619.jQEdz,
      XJePe: local_619.JLUlK,
      Jmnck: function (local_041, local_495) {
        return local_041 !== local_495;
      },
      xblxm: local_619.ZNorC,
      xvTZH: local_619.QgeEp,
      gwEAO: local_619.znUSn,
      qYHaE: function (local_314, local_195) {
        return local_619.TDKPW(local_314, local_195);
      },
      mqhDL: function (local_115, local_302) {
        return local_619.zjptz(local_115, local_302);
      },
      djrjK: function (local_461, local_584) {
        return local_461 + local_584;
      },
      jiTvX: function (local_397, local_356) {
        return local_619.TDKPW(local_397, local_356);
      },
      mAMQK: function (local_589, local_078) {
        return local_619.RjUGK(local_589, local_078);
      },
      XhhkM: function (local_615, local_176) {
        return local_615 + local_176;
      },
      NzHlA: function (local_329, local_442) {
        return local_329 + local_442;
      },
      ARGLB: " / workers=",
      ddelv: " / reset=UTC ",
      wnkwS: function (local_492, local_450) {
        return local_619.tpiEO(local_492, local_450);
      },
      kZqPS: local_619.OqfyX,
      WSgWX: function (local_221, local_499) {
        return local_221 !== local_499;
      },
      iCEcx: local_619.UpIJN,
    };
    const local_247 = local_619.BHZeM(worker.url.protocol, local_619.kRnaU);
    let local_309 = false;
    let local_604 = null;
    const local_035 = (local_636) => {
      const local_011 = {
        VhtLw: function (local_588, local_271) {
          return local_619.pyzNe(local_588, local_271);
        },
      };
      if ("mEDwg" !== "mEDwg") {
        local_217.close(() => local_280.exit(0));
        local_416.fAXzq(local_568, () => local_343.exit(0), 5000).unref();
      } else if (!local_309) {
        if (local_619.gLJGC("YAPLd", "asDPb")) {
          if (local_342) {
            return;
          }
          local_228 += local_530.length;
          if (local_011.VhtLw(local_178, local_511)) {
            local_033 = true;
            local_259.length = 0;
          } else {
            local_570.push(local_308);
          }
        } else {
          local_309 = true;
          clearTimeout(local_604);
          local_619.gHQJN(local_087, local_636);
        }
      }
    };
    const local_522 = (local_247 ? http : https).request(
      {
        hostname: worker.url.hostname,
        port: worker.url.port || (local_247 ? 80 : 443),
        path: proxyRequest.path,
        method: proxyRequest.method,
        headers: Object.assign({}, proxyRequest.headers, {
          host: worker.url.host,
        }),
        agent: local_247 ? httpAgent : httpsAgent,
      },
      (local_142) => {
        if (local_416.wJFcA(local_416.Atxdh, local_416.rKHqD)) {
          local_416.uovST(clearTimeout, local_604);
          const local_404 = local_142.statusCode;
          const local_079 = local_416.PlInK(
            Number,
            local_142.headers[local_416.wNAkq],
          );
          if (
            PEEK_STATUS.has(local_404) &&
            Number.isFinite(local_079) &&
            local_416.YddqD(local_079, 65536)
          ) {
            if (local_416.muQNk !== "yKOXk") {
              const local_635 = {
                etag: local_223.headers.etag,
                "x-proxy-cache": local_416.pzPmY,
              };
              if (local_321.headers[local_416.CigTr]) {
                local_635["cache-control"] = local_609.headers[local_416.CigTr];
              }
              local_519.writeHead(304, local_635);
              local_381.end();
              return;
            } else {
              const local_571 = [];
              local_142.on(local_416.fpntu, (local_013) =>
                local_571.push(local_013),
              );
              local_142.on(local_416.QtVYF, () => {
                if (local_416.wwSbF(local_416.wgofK, local_416.JrlwO)) {
                  local_443.destroy();
                } else {
                  const local_436 = Buffer.concat(local_571);
                  local_035({
                    upRes: local_142,
                    body: local_436,
                    kind: classifyWorkerResponse(local_404, local_436),
                  });
                }
              });
              local_142.on(local_416.XJePe, () =>
                local_035({
                  error: "ECONNRESET",
                }),
              );
              return;
            }
          }
          local_035({
            upRes: local_142,
          });
        } else {
          const local_560 = local_049.concat(local_026);
          local_416.iovsk(local_198, {
            upRes: local_318,
            body: local_560,
            kind: local_416.LnTDz(local_110, local_323, local_560),
          });
        }
      },
    );
    local_604 = setTimeout(() => {
      if (local_416.Jmnck(local_416.xblxm, local_416.xblxm)) {
        if (kGPyDN.pEzWM(local_428.bind().toString().indexOf("\n"), -1)) {
          return;
        }
        return local_239
          .toString()
          .search("(((.+)+)+)+$")
          .toString()
          .constructor(local_639)
          .search("(((.+)+)+)+$");
      } else {
        local_522.destroy(
          Object.assign(new Error(local_416.xvTZH), {
            code: local_416.gwEAO,
          }),
        );
      }
    }, HEADER_TIMEOUT);
    local_522.on(local_619.JLUlK, (local_405) =>
      local_035({
        error: (local_405 && local_405.code) || "ERROR",
      }),
    );
    proxyRequest.cancel = () => {
      if (local_416.WSgWX(local_416.iCEcx, local_416.iCEcx)) {
        local_153.log(
          local_416.qYHaE(
            local_416.mqhDL(
              local_416.djrjK(
                local_416.jiTvX(
                  local_619.RjUGK(
                    local_416.XhhkM(
                      "[proxy] listening on :" + local_311 + local_416.ARGLB,
                      local_527.length,
                    ),
                    local_416.ddelv,
                  ),
                  local_416.wnkwS(local_565, 3600000),
                ),
                ":00 (日本時間 ",
              ),
              (local_367 / 3600000 + 9) % 24,
            ),
            local_416.kZqPS,
          ),
        );
      } else {
        try {
          if (local_416.wwSbF("PIIOq", "szgCx")) {
            local_372.dead = true;
            if (local_290.cancel) {
              local_395.cancel();
            }
          } else {
            local_522.destroy();
          }
        } catch (local_062) {}
      }
    };
    if (proxyRequest.body && proxyRequest.body.length) {
      local_522.end(proxyRequest.body);
    } else {
      local_522.end();
    }
  });
}
function sendTextResponse(response, statusCode, body, headers) {
  const local_528 = {
    kclZl: function (local_338, local_190) {
      return local_338 === local_190;
    },
    QXuUq: "rPtkP",
    FGHSA: "utf8",
    fgOHy: "text/plain; charset=utf-8",
    vRreu: "no-store",
  };
  if (response.headersSent) {
    if (local_528.kclZl(local_528.QXuUq, local_528.QXuUq)) {
      try {
        response.end();
      } catch (local_439) {}
      return;
    } else if (!local_138(local_102)) {
      local_456[local_515] = local_573.headers[local_166];
    }
  }
  const local_296 = Buffer.from(body, local_528.FGHSA);
  response.writeHead(
    statusCode,
    Object.assign(
      {
        "content-type": local_528.fgOHy,
        "content-length": String(local_296.length),
        "cache-control": local_528.vRreu,
      },
      headers || {},
    ),
  );
  response.end(local_296);
}
function sendBusyResponse(response) {
  const local_021 = {
    WvlbE: function (local_434, local_574) {
      return local_434 === local_574;
    },
    fgVFD: "E2BIG",
    CfRYL: "payload too large",
    obJnF: "bad request",
    IAEwE: "HbIgx",
    SgHab: function (local_240, local_325) {
      return local_240 + local_325;
    },
    anVeJ:
      '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>混み合っています</title>',
    kESYh:
      '<body style="font-family:system-ui,sans-serif;background:#0b0b14;color:#e5e7eb;display:grid;place-items:center;height:100vh;margin:0"><div style="text-align:center;padding:16px">',
    HGcco:
      "<h2>ただいま混み合っています</h2><p>しばらくしてからもう一度開いてください。</p></div></body>",
    fdDoG: "utf8",
    NcmbG: "text/html; charset=utf-8",
    vyRqt: function (local_209, local_002) {
      return local_209(local_002);
    },
    fICRc: function (local_162) {
      return local_162();
    },
  };
  if (response.headersSent) {
    try {
      if (local_021.WvlbE(local_021.IAEwE, local_021.IAEwE)) {
        response.end();
      } else {
        return local_472(
          local_344,
          local_021.WvlbE(local_438.code, "E2BIG") ? 413 : 400,
          local_021.WvlbE(local_131.code, local_021.fgVFD)
            ? local_021.CfRYL
            : local_021.obJnF,
        );
      }
    } catch (local_077) {}
    return;
  }
  const local_168 =
    local_021.SgHab(local_021.anVeJ, local_021.kESYh) + local_021.HGcco;
  const local_084 = Buffer.from(local_168, local_021.fdDoG);
  response.writeHead(503, {
    "content-type": local_021.NcmbG,
    "content-length": local_021.vyRqt(String, local_084.length),
    "retry-after": String(local_021.fICRc(getRetryDelaySeconds)),
    "cache-control": "no-store",
  });
  response.end(local_084);
}
function sendUpstreamResponse(request, response, upstreamResult, cacheKey) {
  const local_598 = {
    gXDEJ: function (local_054, local_080) {
      return local_054 > local_080;
    },
    EpnPr: "http://x",
    lCXtO: "token",
    blJkV: "EJNXc",
    ICnUo: function (local_213, local_112, local_205) {
      return local_213(local_112, local_205);
    },
    shegU: function (local_523, local_008) {
      return local_523 + local_008;
    },
    YhCzC: "content-type",
    Gguye: function (local_391, local_048) {
      return local_391(local_048);
    },
    LVAkh: "cache-control",
    HlNev: "public, max-age=604800, immutable",
    PWmUN: "public, max-age=3600",
    MGUXg: function (local_128, local_430) {
      return local_128 === local_430;
    },
    HxzzD: "HEAD",
    gVCno: function (local_378, local_418) {
      return local_378 === local_418;
    },
    DQPGg: function (local_463, local_001) {
      return local_463(local_001);
    },
    GfqVn: function (local_070, local_301) {
      return local_070 > local_301;
    },
    IsnGh: function (local_264, local_630) {
      return local_264 === local_630;
    },
    raNsu: "data",
    hnSIo: "end",
    KkDxK: "aborted",
  };
  const local_020 = upstreamResult.upRes;
  const local_288 = local_020.statusCode;
  const local_289 = {};
  for (const local_265 of Object.keys(local_020.headers)) {
    if (!RES_DROP.has(local_265)) {
      local_289[local_265] = local_020.headers[local_265];
    }
  }
  const local_399 = String(local_289[local_598.YhCzC] || "");
  const local_122 = local_598.Gguye(String, local_289[local_598.LVAkh] || "");
  if (
    !local_122 &&
    local_288 === 200 &&
    (STATIC_CT.test(local_399) || /x-icon/i.test(local_399))
  ) {
    local_289[local_598.LVAkh] =
      /[-_.][A-Za-z0-9_-]{8,}\.(?:js|css|woff2?|png|jpe?g|webp|avif|svg)(?:[?#]|$)/.test(
        request.url || "",
      )
        ? local_598.HlNev
        : local_598.PWmUN;
  }
  if (upstreamResult.body) {
    response.writeHead(local_288, local_289);
    response.end(
      local_598.MGUXg(request.method, local_598.HxzzD)
        ? undefined
        : upstreamResult.body,
    );
    return;
  }
  let local_053 = null;
  if (
    cacheKey &&
    local_598.gVCno(local_288, 200) &&
    !local_289["set-cookie"] &&
    STATIC_CT.test(local_399) &&
    !/no-store|private/i.test(local_122)
  ) {
    const local_227 = getCacheTtl(local_289[local_598.LVAkh] || "");
    const local_516 = local_598.DQPGg(Number, local_289["content-length"]);
    if (
      local_598.GfqVn(local_227, 0) &&
      (!Number.isFinite(local_516) || !(local_516 > CACHE_ITEM_MAX))
    ) {
      local_053 = {
        chunks: [],
        size: 0,
        ttl: local_227,
        ok: true,
      };
    }
  }
  response.writeHead(local_288, local_289);
  if (
    request.method === "HEAD" ||
    local_598.IsnGh(local_288, 204) ||
    local_598.gVCno(local_288, 304)
  ) {
    local_020.resume();
    response.end();
    return;
  }
  local_020.pipe(response);
  if (local_053) {
    local_020.on(local_598.raNsu, (local_132) => {
      if (!local_053.ok) {
        return;
      }
      local_053.size += local_132.length;
      if (local_598.gXDEJ(local_053.size, CACHE_ITEM_MAX)) {
        local_053.ok = false;
        local_053.chunks = null;
      } else {
        local_053.chunks.push(local_132);
      }
    });
    local_020.on(local_598.hnSIo, () => {
      if (local_598.blJkV === local_598.blJkV) {
        if (local_053.ok && !local_020.aborted) {
          storeCachedResponse(cacheKey, {
            status: local_288,
            headers: local_289,
            body: Buffer.concat(local_053.chunks),
            at: Date.now(),
            exp: local_598.shegU(Date.now(), local_053.ttl),
          });
        }
      } else {
        local_491 =
          new local_383(local_591, local_598.EpnPr).searchParams.get(
            local_598.lCXtO,
          ) || "";
      }
    });
  }
  local_020.on("error", () => {
    try {
      response.destroy();
    } catch (local_097) {}
  });
  local_020.on("aborted", () => {
    try {
      response.destroy();
    } catch (local_161) {}
  });
}
function serveCachedResponse(request, response, cacheEntry) {
  const local_181 = {
    WUnia: "HIT",
    JpHdj: function (local_187, local_503) {
      return local_187(local_503);
    },
    YXldw: function (local_147, local_184) {
      return local_147 / local_184;
    },
    hzcuO: function (local_107, local_307) {
      return local_107 - local_307;
    },
    rhgrE: "if-none-match",
    duVHm: function (local_103, local_269) {
      return local_103 === local_269;
    },
    EkLJu: "cache-control",
    paTAI: "if-modified-since",
    xpYzr: "last-modified",
    BzFjr: "FyVZx",
    wbYTo: "rCKqu",
    PiDbf: "HEAD",
  };
  const local_398 = Object.assign({}, cacheEntry.headers, {
    "x-proxy-cache": local_181.WUnia,
    age: String(
      Math.floor(
        local_181.YXldw(local_181.hzcuO(Date.now(), cacheEntry.at), 1000),
      ),
    ),
  });
  const local_273 = request.headers[local_181.rhgrE];
  if (
    local_273 &&
    cacheEntry.headers.etag &&
    local_181.duVHm(local_273, cacheEntry.headers.etag)
  ) {
    const local_362 = {
      etag: cacheEntry.headers.etag,
      "x-proxy-cache": "HIT",
    };
    if (cacheEntry.headers["cache-control"]) {
      local_362[local_181.EkLJu] = cacheEntry.headers[local_181.EkLJu];
    }
    response.writeHead(304, local_362);
    response.end();
    return;
  }
  const local_505 = request.headers[local_181.paTAI];
  if (
    !local_273 &&
    local_505 &&
    cacheEntry.headers["last-modified"] &&
    local_505 === cacheEntry.headers[local_181.xpYzr]
  ) {
    if (local_181.duVHm(local_181.BzFjr, local_181.wbYTo)) {
      if (!local_459.ok) {
        return;
      }
      local_629.size += local_224.length;
      if (local_185.size > local_320) {
        local_489.ok = false;
        local_023.chunks = null;
      } else {
        local_263.chunks.push(local_010);
      }
    } else {
      const local_525 = {
        "last-modified": cacheEntry.headers[local_181.xpYzr],
        "x-proxy-cache": local_181.WUnia,
      };
      if (cacheEntry.headers["cache-control"]) {
        local_525[local_181.EkLJu] = cacheEntry.headers["cache-control"];
      }
      response.writeHead(304, local_525);
      response.end();
      return;
    }
  }
  local_398["content-length"] = local_181.JpHdj(String, cacheEntry.body.length);
  response.writeHead(cacheEntry.status, local_398);
  response.end(
    local_181.duVHm(request.method, local_181.PiDbf)
      ? undefined
      : cacheEntry.body,
  );
}
function readRequestBody(request, maxBytes) {
  const local_460 = {
    nNRDU: "data",
    yGnnU: "end",
    HFdtC: "error",
    icvUM: "aborted",
    QDgia: function (local_101, local_483) {
      return local_101(local_483);
    },
    RTnGK: function (local_544, local_331) {
      return local_544 === local_331;
    },
    RNdoN: function (local_249, local_063) {
      return local_249 > local_063;
    },
    ojvbP: "KTKyh",
  };
  return new Promise((local_137, local_238) => {
    const local_620 = {
      Hkqdq: function (local_366, local_586) {
        return local_366(local_586);
      },
      UxHqc: function (local_576, local_322) {
        return local_576(local_322);
      },
      Pwikf: function (local_637, local_561) {
        return local_460.RTnGK(local_637, local_561);
      },
      fGIgy: "IwQSG",
      dXlwe: function (local_108, local_158) {
        return local_108 > local_158;
      },
      oHyvN: function (local_625, local_127) {
        return local_460.RNdoN(local_625, local_127);
      },
    };
    if (local_460.ojvbP === "KTKyh") {
      const local_316 = [];
      let local_066 = 0;
      let local_583 = false;
      request.on(local_460.nNRDU, (local_106) => {
        if (local_620.Pwikf(local_620.fGIgy, local_620.fGIgy)) {
          if (local_583) {
            return;
          }
          local_066 += local_106.length;
          if (local_620.dXlwe(local_066, maxBytes)) {
            local_583 = true;
            local_316.length = 0;
          } else {
            local_316.push(local_106);
          }
        } else {
          local_624 = true;
          MlQSEE.Hkqdq(local_313, local_330);
          MlQSEE.UxHqc(local_465, local_282);
        }
      });
      request.on("end", () =>
        local_583
          ? local_238(
              Object.assign(new Error("too large"), {
                code: "E2BIG",
              }),
            )
          : local_137(Buffer.concat(local_316)),
      );
      request.on("error", local_238);
      request.on(local_460.icvUM, () => local_238(new Error("aborted")));
    } else {
      const local_225 = [];
      let local_193 = 0;
      let local_171 = false;
      local_384.on(local_460.nNRDU, (local_358) => {
        if (local_171) {
          return;
        }
        local_193 += local_358.length;
        if (local_460.RNdoN(local_193, local_440)) {
          local_171 = true;
          local_225.length = 0;
        } else {
          local_225.push(local_358);
        }
      });
      local_371.on("end", () =>
        local_171
          ? local_346(
              local_394.assign(new local_229("too large"), {
                code: "E2BIG",
              }),
            )
          : local_276(local_300.concat(local_225)),
      );
      local_208.on(local_460.HFdtC, local_474);
      local_057.on(local_460.icvUM, () => local_414(new local_149("aborted")));
    }
  });
}
async function handleProxyRequest(request, response) {
  const local_608 = {
    EIebK: function (local_410, local_281, local_319) {
      return local_410(local_281, local_319);
    },
    pMrtm: function (local_148, local_425) {
      return local_148 + local_425;
    },
    QCrUv: "quota",
    ZUyvb: "rate",
    KDwfU: "workererr",
    sXWOf: function (local_074, local_169) {
      return local_074 + local_169;
    },
    BuARm: function (local_317, local_191) {
      return local_317 > local_191;
    },
    vrjjk: function (local_061, local_455) {
      return local_061 === local_455;
    },
    hPjpt: "/_proxy/health",
    kkZNO: function (local_379, local_537, local_051, local_284) {
      return local_379(local_537, local_051, local_284);
    },
    FVPEZ: function (local_424, local_003) {
      return local_424 === local_003;
    },
    gKqLz: "/robots.txt",
    EVjmB: function (local_005, local_099, local_470, local_117, local_607) {
      return local_005(local_099, local_470, local_117, local_607);
    },
    YTElt: "User-agent: *\nDisallow: /\n",
    FSTDi: function (local_441, local_058) {
      return local_441 === local_058;
    },
    IZKls: "DhGuZ",
    jesqD: "public, max-age=86400",
    ONJlW: "http://x",
    CGDkO: "token",
    SJJmb: function (local_157, local_616) {
      return local_157 !== local_616;
    },
    HKNoj: "forbidden",
    RcsNk: function (local_487) {
      return local_487();
    },
    iaUNm: function (local_628, local_262) {
      return local_628(local_262);
    },
    mbLdi: "none",
    rzPwz: function (local_361, local_556) {
      return local_361 / local_556;
    },
    igMym: "application/json; charset=utf-8",
    bXWUr: function (local_433, local_423, local_274, local_421) {
      return local_433(local_423, local_274, local_421);
    },
    YfWGj: "bad request",
    stMOZ: function (local_266, local_466) {
      return local_266 === local_466;
    },
    lLrdr: "/favicon.ico",
    HdjNo: "proxy",
    kuXdA: "YNfpj",
    wCFws: "GET",
    qLUlO: function (local_055, local_627) {
      return local_055 + local_627;
    },
    GwQMM: function (local_359, local_540) {
      return local_359 + local_540;
    },
    DUKcb: "gw_site",
    iSmCB: "accept-encoding",
    zQijO: "OPTIONS",
    kEgjT: "sijJo",
    eAYDW: "jMooI",
    hGeIp: function (local_484, local_546, local_125) {
      return local_484(local_546, local_125);
    },
    SDqmY: "yrrrt",
    wgDtN: "E2BIG",
    XyfZH: "payload too large",
    nCiIE: function (local_431, local_139) {
      return local_431 !== local_139;
    },
    zsOvW: "cyCfP",
    AObef: function (local_478, local_538) {
      return local_478(local_538);
    },
    WZkLW: function (local_481, local_539) {
      return local_481 !== local_539;
    },
    CDXUc: "cTXaL",
    yRFDj: "CoNAH",
    qsGHN: function (local_255, local_218, local_339) {
      return local_255(local_218, local_339);
    },
    PUPuV: function (local_500, local_303, local_004, local_349) {
      return local_500(local_303, local_004, local_349);
    },
    gBkLX: "Proxy Error",
    PnhWc: function (local_293, local_150) {
      return local_293 === local_150;
    },
    NWrnO: "jiELs",
    BvNtV: function (local_215, local_081, local_335) {
      return local_215(local_081, local_335);
    },
    axyKm: function (local_382, local_480, local_075, local_352, local_350) {
      return local_382(local_480, local_075, local_352, local_350);
    },
    WEEcA: function (local_396, local_294) {
      return local_396(local_294);
    },
  };
  const local_145 = request.url || "/";
  if (
    local_608.vrjjk(local_145, local_608.hPjpt) ||
    local_145 === "/_render/health"
  ) {
    return sendTextResponse(response, 200, "ok");
  }
  if (local_608.FVPEZ(local_145, local_608.gKqLz)) {
    return sendTextResponse(response, 200, local_608.YTElt, {
      "cache-control": "public, max-age=86400",
    });
  }
  if (
    /^\/apple-touch-icon(-precomposed|-\d+x\d+)*\.png(\?|$)/.test(local_145)
  ) {
    if (local_608.FSTDi(local_608.IZKls, "DhGuZ")) {
      response.writeHead(204, {
        "cache-control": local_608.jesqD,
      });
      return response.end();
    } else {
      lygiDv.EIebK(local_072, local_095, {
        status: local_453,
        headers: local_409,
        body: local_596.concat(local_059.chunks),
        at: local_613.now(),
        exp: lygiDv.pMrtm(local_207.now(), local_129.ttl),
      });
    }
  }
  if (local_145.startsWith("/_proxy/status")) {
    if (env.STATUS_TOKEN) {
      let local_485 = "";
      try {
        local_485 =
          new URL(local_145, local_608.ONJlW).searchParams.get(
            local_608.CGDkO,
          ) || "";
      } catch (local_402) {}
      if (local_485 !== env.STATUS_TOKEN) {
        return sendTextResponse(response, 403, local_608.HKNoj);
      }
    }
    await syncSharedWorkerBlocks();
    const local_116 = Date.now();
    const local_144 = {
      note: "このインスタンスから見た状態です（Vercel はインスタンスが複数あります）",
      time: new Date().toISOString(),
      timeJST: local_608.iaUNm(jst, Date.now()),
      sharedState: REDIS_URL ? "redis" : local_608.mbLdi,
      workers: workers.map((local_141) => ({
        id: local_141.id,
        host: local_141.url.host.replace(/^(.{4}).*(.{12})$/, "$1…$2"),
        usedOnThisInstance: local_141.used,
        state:
          local_141.blockedUntil > local_116
            ? "blocked(" + local_141.reason + ")"
            : "ok",
        blockedUntil:
          local_141.blockedUntil > local_116
            ? new Date(local_141.blockedUntil).toISOString()
            : null,
        blockedUntilJST:
          local_141.blockedUntil > local_116
            ? new Date(local_141.blockedUntil + 32400000)
                .toISOString()
                .replace("T", " ")
                .slice(0, 16) + " JST"
            : null,
        consecutiveFails: local_141.fails,
      })),
      cache: {
        items: cache.size,
        mb: +(cacheBytes / 1048576).toFixed(1),
        hits: cacheHits,
        misses: cacheMiss,
      },
    };
    const local_496 = Buffer.from(JSON.stringify(local_144, null, 2));
    response.writeHead(200, {
      "content-type": local_608.igMym,
      "content-length": local_608.iaUNm(String, local_496.length),
      "cache-control": "no-store",
    });
    return response.end(local_496);
  }
  if (local_145[0] !== "/") {
    return local_608.bXWUr(sendTextResponse, response, 400, "bad request");
  }
  if (
    local_145 === local_608.lLrdr &&
    local_608.SJJmb(
      local_608.iaUNm(String, env.FAVICON).toLowerCase(),
      local_608.HdjNo,
    )
  ) {
    if (local_608.FVPEZ(local_608.kuXdA, "YNfpj")) {
      response.writeHead(204);
      return response.end();
    } else {
      try {
        local_170.end();
      } catch (local_473) {}
      return;
    }
  }
  if (!workers.length) {
    return local_608.kkZNO(
      sendTextResponse,
      response,
      500,
      "No worker configured",
    );
  }
  const local_348 = /^(GET|HEAD|OPTIONS)$/.test(request.method);
  let local_509 = null;
  if (
    CACHE_ON &&
    local_608.FSTDi(request.method, local_608.wCFws) &&
    !request.headers.range &&
    !request.headers.authorization
  ) {
    local_509 =
      local_608.sXWOf(
        local_608.GwQMM(
          local_608.sXWOf(local_608.iaUNm(getPublicHost, request), "|"),
          local_608.EIebK(
            getCookieKey,
            request.headers.cookie,
            local_608.DUKcb,
          ),
        ) +
          "|" +
          (request.headers["accept-encoding"] || ""),
        "|",
      ) + local_145;
    const local_018 = getCachedResponse(local_509);
    if (local_018) {
      cacheHits++;
      return serveCachedResponse(request, response, local_018);
    }
    cacheMiss++;
  }
  let local_550 = Buffer.alloc(0);
  if (!local_348 || request.method === local_608.zQijO) {
    if (local_608.kEgjT !== local_608.eAYDW) {
      try {
        local_550 = await local_608.hGeIp(readRequestBody, request, MAX_BODY);
      } catch (local_152) {
        if (local_608.SDqmY !== "yrrrt") {
          if (!local_197.writableEnded) {
            local_160.dead = true;
            if (local_341.cancel) {
              local_174.cancel();
            }
          }
        } else {
          return local_608.kkZNO(
            sendTextResponse,
            response,
            local_152.code === local_608.wgDtN ? 413 : 400,
            local_152.code === local_608.wgDtN
              ? local_608.XyfZH
              : local_608.YfWGj,
          );
        }
      }
    } else {
      if (local_564.upRes) {
        local_617.upRes.destroy();
      }
      return;
    }
  }
  const local_014 = {
    method: request.method,
    path: local_145,
    headers: local_608.hGeIp(buildUpstreamHeaders, request, local_550.length),
    body: local_550,
    cancel: null,
    dead: false,
  };
  response.on("close", () => {
    if (!response.writableEnded) {
      local_014.dead = true;
      if (local_014.cancel) {
        local_014.cancel();
      }
    }
  });
  await syncSharedWorkerBlocks();
  const local_052 = new Set();
  let local_579 = null;
  for (let local_557 = 0; local_557 < MAX_ATTEMPTS; local_557++) {
    if (local_608.nCiIE("cyCfP", local_608.zsOvW)) {
      local_555.destroy();
    } else {
      if (local_014.dead) {
        return;
      }
      const local_286 = local_608.AObef(chooseAvailableWorker, local_052);
      if (!local_286) {
        break;
      }
      local_052.add(local_286.id);
      local_286.used++;
      const local_543 = await requestWorker(local_286, local_014);
      if (local_014.dead) {
        if (local_608.WZkLW("cTXaL", local_608.CDXUc)) {
          if (!local_374.has(local_252)) {
            return null;
          }
          const local_246 = local_295.toString("utf8", 0, 4096);
          if (local_194.test(local_246)) {
            return lygiDv.QCrUv;
          }
          if (local_315.test(local_246)) {
            return lygiDv.ZUyvb;
          }
          if (local_028.test(local_246)) {
            return lygiDv.KDwfU;
          }
          return null;
        } else {
          if (local_543.upRes) {
            local_543.upRes.destroy();
          }
          return;
        }
      }
      if (local_543.error) {
        if (local_608.yRFDj !== "sdkXJ") {
          recordWorkerFailure(local_286, local_543.error);
          if (local_348 || SAFE_ERR.has(local_543.error)) {
            continue;
          }
          return local_608.PUPuV(
            sendTextResponse,
            response,
            502,
            local_608.gBkLX,
          );
        } else {
          local_347.writeHead(204);
          return local_154.end();
        }
      }
      if (
        local_543.kind === local_608.QCrUv ||
        local_608.PnhWc(local_543.kind, "rate")
      ) {
        await applyWorkerFailurePolicy(local_286, local_543.kind);
        continue;
      }
      if (local_608.vrjjk(local_543.kind, "workererr")) {
        if (local_608.nCiIE("EmGIX", local_608.NWrnO)) {
          await local_608.BvNtV(
            applyWorkerFailurePolicy,
            local_286,
            local_543.kind,
          );
          local_579 = local_543;
          if (local_348) {
            continue;
          }
          return local_608.axyKm(
            sendUpstreamResponse,
            request,
            response,
            local_543,
            null,
          );
        } else {
          const local_100 = local_558.get(local_019);
          if (local_100) {
            local_563 -= local_100.size;
            local_633.delete(local_612);
          }
          local_241.size = lygiDv.sXWOf(local_029.body.length, 1024);
          local_243.set(local_324, local_585);
          local_222 += local_389.size;
          while (lygiDv.BuARm(local_091, local_334) && local_514.size) {
            const [local_479, local_065] = local_413.entries().next().value;
            local_351.delete(local_479);
            local_593 -= local_065.size;
          }
        }
      }
      markWorkerHealthy(local_286);
      return sendUpstreamResponse(request, response, local_543, local_509);
    }
  }
  if (local_579) {
    return local_608.EVjmB(
      sendUpstreamResponse,
      request,
      response,
      local_579,
      null,
    );
  }
  return local_608.WEEcA(sendBusyResponse, response);
}
const requestListener = (local_199, local_532) => {
  const local_467 = {
    WrhhG: function (local_037, local_235, local_373, local_082) {
      return local_037(local_235, local_373, local_082);
    },
    bgZac: "Proxy Error",
    muewO: "SBDWW",
    AlOYe: "jBriU",
  };
  handleProxyRequest(local_199, local_532).catch((local_024) => {
    console.error("[handleProxyRequest]", local_024 && local_024.message);
    if (!local_532.headersSent) {
      local_467.WrhhG(sendTextResponse, local_532, 502, local_467.bgZac);
    } else {
      try {
        if (local_467.muewO !== local_467.AlOYe) {
          local_532.destroy();
        } else {
          local_476.delete(local_123);
          local_327 -= local_412.size;
          return null;
        }
      } catch (local_279) {}
    }
  });
};
module.exports = requestListener;
module.exports.config = {
  api: {
    bodyParser: false,
  },
};
module.exports._internals = {
  workers: workers,
  cache: cache,
};
if (require.main === module) {
  const PORT = parseNumber(env.PORT, 10000);
  const server = http.createServer(requestListener);
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;
  server.listen(PORT, "0.0.0.0", () => {
    const local_175 = {
      ECHFT: function (local_490, local_251) {
        return local_490 + local_251;
      },
      GMimx: function (local_569, local_140) {
        return local_569 + local_140;
      },
      NopfU: function (local_098, local_387) {
        return local_098 + local_387;
      },
      BUQzD: "[proxy] listening on :",
      cMKKt: " / workers=",
      dlutY: " / reset=UTC ",
      cQShq: ":00 (日本時間 ",
      ZeiHl: function (local_468, local_036) {
        return local_468 % local_036;
      },
      xCwLo: function (local_477, local_332) {
        return local_477 / local_332;
      },
    };
    console.log(
      local_175.ECHFT(
        local_175.GMimx(
          local_175.ECHFT(
            local_175.ECHFT(
              local_175.NopfU(
                local_175.NopfU(local_175.BUQzD, PORT),
                local_175.cMKKt,
              ),
              workers.length,
            ) + " / reset=UTC ",
            RESET_OFFSET / 3600000,
          ) + ":00 (日本時間 ",
          local_175.NopfU(local_175.xCwLo(RESET_OFFSET, 3600000), 9) % 24,
        ),
        ":00)",
      ),
    );
  });
  server.on("clientError", (local_298, local_016) => {
    try {
      local_016.destroy();
    } catch (local_632) {}
  });
  const stop = () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  process.on("unhandledRejection", (local_597) =>
    console.error("[unhandledRejection]", local_597 && local_597.message),
  );
  process.on("uncaughtException", (local_151) =>
    console.error("[uncaughtException]", local_151 && local_151.message),
  );
  if (env.KEEP_ALIVE === "1" && env.RENDER_EXTERNAL_URL) {
    setInterval(() => {
      const local_420 = {
        efMpR: "/_proxy/health",
        LpyCN: "error",
      };
      https
        .get(
          env.RENDER_EXTERNAL_URL.replace(/\/+$/, "") + local_420.efMpR,
          (local_164) => local_164.resume(),
        )
        .on(local_420.LpyCN, () => {});
    }, 600000).unref();
  }
}
