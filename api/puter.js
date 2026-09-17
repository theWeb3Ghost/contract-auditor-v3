// api/puter.js  —  patched
//
// Fixes three defects found in the 2026-09-17 COM failure:
//
//   1. init() was called per request, leaking 3 TLSSockets + ~1.5MB heap each
//      time (measured on @heyputer/puter.js@2.6.3). Now a module-scope singleton.
//
//   2. The request was non-streaming, so nothing came back for >5 minutes and an
//      upstream idle timeout killed the socket at exactly 300s. Now streams.
//
//   3. The SDK rejects with a raw XMLHttpRequest object (reject(this) in the
//      bundle's driver error handler), which has no .name/.message/.stack. That
//      is why the log read "RAW ERROR: se {}". Now normalized into a real Error
//      with a distinguishable code.

const { init } = require("@heyputer/puter.js/src/init.cjs");

// ============================================================
// SINGLETON CLIENT
// ============================================================
//
// init() evaluates the 400KB browser bundle in a fresh node:vm context and
// opens persistent connections that are never closed. Calling it per request
// leaks handles until the process runs out of file descriptors -- which itself
// surfaces as status:0 network errors, i.e. more of the error we are fixing.

let cachedPuter = null;
let cachedTokenFingerprint = null;

function fingerprint(token) {
  // Enough to detect a token change without logging the token.
  return `${token.length}:${token.slice(0, 8)}:${token.slice(-6)}`;
}

function getPuterClient(authToken) {
  const fp = fingerprint(authToken);

  if (cachedPuter && cachedTokenFingerprint === fp) {
    return cachedPuter;
  }

  if (cachedPuter) {
    console.warn(
      "[PUTER] Auth token changed; re-initializing client. " +
      "Note: the SDK does not expose a teardown, so the previous " +
      "client's sockets remain open until process exit."
    );
  }

  cachedPuter = init(authToken);
  cachedTokenFingerprint = fp;

  console.log(
    `[PUTER] Client initialized (env=${cachedPuter?.env}, origin=${cachedPuter?.APIOrigin})`
  );

  return cachedPuter;
}

// ============================================================
// ERROR NORMALIZATION
// ============================================================
//
// The SDK's driver error path does `reject(this)` where `this` is the XHR
// polyfill instance. Standard Error properties are absent. Detect that shape
// explicitly rather than relying on error.message.

function normalizePuterError(raw, elapsedMs) {
  const isXhrShaped =
    raw &&
    typeof raw === "object" &&
    "status" in raw &&
    "responseURL" in raw;

  // status 0 with no response means the XHR "error" event fired: the socket
  // died before any response headers arrived.
  const isNetworkFailure =
    isXhrShaped &&
    Number(raw.status) === 0 &&
    (raw.response === null || raw.response === undefined);

  if (isNetworkFailure) {
    const seconds = (elapsedMs / 1000).toFixed(1);

    const error = new Error(
      `Puter connection failed after ${seconds}s with no response ` +
      `(XHR status 0). The socket was closed before response headers ` +
      `arrived -- typically an upstream idle timeout on a long, ` +
      `non-streaming completion, or local socket/FD exhaustion.`
    );

    error.code = "PUTER_NETWORK_ERROR";
    error.elapsedMs = elapsedMs;
    error.xhrStatus = 0;
    error.endpoint = raw.responseURL || null;
    error.retryable = true;

    return error;
  }

  if (isXhrShaped && Number(raw.status) > 0) {
    const error = new Error(
      `Puter HTTP ${raw.status} ${raw.statusText || ""}`.trim()
    );

    error.code =
      Number(raw.status) === 429
        ? "RATE_LIMIT"
        : Number(raw.status) === 401
        ? "INVALID_KEY"
        : Number(raw.status) >= 500
        ? "PROVIDER_ERROR"
        : "PUTER_ERROR";

    error.httpStatus = Number(raw.status);
    error.elapsedMs = elapsedMs;
    error.retryable = ["RATE_LIMIT", "PROVIDER_ERROR"].includes(error.code);

    return error;
  }

  // Genuine Error, or a plain {message, code} object from the driver.
  const error = new Error(
    `Puter AI request failed: ${
      raw?.message || raw?.error?.message || String(raw)
    }`
  );

  error.code = raw?.code || "PUTER_ERROR";
  error.elapsedMs = elapsedMs;
  error.retryable = false;

  return error;
}

// ============================================================
// STREAM COLLECTION
// ============================================================
//
// With stream:true the SDK returns an async iterable of chunks instead of a
// single response object. Streaming is what actually prevents the 300s idle
// cutoff, because bytes keep flowing while the model generates.

async function collectStream(stream, { model, onProgress }) {
  let text = "";
  let chunks = 0;
  let lastLog = Date.now();

  for await (const part of stream) {
    const piece =
      typeof part === "string"
        ? part
        : part?.text ??
          part?.message?.content ??
          part?.delta?.content ??
          "";

    if (piece) {
      text += piece;
      chunks++;
    }

    // Heartbeat so a slow audit is visibly alive in the logs.
    if (Date.now() - lastLog > 30000) {
      lastLog = Date.now();
      console.log(
        `[PUTER] streaming ${model}: ${chunks} chunks, ${text.length} chars`
      );
      if (typeof onProgress === "function") {
        onProgress({ chars: text.length, chunks });
      }
    }
  }

  return { text, chunks };
}

// ============================================================
// MAIN
// ============================================================

async function runPuterAudit({
  systemPrompt,
  userMessage,
  model,
  stream = true,
  maxRetries = 3,
  onProgress
}) {
  const authToken = process.env.PUTER_AUTH_TOKEN;

  if (!authToken) {
    const error = new Error(
      "PUTER_AUTH_TOKEN is missing from the server environment"
    );
    error.code = "PUTER_AUTH_MISSING";
    throw error;
  }

  if (!model || typeof model !== "string" || !model.trim()) {
    const error = new Error("Puter model is required");
    error.code = "PUTER_MODEL_MISSING";
    throw error;
  }

  const puter = getPuterClient(authToken);
  const resolvedModel = model.trim();

  const messages = [
    { role: "system", content: systemPrompt || "" },
    { role: "user", content: userMessage }
  ];

  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    const startedAt = Date.now();

    try {
      console.log(
        `[PUTER] Audit request (attempt ${attempt}/${maxRetries}) ` +
        `model=${resolvedModel} stream=${stream}`
      );

      const response = await puter.ai.chat(messages, {
        model: resolvedModel,
        normalize: true,
        stream
      });

      let result;

      if (stream && response && typeof response[Symbol.asyncIterator] === "function") {
        const collected = await collectStream(response, {
          model: resolvedModel,
          onProgress
        });

        result = collected.text;

        console.log(
          `[PUTER] Stream complete: ${collected.chunks} chunks, ` +
          `${result.length} chars in ${((Date.now() - startedAt) / 1000).toFixed(1)}s`
        );
      } else {
        // Driver ignored stream, or stream was explicitly disabled.
        result = response?.message?.content ?? response?.text ?? "";
      }

      if (!result || !String(result).trim()) {
        const error = new Error("Puter returned an empty audit response");
        error.code = "EMPTY_RESPONSE";
        throw error;
      }

      return {
        result: String(result).trim(),
        truncated: false
      };

    } catch (raw) {
      const elapsedMs = Date.now() - startedAt;

      // EMPTY_RESPONSE is handled by batch.js key rotation; pass it through.
      if (raw?.code === "EMPTY_RESPONSE") {
        throw raw;
      }

      const error = normalizePuterError(raw, elapsedMs);
      lastError = error;

      console.error(
        `[PUTER] Attempt ${attempt}/${maxRetries} failed after ` +
        `${(elapsedMs / 1000).toFixed(1)}s: [${error.code}] ${error.message}`
      );

      // Keep the raw shape available at debug level only; it stringifies to
      // near-nothing, which is what made the original log unreadable.
      if (process.env.PUTER_DEBUG === "1") {
        console.error("[PUTER] raw rejection keys:", Object.keys(raw || {}));
      }

      if (!error.retryable || attempt === maxRetries) {
        throw error;
      }

      const delay = Math.min(30000, 2000 * Math.pow(2, attempt - 1));
      console.warn(`[PUTER] Retrying in ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
    }
  }

  throw lastError || new Error("Puter AI request failed");
}

module.exports = {
  runPuterAudit
};
