// api/puter.js
const { fetch, Agent } = require('undici');

const PUTER_TIMEOUT = 30 * 60 * 1000;
const PUTER_ENDPOINT = 'https://api.puter.com/drivers/call';

const puterDispatcher = new Agent({
  headersTimeout: PUTER_TIMEOUT,
  bodyTimeout: PUTER_TIMEOUT,
  connect: { timeout: 50000 }
});

function contentToText(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.map(p => (typeof p === 'string' ? p : p?.text || '')).join('');
  }
  return content == null ? '' : String(content);
}

function httpError(status, rawText) {
  const error = new Error(
    `Puter returned HTTP ${status}: ${String(rawText || '').slice(0, 1000)}`
  );

  if (status === 429)                        error.code = 'RATE_LIMIT';
  else if (status === 401 || status === 403) error.code = 'INVALID_KEY';
  else if (status >= 500)                    error.code = 'PROVIDER_ERROR';
  else                                       error.code = 'PUTER_ERROR';

  error.httpStatus = status;
  error.responseText = String(rawText || '');
  return error;
}

// ----------------------------------------------------------
// NON-STREAMED JSON FALLBACK PARSER
// ----------------------------------------------------------

function extractFromJson(json) {
  if (json?.error || json?.success === false) {
    const detail =
      typeof json.error === 'string'
        ? json.error
        : json.error?.message || JSON.stringify(json.error || json);

    const error = new Error(`Puter API error: ${detail}`);
    error.code = 'PUTER_ERROR';
    error.apiError = json.error || json;
    throw error;
  }

  const result = json?.result;
  const message = result?.message ?? result ?? {};

  return {
    content: contentToText(message.content),
    reasoning: contentToText(
      message.reasoning ||
      message.reasoning_details?.[0]?.text ||
      ''
    ),
    finishReason: result?.finish_reason || result?.native_finish_reason || null,
    usage: result?.usage || null
  };
}

// ----------------------------------------------------------
// STREAM CONSUMER
//
// Handles Puter's native typed-event NDJSON format:
//
//   {"type":"reasoning","reasoning":"..."}
//   {"type":"text","text":"..."}
//   {"type":"usage","usage":{...}}
//
// Also tolerates OpenAI SSE framing (data: {...}) and a
// non-streamed JSON body, in case Puter changes behavior.
// ----------------------------------------------------------

async function consumeStream(response) {
  const decoder = new TextDecoder();
  let buffer = '';

  let content = '';
  let reasoning = '';
  let usage = null;
  let finishReason = null;

  const startedAt = Date.now();
  let firstByteMs = null;
  let lastLogAt = Date.now();

  function handleEvent(evt) {
    if (!evt || typeof evt !== 'object') return;

    // --- Puter native typed events ---
    if (evt.type === 'text') {
      content += evt.text || '';
      return;
    }
    if (evt.type === 'reasoning') {
      reasoning += evt.reasoning || '';
      return;
    }
    if (evt.type === 'usage') {
      usage = evt.usage || usage;
      return;
    }

    // --- OpenAI-compatible delta shape (defensive) ---
    const choice = evt.choices?.[0];
    if (choice?.delta) {
      if (choice.delta.content)   content += choice.delta.content;
      if (choice.delta.reasoning) reasoning += choice.delta.reasoning;
      if (choice.finish_reason)   finishReason = choice.finish_reason;
      return;
    }

    // --- Full non-streamed completion (defensive) ---
    if (evt.result || evt.message || evt.error || evt.success === false) {
      const parsed = extractFromJson(evt);
      content += parsed.content;
      reasoning += parsed.reasoning;
      usage = parsed.usage || usage;
      finishReason = parsed.finishReason || finishReason;
    }
  }

  function handleLine(rawLine) {
    let line = rawLine.trim();
    if (!line) return;
    if (line.startsWith('data:')) line = line.slice(5).trim(); // tolerate SSE framing
    if (!line || line === '[DONE]') return;

    let evt;
    try {
      evt = JSON.parse(line);
    } catch {
      return; // keep-alive noise or partial line — ignore
    }
    handleEvent(evt);
  }

  for await (const chunk of response.body) {
    if (firstByteMs === null) {
      firstByteMs = Date.now() - startedAt;
      console.log(`[PUTER] First stream byte after ${(firstByteMs / 1000).toFixed(1)}s`);
    }

    buffer += decoder.decode(chunk, { stream: true });

    let idx;
    while ((idx = buffer.indexOf('\n')) !== -1) {
      handleLine(buffer.slice(0, idx));
      buffer = buffer.slice(idx + 1);
    }

    const nowMs = Date.now();
    if (nowMs - lastLogAt > 15000) {
      lastLogAt = nowMs;
      console.log(
        `[PUTER] streaming... content=${content.length} chars, ` +
        `reasoning=${reasoning.length} chars, ` +
        `${((nowMs - startedAt) / 1000).toFixed(0)}s elapsed`
      );
    }
  }

  // IMPORTANT: the final line may not end with a newline
  // (your curl output ends at the usage event with no trailing \n).
  if (buffer.trim()) {
    handleLine(buffer);
  }

  console.log(
    `[PUTER] Stream complete: content=${content.length} chars, ` +
    `reasoning=${reasoning.length} chars, ` +
    `${((Date.now() - startedAt) / 1000).toFixed(1)}s total`
  );

  return { content, reasoning, usage, finishReason };
}

// ----------------------------------------------------------
// MAIN
// ----------------------------------------------------------

async function runPuterAudit({ systemPrompt, userMessage, model }) {
  const authToken = process.env.PUTER_AUTH_TOKEN;

  if (!authToken) {
    const error = new Error('PUTER_AUTH_TOKEN is missing from the server environment');
    error.code = 'PUTER_AUTH_MISSING';
    throw error;
  }

  if (!model || typeof model !== 'string' || !model.trim()) {
    const error = new Error('Puter model is required');
    error.code = 'PUTER_MODEL_MISSING';
    throw error;
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), PUTER_TIMEOUT);

  let response;

  try {
    console.log(`[PUTER] Sending audit request (streaming) using model: ${model}`);

    response = await fetch(PUTER_ENDPOINT, {
      method: 'POST',
      dispatcher: puterDispatcher,
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${authToken}`
      },
      body: JSON.stringify({
        interface: 'puter-chat-completion',
        driver: 'ai-chat',
        test_mode: false,
        method: 'complete',
        args: {
          model: model.trim(),
          stream: true,
          messages: [
            { role: 'system', content: systemPrompt || '' },
            { role: 'user', content: userMessage }
          ]
        }
      })
    });
  } catch (error) {
    if (error?.name === 'AbortError' || error?.name === 'TimeoutError') {
      const timeoutError = new Error(
        `Puter request timed out after ${PUTER_TIMEOUT / 60000} minutes`
      );
      timeoutError.code = 'LLM_TIMEOUT';
      throw timeoutError;
    }

    const networkError = new Error(
      `Puter network request failed: ${error?.message || error}`
    );
    networkError.code = 'NETWORK_ERROR';
    networkError.originalError = error;
    throw networkError;
  } finally {
    clearTimeout(timeoutId);
  }

  if (!response.ok) {
    const rawText = await response.text();
    throw httpError(response.status, rawText);
  }

  console.log(`[PUTER] content-type: ${response.headers.get('content-type')}`);

  let parsed;

  try {
    // Consume as a stream regardless — the line handler gracefully
    // covers both typed events AND a single JSON blob fallback.
    parsed = await consumeStream(response);
  } catch (error) {
    if (error?.name === 'AbortError' || error?.name === 'TimeoutError') {
      const timeoutError = new Error(
        `Puter stream timed out after ${PUTER_TIMEOUT / 60000} minutes`
      );
      timeoutError.code = 'LLM_TIMEOUT';
      throw timeoutError;
    }
    throw error;
  }

  if (parsed.usage) {
    console.log(`[PUTER] usage: ${JSON.stringify(parsed.usage)}`);
  }

  if (!parsed.content.trim()) {
    const error = new Error(
      'Puter returned an empty audit response' +
      (parsed.reasoning.trim()
        ? ` (model produced reasoning=${parsed.reasoning.length} chars but no content)`
        : '')
    );
    error.code = 'EMPTY_RESPONSE';
    throw error;
  }

  console.log(`[PUTER] Audit response received using model: ${model}`);

  return {
    result: parsed.content.trim(),
    truncated: false // no finish_reason in Puter's stream format
  };
}

module.exports = { runPuterAudit };
