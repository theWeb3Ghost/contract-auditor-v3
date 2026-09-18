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
    console.log(`[PUTER] Sending audit request using model: ${model}`);

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

  const rawText = await response.text();

  console.log(`[PUTER] HTTP ${response.status}`);

  if (!response.ok) {
    const error = new Error(
      `Puter returned HTTP ${response.status}: ${rawText.slice(0, 1000)}`
    );
    error.code = response.status === 429 ? 'RATE_LIMIT' : 'PUTER_ERROR';
    error.httpStatus = response.status;
    error.responseText = rawText;
    throw error;
  }

  let json;

  try {
    json = JSON.parse(rawText);
  } catch {
    const error = new Error(`Puter returned invalid JSON: ${rawText.slice(0, 1000)}`);
    error.code = 'INVALID_RESPONSE';
    error.responseText = rawText;
    throw error;
  }

  // Structured provider error (works for both {"error": "..."} and {"error": {...}})
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

  const result =
    json?.result?.message?.content ??
    json?.message?.content ??
    json?.result?.text ??
    json?.text ??
    '';

  const text = contentToText(result);

  if (!text.trim()) {
    const error = new Error('Puter returned an empty audit response');
    error.code = 'EMPTY_RESPONSE';
    error.responseText = rawText;
    throw error;
  }

  console.log(`[PUTER] Audit response received using model: ${model}`);

  return {
    result: text.trim(),
    truncated: false
  };
}

module.exports = { runPuterAudit };
