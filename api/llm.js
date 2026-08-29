
// api/llm.js

const { fetch, Agent } = require('undici');

const MAX_CHARS = 60000;
const LLM_TIMEOUT = 10 * 60 * 1000;

const llmDispatcher = new Agent({
  headersTimeout: LLM_TIMEOUT,
  bodyTimeout: LLM_TIMEOUT,

  connect: {
    timeout: 30000
  }
});


function isRateLimitError(status, text) {
  const lower = String(text || '').toLowerCase();

  return (
    status === 429 ||
    lower.includes('rate limit') ||
    lower.includes('rate_limit') ||
    lower.includes('too many requests')
  );
}


async function runLLMAudit({
  source,
  systemPrompt,
  model,
  contractName,
  address,
  llmUrl,
  apiKey
}) {

  if (!source || typeof source !== 'string') {
    throw new Error('Invalid Solidity source');
  }

  let src = source;
  let truncated = false;

  if (src.length > MAX_CHARS) {
    src = src.slice(0, MAX_CHARS);
    truncated = true;
  }


  const endpoint =
    llmUrl &&
    typeof llmUrl === 'string' &&
    /^https?:\/\//.test(llmUrl)
      ? llmUrl
      : 'https://api.openai.com/v1/chat/completions';


  const userMessage = `
You are auditing the following Solidity smart contract.

Contract Name: ${contractName || 'Unknown'}

Contract Address: ${address || 'Unknown'}

SOURCE CODE:

\`\`\`solidity
${src}
\`\`\`

${truncated
  ? '\nWARNING: Source code was truncated because it exceeded the context limit.'
  : ''
}
`;


  const controller = new AbortController();

  const timeoutId = setTimeout(() => {
    controller.abort();
  }, LLM_TIMEOUT);


  let response;

  try {

    console.log(
      `[LLM] Sending audit request for ${address}`
    );

    response = await fetch(endpoint, {
      method: 'POST',

      dispatcher: llmDispatcher,

      signal: controller.signal,

      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
        'Accept': 'application/json'
      },

      body: JSON.stringify({
        model: model || 'gpt-4o-mini',

        messages: [
          {
            role: 'system',
            content: systemPrompt
          },
          {
            role: 'user',
            content: userMessage
          }
        ]
      })
    });

  } catch (error) {

    if (
      error?.name === 'AbortError' ||
      error?.name === 'TimeoutError'
    ) {
      const timeoutError = new Error(
        `LLM request timed out after ${LLM_TIMEOUT / 60000} minutes`
      );

      timeoutError.code = 'LLM_TIMEOUT';

      throw timeoutError;
    }

    throw error;

  } finally {

    clearTimeout(timeoutId);

  }


  const rawText = await response.text();


  console.log(
    `[LLM] ${address} HTTP ${response.status}`
  );


  if (!response.ok) {

    const error = new Error(
      `LLM API returned HTTP ${response.status}: ${rawText.slice(0, 1000)}`
    );

    error.httpStatus = response.status;
    error.responseText = rawText;

    if (isRateLimitError(response.status, rawText)) {
      error.code = 'RATE_LIMIT';
    }

    throw error;
  }


  let json;

  try {
    json = JSON.parse(rawText);
  } catch {
    const error = new Error(
      `LLM returned invalid JSON: ${rawText.slice(0, 1000)}`
    );

    error.code = 'INVALID_RESPONSE';

    throw error;
  }


  if (json.error) {

    const message =
      json.error.message ||
      JSON.stringify(json.error);

    const error = new Error(message);

    error.apiError = json.error;

    if (
      isRateLimitError(
        response.status,
        message
      )
    ) {
      error.code = 'RATE_LIMIT';
    }

    throw error;
  }


  const result =
    json?.choices?.[0]?.message?.content ||
    json?.choices?.[0]?.text ||
    json?.output_text ||
    '';


  if (!result || !String(result).trim()) {

    const error = new Error(
      'LLM returned an empty audit response'
    );

    error.code = 'EMPTY_RESPONSE';

    throw error;
  }


  return {
    result: String(result).trim(),
    truncated
  };
}


module.exports = {
  runLLMAudit,
  isRateLimitError
};
