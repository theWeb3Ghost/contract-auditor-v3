
// api/llm.js

const { fetch, Agent } = require('undici');

const MAX_CHARS = 60000;

// Keep the long timeout because some audits can take several minutes.
const LLM_TIMEOUT = 10 * 60 * 1000;

// Free-tier friendly.
// The batch worker also enforces the spacing between requests.
const LLM_REQUEST_TIMEOUT = LLM_TIMEOUT;

const llmDispatcher = new Agent({
  headersTimeout: LLM_REQUEST_TIMEOUT,
  bodyTimeout: LLM_REQUEST_TIMEOUT,

  connect: {
    timeout: 30000
  }
});


// ============================================================
// ERROR CLASSIFICATION
// ============================================================

function classifyProviderError(status, text) {

  const lower =
    String(text || '').toLowerCase();


  // ----------------------------------------------------------
  // HTTP rate limiting
  // ----------------------------------------------------------

  if (
    status === 429 ||
    lower.includes('rate limit') ||
    lower.includes('rate_limit') ||
    lower.includes('too many requests') ||
    lower.includes('requests per second') ||
    lower.includes('request limit')
  ) {
    return 'RATE_LIMIT';
  }


  // ----------------------------------------------------------
  // Free quota / account quota exhaustion
  // ----------------------------------------------------------

  if (
    lower.includes('free quota') ||
    lower.includes('free resources') ||
    lower.includes('only try 10 times') ||
    lower.includes('only try 10') ||
    lower.includes('not been recharged') ||
    lower.includes('have not been recharged') ||
    lower.includes('recharged') ||
    lower.includes('recharge') ||
    lower.includes('topup') ||
    lower.includes('top-up') ||
    lower.includes('quota exceeded') ||
    lower.includes('quota has been exceeded') ||
    lower.includes('insufficient quota')
  ) {
    return 'QUOTA';
  }


  return null;
}


function createProviderError(
  code,
  message,
  extra = {}
) {

  const error =
    new Error(message);

  error.code = code;

  Object.assign(
    error,
    extra
  );

  return error;
}


function isRateLimitError(
  status,
  text
) {

  return (
    classifyProviderError(
      status,
      text
    ) === 'RATE_LIMIT'
  );
}


function isQuotaError(
  status,
  text
) {

  return (
    classifyProviderError(
      status,
      text
    ) === 'QUOTA'
  );
}


// ============================================================
// RUN LLM AUDIT
// ============================================================

async function runLLMAudit({
  source,
  systemPrompt,
  model,
  contractName,
  address,
  llmUrl,
  apiKey
}) {

  if (
    !source ||
    typeof source !== 'string'
  ) {

    throw new Error(
      'Invalid Solidity source'
    );
  }


  if (!apiKey) {

    throw new Error(
      'LLM API key is missing'
    );
  }


  let src =
    source;

  let truncated =
    false;


  if (
    src.length >
    MAX_CHARS
  ) {

    src =
      src.slice(
        0,
        MAX_CHARS
      );

    truncated =
      true;
  }


  const endpoint =
    llmUrl &&
    typeof llmUrl === 'string' &&
    /^https?:\/\//i.test(
      llmUrl
    )
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

${
  truncated
    ? '\nWARNING: Source code was truncated because it exceeded the configured source limit.\n'
    : ''
}
`;


  const controller =
    new AbortController();


  const timeoutId =
    setTimeout(
      () => {
        controller.abort();
      },
      LLM_TIMEOUT
    );


  let response;


  try {

    console.log(
      `[LLM] Sending audit request for ${address}`
    );


    response =
      await fetch(
        endpoint,
        {
          method: 'POST',

          dispatcher:
            llmDispatcher,

          signal:
            controller.signal,

          headers: {
            'Content-Type':
              'application/json',

            'Authorization':
              `Bearer ${apiKey}`,

            'Accept':
              'application/json'
          },

          body:
            JSON.stringify({
              model:
                model ||
                'gpt-4o-mini',

              messages: [
                {
                  role: 'system',
                  content:
                    systemPrompt
                },

                {
                  role: 'user',
                  content:
                    userMessage
                }
              ]
            })
        }
      );

  } catch (error) {

    if (
      error?.name ===
        'AbortError' ||
      error?.name ===
        'TimeoutError'
    ) {

      const timeoutError =
        new Error(
          `LLM request timed out after ${LLM_TIMEOUT / 60000} minutes`
        );

      timeoutError.code =
        'LLM_TIMEOUT';

      throw timeoutError;
    }


    throw error;

  } finally {

    clearTimeout(
      timeoutId
    );
  }


  const rawText =
    await response.text();


  console.log(
    `[LLM] ${address} HTTP ${response.status}`
  );


  // ==========================================================
  // CLASSIFY PROVIDER RESPONSE BEFORE ACCEPTING IT
  // ==========================================================

  const providerCode =
    classifyProviderError(
      response.status,
      rawText
    );


  if (
    providerCode ===
    'RATE_LIMIT'
  ) {

    throw createProviderError(
      'RATE_LIMIT',
      `LLM provider rate limit: ${rawText.slice(0, 1000)}`,
      {
        httpStatus:
          response.status,

        responseText:
          rawText
      }
    );
  }


  if (
    providerCode ===
    'QUOTA'
  ) {

    throw createProviderError(
      'QUOTA',
      `LLM provider quota exhausted: ${rawText.slice(0, 1000)}`,
      {
        httpStatus:
          response.status,

        responseText:
          rawText
      }
    );
  }


  // ==========================================================
  // HTTP ERRORS
  // ==========================================================

  if (
    !response.ok
  ) {

    const error =
      new Error(
        `LLM API returned HTTP ${response.status}: ${rawText.slice(0, 1000)}`
      );

    error.httpStatus =
      response.status;

    error.responseText =
      rawText;


    if (
      isRateLimitError(
        response.status,
        rawText
      )
    ) {

      error.code =
        'RATE_LIMIT';

    } else if (
      isQuotaError(
        response.status,
        rawText
      )
    ) {

      error.code =
        'QUOTA';
    }


    throw error;
  }


  // ==========================================================
  // JSON PARSING
  // ==========================================================

  let json;


  try {

    json =
      JSON.parse(
        rawText
      );

  } catch {

    const error =
      new Error(
        `LLM returned invalid JSON: ${rawText.slice(0, 1000)}`
      );

    error.code =
      'INVALID_RESPONSE';

    throw error;
  }


  // ==========================================================
  // PROVIDER-LEVEL JSON ERROR
  // ==========================================================

  if (
    json.error
  ) {

    const message =
      json.error.message ||
      JSON.stringify(
        json.error
      );


    const providerCode =
      classifyProviderError(
        response.status,
        message
      );


    const error =
      new Error(
        message
      );


    error.apiError =
      json.error;


    error.httpStatus =
      response.status;


    error.responseText =
      rawText;


    if (
      providerCode
    ) {

      error.code =
        providerCode;
    }


    throw error;
  }


  // ==========================================================
  // EXTRACT RESULT
  // ==========================================================

  const result =
    json?.choices?.[0]?.message?.content ||
    json?.choices?.[0]?.text ||
    json?.output_text ||
    '';


  if (
    !result ||
    !String(result).trim()
  ) {

    const error =
      new Error(
        'LLM returned an empty audit response'
      );

    error.code =
      'EMPTY_RESPONSE';

    throw error;
  }


  // ==========================================================
  // FINAL SAFETY CHECK
  //
  // Prevent provider quota/rate-limit messages from ever
  // becoming a "completed audit".
  // ==========================================================

  const resultProviderCode =
    classifyProviderError(
      response.status,
      result
    );


  if (
    resultProviderCode ===
    'RATE_LIMIT'
  ) {

    throw createProviderError(
      'RATE_LIMIT',
      String(result).slice(
        0,
        2000
      ),
      {
        httpStatus:
          response.status,

        responseText:
          rawText
      }
    );
  }


  if (
    resultProviderCode ===
    'QUOTA'
  ) {

    throw createProviderError(
      'QUOTA',
      String(result).slice(
        0,
        2000
      ),
      {
        httpStatus:
          response.status,

        responseText:
          rawText
      }
    );
  }


  return {
    result:
      String(result).trim(),

    truncated
  };
}


// ============================================================
// EXPORTS
// ============================================================

module.exports = {
  runLLMAudit,
  isRateLimitError,
  isQuotaError
};
