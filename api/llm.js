// api/llm.js

const { fetch, Agent } = require('undici');

const MAX_CHARS = 60000;

// Keep the long timeout because some audits can take several minutes.
const LLM_TIMEOUT = 10 * 60 * 1000;

const llmDispatcher = new Agent({
  headersTimeout: LLM_TIMEOUT,
  bodyTimeout: LLM_TIMEOUT,

  connect: {
    timeout: 30000
  }
});

// ============================================================
// ERROR CLASSIFICATION
// ============================================================
//
// IMPORTANT:
//
// We classify PROVIDER ERRORS from:
//
//   1. HTTP status
//   2. Provider error response bodies
//   3. Structured JSON error objects
//
// We DO NOT classify successful LLM audit content.
//
// A Solidity audit can legitimately contain words such as:
//
//   "unauthorized"
//   "rate limit"
//   "quota"
//
// Those words inside an actual audit MUST NEVER disable a key.
// ============================================================

function classifyProviderError(status, text) {

  const lower =
    String(text || '').toLowerCase();

  const httpStatus =
    Number(status);

  // ----------------------------------------------------------
  // INVALID API KEY
  //
  // Require an authentication HTTP status AND explicit
  // API-key wording.
  // ----------------------------------------------------------

  if (
    httpStatus === 401 &&
    (
      lower.includes('invalid api key') ||
      lower.includes('invalid_api_key') ||
      lower.includes('incorrect api key') ||
      lower.includes('api key is invalid') ||
      lower.includes('api key not valid') ||
      lower.includes('invalid authentication key')
    )
  ) {
    return 'INVALID_KEY';
  }

  // ----------------------------------------------------------
  // RATE LIMIT
  // ----------------------------------------------------------

  if (
    httpStatus === 429 ||
    lower.includes('rate limit') ||
    lower.includes('rate_limit') ||
    lower.includes('too many requests') ||
    lower.includes('requests per second') ||
    lower.includes('request limit') ||
    lower.includes('requests per minute') ||
    lower.includes('rpm limit') ||
    lower.includes('tpm limit') ||
    lower.includes('rate-limit')
  ) {
    return 'RATE_LIMIT';
  }

  // ----------------------------------------------------------
  // QUOTA / CREDITS
  // ----------------------------------------------------------

  if (
    lower.includes('free quota') ||
    lower.includes('free resources') ||
    lower.includes('quota exceeded') ||
    lower.includes('quota has been exceeded') ||
    lower.includes('insufficient quota') ||
    lower.includes('quota exhausted') ||
    lower.includes('quota is exhausted') ||
    lower.includes('credit balance') ||
    lower.includes('credits exhausted') ||
    lower.includes('credits exceeded') ||
    lower.includes('free credits') ||
    lower.includes('billing limit') ||
    lower.includes('billing quota') ||
    lower.includes('only try 10 times') ||
    lower.includes('only try 10')
  ) {
    return 'QUOTA';
  }

  // ----------------------------------------------------------
  // SERVER ERROR
  // ----------------------------------------------------------

  if (
    httpStatus >= 500 &&
    httpStatus <= 599
  ) {
    return 'PROVIDER_ERROR';
  }

  return null;
}


// ============================================================
// CREATE STANDARD ERROR
// ============================================================

function createProviderError(
  code,
  message,
  extra = {}
) {

  const error =
    new Error(message);

  error.code =
    code;

  Object.assign(
    error,
    extra
  );

  return error;
}


// ============================================================
// EXTRACT STRUCTURED PROVIDER ERROR
// ============================================================
//
// Handles OpenAI-compatible providers returning:
//
// {
//   "error": {
//      "message": "...",
//      "code": "...",
//      "type": "..."
//   }
// }
//
// Also supports simpler:
//
// {
//   "error": "some error"
// }
// ============================================================

function extractStructuredProviderError(json) {

  if (
    !json ||
    typeof json !== 'object' ||
    !json.error
  ) {
    return null;
  }

  if (
    typeof json.error === 'string'
  ) {
    return {
      message:
        json.error,

      code:
        null,

      type:
        null
    };
  }

  if (
    typeof json.error === 'object'
  ) {
    return {
      message:
        json.error.message ||
        json.error.error ||
        JSON.stringify(json.error),

      code:
        json.error.code ||
        null,

      type:
        json.error.type ||
        null
    };
  }

  return {
    message:
      String(json.error),

    code:
      null,

    type:
      null
  };
}


// ============================================================
// ERROR HELPERS
// ============================================================

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


function isInvalidKeyError(
  status,
  text
) {
  return (
    classifyProviderError(
      status,
      text
    ) === 'INVALID_KEY'
  );
}


function isProviderError(
  status,
  text
) {
  return (
    classifyProviderError(
      status,
      text
    ) === 'PROVIDER_ERROR'
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

  // ----------------------------------------------------------
  // VALIDATE SOURCE
  // ----------------------------------------------------------

  if (
    !source ||
    typeof source !== 'string'
  ) {
    throw new Error(
      'Invalid Solidity source'
    );
  }


  // ----------------------------------------------------------
  // VALIDATE API KEY
  // ----------------------------------------------------------

  if (!apiKey) {

    const error =
      new Error(
        'LLM API key is missing'
      );

    error.code =
      'NO_API_KEY';

    throw error;
  }


  // ----------------------------------------------------------
  // SOURCE SIZE LIMIT
  // ----------------------------------------------------------

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


  // ----------------------------------------------------------
  // ENDPOINT
  // ----------------------------------------------------------

  const endpoint =
    llmUrl &&
    typeof llmUrl === 'string' &&
    /^https?:\/\//i.test(llmUrl)
      ? llmUrl
      : 'https://api.openai.com/v1/chat/completions';


  // ----------------------------------------------------------
  // USER MESSAGE
  // ----------------------------------------------------------

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


  // ----------------------------------------------------------
  // REQUEST TIMEOUT
  // ----------------------------------------------------------

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

  // ==========================================================
  // SEND REQUEST
  // ==========================================================

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

    // --------------------------------------------------------
    // TIMEOUT
    // --------------------------------------------------------

    if (
      error?.name === 'AbortError' ||
      error?.name === 'TimeoutError'
    ) {

      const timeoutError =
        new Error(
          `LLM request timed out after ${LLM_TIMEOUT / 60000} minutes`
        );

      timeoutError.code =
        'LLM_TIMEOUT';

      throw timeoutError;
    }


    // --------------------------------------------------------
    // NETWORK ERROR
    // --------------------------------------------------------

    const networkError =
      new Error(
        `LLM network request failed: ${error?.message || error}`
      );

    networkError.code =
      'NETWORK_ERROR';

    networkError.originalError =
      error;

    throw networkError;

  } finally {

    clearTimeout(
      timeoutId
    );
  }


  // ==========================================================
  // READ RAW RESPONSE
  // ==========================================================

  const rawText =
    await response.text();

  console.log(
    `[LLM] ${address} HTTP ${response.status}`
  );


  // ==========================================================
  // NON-200 HTTP RESPONSE
  //
  // ONLY HERE do we classify arbitrary response text by
  // provider-error keywords.
  // ==========================================================

  if (!response.ok) {

    const providerCode =
      classifyProviderError(
        response.status,
        rawText
      );

    const error =
      createProviderError(

        providerCode ||
        'HTTP_ERROR',

        `LLM API returned HTTP ${response.status}: ${rawText.slice(0, 1000)}`,

        {
          httpStatus:
            response.status,

          responseText:
            rawText
        }
      );

    throw error;
  }


  // ==========================================================
  // HTTP 200+ DOES NOT AUTOMATICALLY MEAN SUCCESS
  //
  // Parse the body structurally.
  // ==========================================================

  let json;

  try {

    json =
      JSON.parse(
        rawText
      );

    console.log(
      '[LLM DEBUG] Response:',
      JSON.stringify(json).slice(0, 5000)
    );

  } catch {

    // HTTP 200 but not valid JSON.
    const error =
      createProviderError(

        'INVALID_RESPONSE',

        `LLM returned invalid JSON: ${rawText.slice(0, 1000)}`,

        {
          httpStatus:
            response.status,

          responseText:
            rawText
        }
      );

    throw error;
  }


  // ==========================================================
  // STRUCTURED PROVIDER ERROR
  //
  // Example:
  //
  // HTTP 200
  // {
  //   "error": {
  //      "message": "rate limit exceeded"
  //   }
  // }
  //
  // This is why we still inspect HTTP-200 responses.
  // ==========================================================

  const structuredError =
    extractStructuredProviderError(
      json
    );

  if (structuredError) {

    const providerCode =
      classifyProviderError(
        response.status,
        [
          structuredError.message,
          structuredError.code,
          structuredError.type
        ]
          .filter(Boolean)
          .join(' ')
      );

    throw createProviderError(

      providerCode ||
      'PROVIDER_ERROR',

      structuredError.message,

      {
        httpStatus:
          response.status,

        responseText:
          rawText,

        apiError:
          json.error
      }
    );
  }


  // ==========================================================
  // EXTRACT COMPLETION
  //
  // Supports OpenAI-compatible response shapes.
  // ==========================================================

  const result =
    json?.choices?.[0]?.message?.content ??
    json?.choices?.[0]?.text ??
    json?.output_text ??
    '';


  // ==========================================================
  // EMPTY RESPONSE
  //
  // HTTP 200 is valid transport-wise but the provider gave
  // no usable audit completion.
  //
  // This is NOT:
  //
  //   - INVALID_KEY
  //   - RATE_LIMIT
  //   - QUOTA
  //
  // batch.js handles it independently.
  // ==========================================================

  if (
    !result ||
    !String(result).trim()
  ) {

    throw createProviderError(

      'EMPTY_RESPONSE',

      'LLM returned an empty audit response',

      {
        httpStatus:
          response.status,

        responseText:
          rawText,

        responseShape:
          Object.keys(json || {})
      }
    );
  }


  // ==========================================================
  // SUCCESS
  //
  // IMPORTANT:
  //
  // We deliberately DO NOT scan `result` for words such as:
  //
  // "unauthorized"
  // "rate limit"
  // "quota"
  //
  // because those may legitimately appear in a Solidity audit.
  // ==========================================================

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

  classifyProviderError,

  extractStructuredProviderError,

  isRateLimitError,

  isQuotaError,

  isInvalidKeyError,

  isProviderError
};
