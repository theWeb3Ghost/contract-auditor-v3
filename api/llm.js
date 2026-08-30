
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
//
// This file ONLY identifies what went wrong.
//
// batch.js is responsible for:
//   - API-key selection
//   - rate learning
//   - cooldowns
//   - failover
//   - pausing when all keys are unavailable
//
// ============================================================

function classifyProviderError(status, text) {

  const lower =
    String(text || '').toLowerCase();


  // ----------------------------------------------------------
  // INVALID / UNAUTHORIZED API KEY
  // ----------------------------------------------------------

  if (
    status === 401 ||
    lower.includes('invalid api key') ||
    lower.includes('invalid_api_key') ||
    lower.includes('incorrect api key') ||
    lower.includes('authentication failed') ||
    lower.includes('invalid authentication') ||
    lower.includes('unauthorized')
  ) {

    return 'INVALID_KEY';
  }


  // ----------------------------------------------------------
  // RATE LIMIT
  //
  // This means the key is temporarily sending too many
  // requests and should be slowed/cooldowned.
  // ----------------------------------------------------------

  if (
    status === 429 ||
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
  // QUOTA / CREDITS EXHAUSTED
  //
  // This is different from a temporary rate limit.
  //
  // batch.js can therefore give this key a much longer
  // cooldown instead of repeatedly retrying it.
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
    lower.includes('not been recharged') ||
    lower.includes('have not been recharged') ||
    lower.includes('recharged') ||
    lower.includes('recharge') ||
    lower.includes('topup') ||
    lower.includes('top-up') ||
    lower.includes('billing limit') ||
    lower.includes('billing quota') ||
    lower.includes('only try 10 times') ||
    lower.includes('only try 10')
  ) {

    return 'QUOTA';
  }


  // ----------------------------------------------------------
  // PROVIDER SERVER ERROR
  //
  // 5xx errors don't automatically mean the API key is bad.
  // batch.js should therefore use a short cooldown rather
  // than permanently disabling the key.
  // ----------------------------------------------------------

  if (
    Number(status) >= 500 &&
    Number(status) <= 599
  ) {

    return 'PROVIDER_ERROR';
  }


  return null;
}


// ============================================================
// CREATE STANDARD PROVIDER ERROR
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
    /^https?:\/\//i.test(
      llmUrl
    )
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


    // --------------------------------------------------------
    // NETWORK ERROR
    //
    // Don't classify an ordinary network failure as a bad
    // API key.
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
  // CLASSIFY PROVIDER RESPONSE
  // ==========================================================

  const providerCode =
    classifyProviderError(
      response.status,
      rawText
    );


  // ==========================================================
  // RATE LIMIT
  // ==========================================================

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


  // ==========================================================
  // QUOTA
  // ==========================================================

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
  // INVALID API KEY
  // ==========================================================

  if (
    providerCode ===
    'INVALID_KEY'
  ) {

    throw createProviderError(

      'INVALID_KEY',

      `LLM API key rejected: ${rawText.slice(0, 1000)}`,

      {
        httpStatus:
          response.status,

        responseText:
          rawText
      }

    );
  }


  // ==========================================================
  // PROVIDER SERVER ERROR
  // ==========================================================

  if (
    providerCode ===
    'PROVIDER_ERROR'
  ) {

    throw createProviderError(

      'PROVIDER_ERROR',

      `LLM provider server error (${response.status}): ${rawText.slice(0, 1000)}`,

      {
        httpStatus:
          response.status,

        responseText:
          rawText
      }

    );
  }


  // ==========================================================
  // OTHER HTTP ERRORS
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

    // Run the classifier again as a final safety net.
    const classified =
      classifyProviderError(
        response.status,
        rawText
      );

    if (classified) {
      error.code =
        classified;
    } else {
      error.code =
        'HTTP_ERROR';
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

    error.httpStatus =
      response.status;

    error.responseText =
      rawText;

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

    } else {

      error.code =
        'PROVIDER_ERROR';
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


  // ==========================================================
  // EMPTY RESPONSE
  // ==========================================================

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
  // Some providers can technically return HTTP 200 while
  // placing an error/rate-limit/quota message inside the
  // response body.
  //
  // Never allow that to become a completed audit.
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


  if (
    resultProviderCode ===
    'INVALID_KEY'
  ) {

    throw createProviderError(

      'INVALID_KEY',

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


  // ==========================================================
  // SUCCESS
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

  isRateLimitError,

  isQuotaError,

  isInvalidKeyError,

  isProviderError
};
