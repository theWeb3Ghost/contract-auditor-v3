// api/llm.js

const { fetch, Agent } = require('undici');

const MAX_CHARS = 300000;

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
  // PROMPT TOO LARGE FOR THIS TIER
  //
  // This is a PERMANENT rejection, not a transient rate limit,
  // even though provider error codes like "free_rate_limited"
  // contain the substring "rate_limit". Providers that support
  // it tell us directly via "retryable": false in the body —
  // rotating keys or waiting will never make this succeed, so
  // it must be classified and handled separately from RATE_LIMIT.
  // ----------------------------------------------------------

  if (
    lower.includes('longer than the free tier allows') ||
    lower.includes('prompt is too long') ||
    lower.includes('prompt too long') ||
    lower.includes('exceeds the free tier') ||
    lower.includes('err_free_prompt_cap') ||
    lower.includes('"retryable":false')
  ) {
    return 'PROMPT_TOO_LARGE';
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


function isPromptTooLargeError(
  status,
  text
) {
  return (
    classifyProviderError(
      status,
      text
    ) === 'PROMPT_TOO_LARGE'
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
// CHUNKING FOR OVERSIZED PROMPTS
// ============================================================
//
// Some free-tier models/providers reject a request outright if
// the prompt (system + user message) is too large — see
// PROMPT_TOO_LARGE above. Rather than always truncating (which
// can cut a contract off mid-function and lose real findings),
// we estimate the size up front and, if it's too big, split the
// source into two parts, audit each part, and have the second
// call fold in the first part's findings so cross-part issues
// still get caught.
// ============================================================

// Rough estimate: ~4 characters per token for English/code text.
// This is deliberately conservative — it doesn't need to be
// exact, just good enough to decide "will this likely fit".
function estimateTokens(text) {
  return Math.ceil(
    String(text || '').length / 4
  );
}

// Leave real headroom under a typical free-tier ~64-66K token
// cap: system prompt + both audit passes' overhead all eat into
// this budget too, not just the raw source.
const SAFE_PROMPT_TOKEN_LIMIT = 50000;

// Find (numParts - 1) safe places to cut the source — each one
// the end of a function/block ("}" followed by a newline) nearest
// its ideal even-split target, so we never slice a function
// definition in two. Falls back to a plain character split for
// any target where no boundary is found nearby, and guarantees
// the returned indices are strictly increasing.
function findSplitBoundaries(src, numParts) {

  const searchWindow =
    5000;

  const boundaries =
    [];

  let prevBoundary =
    0;

  for (let i = 1; i < numParts; i++) {

    const idealTarget =
      Math.floor(
        (src.length * i) / numParts
      );

    const windowStart =
      Math.max(prevBoundary, idealTarget - searchWindow);

    const chunk =
      src.slice(windowStart, idealTarget);

    const lastBoundary =
      chunk.lastIndexOf('}\n');

    let boundary =
      lastBoundary === -1
        ? idealTarget
        : windowStart + lastBoundary + 2;

    // Guard against a degenerate/tiny source where the search
    // window logic could produce a non-increasing boundary.
    if (boundary <= prevBoundary) {
      boundary =
        Math.max(idealTarget, prevBoundary + 1);
    }

    boundaries.push(boundary);
    prevBoundary = boundary;
  }

  return boundaries;
}

// Split src into numParts chunks using findSplitBoundaries.
function splitIntoParts(src, numParts) {

  const boundaries =
    findSplitBoundaries(src, numParts);

  const cutPoints =
    [0, ...boundaries, src.length];

  const parts =
    [];

  for (let i = 0; i < cutPoints.length - 1; i++) {
    parts.push(
      src.slice(cutPoints[i], cutPoints[i + 1])
    );
  }

  return parts;
}

// Hard ceiling on how many parts we'll ever split into. If the
// math asks for more than this, the contract is too large to
// audit under this model's prompt cap even with splitting — fail
// clearly instead of silently making 15+ sequential LLM calls.
const MAX_SPLIT_PARTS = 8;

// How much of the token budget to reserve for the system prompt,
// wrapper text, and the rolling summary carried between parts.
// Kept generous since the summary itself can grow a little.
const RESERVED_OVERHEAD_TOKENS = 4000;

// Cap on the rolling summary's own size (in words), enforced via
// the prompt instruction rather than code — keeps context bounded
// as parts increase instead of growing with every part.
const ROLLING_SUMMARY_MAX_WORDS = 400;


// ============================================================
// RUN LLM AUDIT
// ============================================================

// ============================================================
// SEND ONE COMPLETION REQUEST
//
// Everything about talking to the provider for ONE prompt:
// send it, handle timeouts/network errors, classify non-200s,
// parse the body, and return the raw completion text (or throw
// one of the classified errors above). runLLMAudit calls this
// once for a normal-sized contract, or twice for a split one.
// ============================================================

async function sendCompletionRequest({
  endpoint,
  apiKey,
  model,
  systemPrompt,
  userMessage,
  address
}) {

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

              max_tokens: 32000,
              reasoning: { effort: 'medium' },

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


  const rawText =
    await response.text();

  console.log(
    `[LLM] ${address} HTTP ${response.status}`
  );


  if (!response.ok) {

    const providerCode =
      classifyProviderError(
        response.status,
        rawText
      );

    if (providerCode === 'PROMPT_TOO_LARGE') {

      console.log(
        `[LLM] ${address} PROMPT_TOO_LARGE on this specific ` +
        `request: ~${estimateTokens(systemPrompt) + estimateTokens(userMessage)} ` +
        `est. tokens (${systemPrompt.length + userMessage.length} total chars sent)`
      );
    }

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


  const result =
    json?.choices?.[0]?.message?.content ??
    json?.choices?.[0]?.text ??
    json?.output_text ??
    '';


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

  return String(result).trim();
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
  // SOURCE SIZE LIMIT (hard ceiling, unchanged)
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
  // DECIDE: NORMAL SINGLE REQUEST, OR SPLIT INTO TWO
  // ----------------------------------------------------------

  const estimatedTotalTokens =
    estimateTokens(systemPrompt) +
    estimateTokens(src);

  const needsSplit =
    estimatedTotalTokens >
    SAFE_PROMPT_TOKEN_LIMIT;

  // Always log the size decision — not just on failure. This is
  // what lets us calibrate SAFE_PROMPT_TOKEN_LIMIT and the chars-
  // per-token ratio against real provider behavior instead of
  // guessing: compare the largest sizes that succeeded against
  // the smallest sizes that got PROMPT_TOO_LARGE below.
  console.log(
    `[LLM] ${address} prompt size check: ` +
    `~${estimatedTotalTokens} est. tokens ` +
    `(${systemPrompt.length + src.length} total chars: ` +
    `${systemPrompt.length} system + ${src.length} source) — ` +
    `${needsSplit ? 'splitting' : 'sending as one request'}`
  );


  // ----------------------------------------------------------
  // NORMAL PATH — fits comfortably in one request
  // ----------------------------------------------------------

  if (!needsSplit) {

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

    const result =
      await sendCompletionRequest({
        endpoint,
        apiKey,
        model,
        systemPrompt,
        userMessage,
        address
      });

    return {
      result,
      truncated
    };
  }


  // ----------------------------------------------------------
  // SPLIT PATH — too large for one request
  // ----------------------------------------------------------
  //
  // Splits into however many parts are actually needed, not a
  // fixed number. Each non-final part produces a short, bounded
  // "rolling summary" (not its full raw output) which gets carried
  // into the next part — this keeps context size roughly constant
  // as the number of parts grows, instead of growing with it. Only
  // the final part produces the full report.
  //
  // Note: if this attempt fails and batch.js retries with a
  // different key, ALL parts are resent from scratch — there is
  // no partial-progress caching at this granularity.
  // ----------------------------------------------------------

  const budgetPerPart =
    SAFE_PROMPT_TOKEN_LIMIT -
    estimateTokens(systemPrompt) -
    RESERVED_OVERHEAD_TOKENS;

  const numParts =
    Math.max(
      2,
      Math.ceil(
        estimateTokens(src) / budgetPerPart
      )
    );

  if (numParts > MAX_SPLIT_PARTS) {

    throw createProviderError(

      'PROMPT_TOO_LARGE',

      `Contract requires ${numParts} parts to fit this model's ` +
      `prompt limit, which exceeds the maximum of ${MAX_SPLIT_PARTS} ` +
      `parts supported. This contract cannot be audited on this ` +
      `model even with splitting.`,

      {
        estimatedTokens:
          estimateTokens(src),

        numPartsRequired:
          numParts
      }
    );
  }

  const parts =
    splitIntoParts(src, numParts);

  console.log(
    `[LLM] ${address} prompt too large ` +
    `(~${estimatedTotalTokens} est. tokens) — splitting into ${numParts} parts`
  );

  let runningSummary =
    '';

  let finalResult =
    '';

  for (let i = 0; i < parts.length; i++) {

    const isFirst =
      i === 0;

    const isLast =
      i === parts.length - 1;

    const partLabel =
      `PART ${i + 1} OF ${parts.length}`;

    if (!isLast) {

      // Non-final part: analyze this chunk, fold it into an
      // updated rolling summary, and return ONLY that summary —
      // not a full report. This is what keeps context bounded
      // regardless of how many parts there are.
      const userMessage = `
You are auditing the following Solidity smart contract. It is
too large for a single request, so it has been split into
${parts.length} parts. This is ${partLabel}.

${
  isFirst
    ? ''
    : `PRIOR RUNNING SUMMARY (from earlier parts):\n${runningSummary}\n\n`
}
SOURCE CODE (${partLabel}):
\`\`\`solidity
${parts[i]}
\`\`\`

Analyze this part for atomic flaws (STAGE 0/STAGE 1 style:
recon, access control, reentrancy, math, external calls, etc).

${
  isFirst
    ? 'Then produce a running summary of your findings so far.'
    : 'Then MERGE the findings from this part with the prior ' +
      'running summary above into one UPDATED running summary.'
}

IMPORTANT: Output ONLY the updated running summary — no STAGE
headers, no final verdict, no full report. Keep it under
${ROLLING_SUMMARY_MAX_WORDS} words. Cover: contract structure
seen so far, atomic flaws found so far (with location), and
anything that looks like it could chain with code in a later
part. Further parts of this contract will follow.
`;

      runningSummary =
        await sendCompletionRequest({
          endpoint,
          apiKey,
          model,
          systemPrompt,
          userMessage,
          address
        });

      // Brief pause between sequential calls on the same key —
      // we don't have access to the rate learner's interval here.
      await new Promise(
        resolve => setTimeout(resolve, 2000)
      );

      continue;
    }

    // Final part: give it the rolling summary plus this part's
    // code, and ask for the actual final consolidated report.
    const userMessage = `
This is ${partLabel} — the final part — for the same contract as
your previous message(s). Here is the rest of the source code,
followed by the running summary of everything found so far.

Contract Name: ${contractName || 'Unknown'}
Contract Address: ${address || 'Unknown'}

SOURCE CODE (${partLabel}):
\`\`\`solidity
${parts[i]}
\`\`\`

${
  truncated
    ? '\nWARNING: Source code was truncated because it exceeded the configured source limit.\n'
    : ''
}

RUNNING SUMMARY OF FINDINGS FROM EARLIER PARTS (for reference —
do not re-analyze that code, just use these findings):
${runningSummary}

Now complete the analysis for this part's code, then STAGE 2
(Chaining) considering the WHOLE contract together — including
any chain that spans a function in an earlier part and a function
in this part — and produce the FINAL consolidated audit report
covering the whole contract, in the normal report format.
`;

    finalResult =
      await sendCompletionRequest({
        endpoint,
        apiKey,
        model,
        systemPrompt,
        userMessage,
        address
      });
  }

  return {

    result:
      finalResult,

    truncated,

    split:
      true,

    splitParts:
      numParts
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

  isPromptTooLargeError,

  isQuotaError,

  isInvalidKeyError,

  isProviderError
};
