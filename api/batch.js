
// api/batch.js

const crypto =
  require('crypto');

const express =
  require('express');

const {
  getDb
} = require('./db');

const {
  runLLMAudit
} = require('./llm');


// ============================================================
// CONFIGURATION
// ============================================================

// Maximum source sent to the LLM.
const MAX_SOURCE_CHARS =
  150000;

// ============================================================
// ADAPTIVE LLM RATE LEARNING
// ============================================================
//
// The system learns the safest request interval for each:
//
// Provider + Model + API Key
//
// It starts conservatively, slowly probes faster after sustained
// success, and slows down aggressively when a provider rate limit
// is detected.
//
// The learned state is persisted in MongoDB, so restarting the
// server does NOT reset the learned rate.
// ============================================================

const RATE_LEARNING = {

  // Starting point for a completely unknown provider/profile.
  //
  // 600ms ~= 1.67 requests/sec.
  DEFAULT_INTERVAL_MS:
    600,

  // Never go faster than this.
  //
  // This protects against the learner becoming too aggressive.
  MIN_INTERVAL_MS:
    300,

  // Never become slower than this.
  //
  // Prevents pathological rate-limit loops from making the
  // pipeline effectively unusable.
  MAX_INTERVAL_MS:
    30000,

  // Number of successful LLM requests before we cautiously
  // attempt to increase speed.
  SUCCESS_THRESHOLD:
    50,

  // After SUCCESS_THRESHOLD consecutive successes:
  //
  // 1000ms -> 970ms
  //
  // Small changes make learning stable.
  SUCCESS_SPEEDUP_FACTOR:
    0.97,

  // On rate limit:
  //
  // 600ms -> 900ms
  //
  // This is intentionally much more aggressive than speeding up.
  RATE_LIMIT_SLOWDOWN_FACTOR:
    1.5,

  // Keep request timestamps for this rolling window.
  WINDOW_MS:
    30 * 60 * 1000,

  // Hard cap on timestamps stored in memory/database.
  MAX_HISTORY:
    3000,

  // Persist normal learning progress every N successful requests.
  SAVE_EVERY_SUCCESSES:
    10
};

// ============================================================
// API KEY POOL
// ============================================================
//
// Multiple keys can be configured.
//
// Example:
//
// LLM_API_KEYS=key1,key2,key3
//
// Each key gets:
//   - Independent rate learning
//   - Independent cooldown
//   - Independent quota state
//   - Independent health tracking
//
// Raw keys are NEVER returned through the API.
// ============================================================

const KEY_POOL = {

  // Temporary 429 cooldown.
  RATE_LIMIT_COOLDOWN_MS:
    2 * 60 * 1000,

  // Quota failures usually last longer than ordinary rate limits.
  QUOTA_COOLDOWN_MS:
    60 * 60 * 1000,

  // Provider/server failures get a short cooldown.
  PROVIDER_ERROR_COOLDOWN_MS:
    30 * 1000,

  // Invalid credentials stay disabled until server restart/config change.
  INVALID_KEY_COOLDOWN_MS:
    24 * 60 * 60 * 1000
};


function getConfiguredLLMKeys(batch) {

  const keys = [];

  if (Array.isArray(batch?.llmApiKeys)) {
    keys.push(
      ...batch.llmApiKeys.map(key=> String(key || '').trim()).filter(Boolean) 
    );
  }

  if (process.env.LLM_API_KEYS) {
    keys.push(
      ...process.env.LLM_API_KEYS
        .split(',')
        .map(key => key.trim())
        .filter(Boolean)
    );
  }

  if (
    batch?.openaiKey &&
    String(batch.openaiKey).trim()
  ) {
    keys.push(
      String(batch.openaiKey).trim()
    );
  }

  return [
    ...new Set(
      keys.filter(Boolean)
    )
  ];
}


function getKeyFingerprint(apiKey) {

  return crypto
    .createHash('sha256')
    .update(String(apiKey || ''))
    .digest('hex')
    .slice(-12);
}

// How long to wait between completed contracts.
const ITEM_DELAY =
  200;

// Maximum number of attempts when the LLM returns an empty response.
// 3 total attempts = initial attempt + 2 retries.
const MAX_EMPTY_AUDIT_ATTEMPTS =
  3;

// ============================================================
// WORKER STATE
// ============================================================

const activeWorkers =
  new Set();


// ============================================================
// ADDRESS VALIDATION
// ============================================================

const ADDRESS_RE =
  /^0x[a-fA-F0-9]{40}$/;


function cleanAddress(
  address
) {

  if (
    typeof address !==
    'string'
  ) {
    return null;
  }


  const value =
    address.trim();


  if (
    !ADDRESS_RE.test(
      value
    )
  ) {
    return null;
  }


  return value;
}


// ============================================================
// PARSE ADDRESSES
// ============================================================
//
// Supports:
//
// [
//   {"address":"0x..."},
//   {"address":"0x..."}
// ]
//
// JSONL:
//
// {"address":"0x..."}
// {"address":"0x..."}
//
// Plain:
//
// 0x...
// 0x...
//
// Also:
//
// [
//   "0x...",
//   "0x..."
// ]
//
// Duplicates are removed.
// ============================================================

function parseAddresses(
  input
) {

  const addresses =
    [];

  const seen =
    new Set();


  function add(
    value
  ) {

    const address =
      cleanAddress(
        value
      );


    if (!address) {
      return;
    }


    const normalized =
      address.toLowerCase();


    if (
      seen.has(
        normalized
      )
    ) {
      return;
    }


    seen.add(
      normalized
    );


    addresses.push(
      address
    );
  }


  function parseValue(
    value
  ) {

    if (
      typeof value ===
      'string'
    ) {

      // A string may contain one or more addresses.
      const matches =
        value.match(
          /0x[a-fA-F0-9]{40}/g
        );


      if (matches) {

        for (
          const address
          of matches
        ) {
          add(address);
        }

      } else {

        add(value);
      }


      return;
    }


    if (
      Array.isArray(
        value
      )
    ) {

      for (
        const item
        of value
      ) {

        parseValue(
          item
        );
      }


      return;
    }


    if (
      value &&
      typeof value ===
      'object'
    ) {

      if (
        value.address
      ) {

        add(
          value.address
        );
      }


      if (
        value.contractAddress
      ) {

        add(
          value.contractAddress
        );
      }


      if (
        Array.isArray(
          value.addresses
        )
      ) {

        parseValue(
          value.addresses
        );
      }
    }
  }


  // Already parsed object/array.
  if (
    input &&
    typeof input ===
    'object'
  ) {

    parseValue(
      input
    );

    return addresses;
  }


  if (
    typeof input !==
    'string'
  ) {

    return addresses;
  }


  const text =
    input.trim();


  if (!text) {
    return addresses;
  }


  // ==========================================================
  // FIRST: TRY THE ENTIRE STRING AS JSON
  //
  // This is the important fix for pretty-printed JSON.
  // ==========================================================

  try {

    const parsed =
      JSON.parse(
        text
      );


    parseValue(
      parsed
    );


    if (
      addresses.length
    ) {

      return addresses;
    }

  } catch {
    // Continue to JSONL/plain-text parsing.
  }


  // ==========================================================
  // JSONL / PLAIN TEXT
  // ==========================================================

  const lines =
    text.split(
      /\r?\n/
    );


  for (
    const line
    of lines
  ) {

    const trimmed =
      line.trim();


    if (!trimmed) {
      continue;
    }


    // Try one complete JSONL line.
    try {

      const parsed =
        JSON.parse(
          trimmed
        );


      parseValue(
        parsed
      );


      continue;

    } catch {
      // Fall through.
    }


    // Find Ethereum addresses anywhere in the line.
    const matches =
      trimmed.match(
        /0x[a-fA-F0-9]{40}/g
      );


    if (matches) {

      for (
        const address
        of matches
      ) {

        add(
          address
        );
      }
    }
  }


  return addresses;
}


// ============================================================
// ERROR HELPERS
// ============================================================

function errorText(
  error
) {

  return String(
    error?.message ||
    error ||
    'Unknown error'
  );
}


function isEmptyAuditResponseError(
  error
) {

  const message =
    errorText(
      error
    ).toLowerCase();

  return (
    message.includes(
      'llm returned an empty audit response'
    )
  );
}


function isPipelineStopError(
  error
) {

  if (!error) {
    return false;
  }


  const code =
    String(
      error.code ||
      ''
    ).toUpperCase();


  if (
    code === 'RATE_LIMIT' ||
    code === 'QUOTA' ||
    code === 'ALL_KEYS_UNAVAILABLE'
  ) {

    return true;
  }


  const message =
    errorText(
      error
    ).toLowerCase();


  return (
    message.includes(
      'rate limit'
    ) ||
    message.includes(
      'rate_limit'
    ) ||
    message.includes(
      'too many requests'
    ) ||
    message.includes(
      'requests per second'
    ) ||
    message.includes(
      'free quota'
    ) ||
    message.includes(
      'quota exceeded'
    ) ||
    message.includes(
      'only try 10'
    ) ||
    message.includes(
      'not been recharged'
    ) ||
    message.includes(
      'recharged'
    ) ||
    message.includes(
      'topup'
    ) ||
    message.includes(
      'top-up'
    )
  );
}


function pipelineStopType(
  error
) {

  const code =
    String(
      error?.code ||
      ''
    ).toUpperCase();


  if (
    code ===
    'QUOTA'
  ) {

    return 'paused_quota';
  }


  return 'paused_rate_limit';
}


// ============================================================
// TIME HELPERS
// ============================================================

function now() {
  return new Date();
}


function sleep(
  milliseconds
) {

  return new Promise(
    resolve =>
      setTimeout(
        resolve,
        milliseconds
      )
  );
}


// ============================================================
// ADAPTIVE GLOBAL LLM RATE CONTROLLER
// ============================================================
//
// One Node process may process multiple batches, but all LLM
// requests pass through this single controller.
//
// The controller:
//   1. Serializes LLM requests.
//   2. Learns safe request intervals.
//   3. Persists learning in MongoDB.
//   4. Survives server restarts.
//   5. Tracks recent request timestamps.
// ============================================================


const rateProfiles =
  new Map();


let llmThrottle =
  Promise.resolve();


// ============================================================
// PROFILE KEY
// ============================================================
//
// We never store the raw API key in the rate profile.
//
// Instead:
//
// SHA256(llmUrl + model + apiKey)
//
// This creates a stable identity for one provider/model/key
// combination without exposing the key.
// ============================================================

function getRateProfileId({
  llmUrl,
  model, 
  apiKey
}) {

  return crypto
    .createHash(
      'sha256'
    )
    .update(
      [
        String(
          llmUrl ||
          ''
        ),

        String(
          model ||
          ''
        ),

        String(
          apiKey ||
          ''
        )
      ].join(
        '|'
      )
    )
    .digest(
      'hex'
    );
}


// ============================================================
// CREATE DEFAULT PROFILE
// ============================================================

function createDefaultRateProfile({
  profileId,
  llmUrl,
  model, 
  apiKey
}) {

  return {

    _id:
      profileId,

    llmUrl:
      llmUrl ||
      null,

    model:
      model ||
      null,


    // --------------------------------------------------------
    // CURRENT LEARNED STATE
    // --------------------------------------------------------

    currentIntervalMs:
      RATE_LEARNING.DEFAULT_INTERVAL_MS,

    fastestKnownSafeMs:
      RATE_LEARNING.DEFAULT_INTERVAL_MS,

    lastFailedIntervalMs:
      null,


    // --------------------------------------------------------
    // SUCCESS / FAILURE STATS
    // --------------------------------------------------------

    consecutiveSuccesses:
      0,

    totalSuccesses:
      0,

    rateLimitHits:
      0,


    // --------------------------------------------------------
    // REQUEST HISTORY
    // --------------------------------------------------------

    recentRequests:
      [],

    lastRequestAt:
      0,

    lastRateLimitAt:
      null,

    // --------------------------------------------------------
// KEY POOL STATE
// --------------------------------------------------------

keyFingerprint:
  getKeyFingerprint(apiKey),

keyStatus:
  'available',

cooldownUntil:
  null,

lastErrorCode:
  null,

lastErrorAt:
  null,

quotaHits:
  0,

invalidKeyHits:
  0,

providerErrorHits:
  0,

lastUsedAt:
  null,

    // --------------------------------------------------------
    // INTERNAL STATE
    // --------------------------------------------------------

    unsavedSuccesses:
      0,

    createdAt:
      now(),

    updatedAt:
      now()
  };
}


// ============================================================
// NORMALIZE PROFILE
// ============================================================
//
// Allows us to safely load older/incomplete MongoDB documents.
// ============================================================

function normalizeRateProfile(
  profile,
  defaults
) {

  const fallback =
    createDefaultRateProfile(
      defaults
    );


  return {

    ...fallback,

    ...profile,


    currentIntervalMs:
      clampInterval(
        profile?.currentIntervalMs ??
        fallback.currentIntervalMs
      ),

    fastestKnownSafeMs:
      clampInterval(
        profile?.fastestKnownSafeMs ??
        fallback.fastestKnownSafeMs
      ),

    recentRequests:
      Array.isArray(
        profile?.recentRequests
      )
        ? profile.recentRequests
        : [],

    unsavedSuccesses:
      0
  };
}


// ============================================================
// CLAMP INTERVAL
// ============================================================

function clampInterval(
  value
) {

  const numeric =
    Number(
      value
    );

  if (
    !Number.isFinite(
      numeric
    )
  ) {
    return RATE_LEARNING.DEFAULT_INTERVAL_MS;
  }


  return Math.round(
    Math.max(
      RATE_LEARNING.MIN_INTERVAL_MS,

      Math.min(
        RATE_LEARNING.MAX_INTERVAL_MS,
        numeric
      )
    )
  );
}


// ============================================================
// PRUNE REQUEST HISTORY
// ============================================================
//
// We only care about recent behavior.
//
// Old timestamps are removed so MongoDB does not grow forever.
// ============================================================

function pruneRateHistory(
  profile
) {

  const cutoff =
    Date.now() -
    RATE_LEARNING.WINDOW_MS;


  profile.recentRequests =
    profile.recentRequests
      .map(
        timestamp =>
          new Date(
            timestamp
          ).getTime()
      )
      .filter(
        timestamp =>
          Number.isFinite(
            timestamp
          ) &&
          timestamp >= cutoff
      )
      .slice(
        -RATE_LEARNING.MAX_HISTORY
      );
}


// ============================================================
// LOAD RATE PROFILE
// ============================================================
//
// MongoDB is checked only when a profile is first needed.
//
// Afterwards the profile stays cached in memory.
// ============================================================

async function getRateProfile({
  llmUrl,
  model,
  apiKey
}) {

  const profileId =
    getRateProfileId({
      llmUrl,
      model,
      apiKey
    });


  if (
    rateProfiles.has(
      profileId
    )
  ) {

    return rateProfiles.get(
      profileId
    );
  }


  const db =
    await getDb();


  const collection =
    db.collection(
      'llm_rate_profiles'
    );


  const existing =
    await collection.findOne({
      _id:
        profileId
    });


  const profile =
    normalizeRateProfile(
      existing,
      {
        profileId,
        llmUrl,
        model
      }
    );


  pruneRateHistory(
    profile
  );


  // Cache immediately.
  rateProfiles.set(
    profileId,
    profile
  );


  // Create MongoDB document if this is a new profile.
  if (!existing) {

    await collection.insertOne({
      ...profile,

      updatedAt:
        now()
    });


    console.log(
      `[RATE LEARNER] Created profile for ${model}`
    );

  } else {

    console.log(
      `[RATE LEARNER] Loaded profile for ${model}: ` +
      `${profile.currentIntervalMs}ms interval, ` +
      `${profile.totalSuccesses} successes, ` +
      `${profile.rateLimitHits} rate limits`
    );
  }


  return profile;
}


// ============================================================
// SAVE RATE PROFILE
// ============================================================

async function saveRateProfile(
  profile
) {

  pruneRateHistory(
    profile
  );


  const db =
    await getDb();


  await db
    .collection(
      'llm_rate_profiles'
    )
    .updateOne(
      {
        _id:
          profile._id
      },
      {
        $set: {
          keyFingerprint:
  profile.keyFingerprint,

keyStatus:
  profile.keyStatus,

cooldownUntil:
  profile.cooldownUntil,

lastErrorCode:
  profile.lastErrorCode,

lastErrorAt:
  profile.lastErrorAt,

quotaHits:
  profile.quotaHits,

invalidKeyHits:
  profile.invalidKeyHits,

providerErrorHits:
  profile.providerErrorHits,

lastUsedAt:
  profile.lastUsedAt,

          llmUrl:
            profile.llmUrl,

          model:
            profile.model,

          currentIntervalMs:
            profile.currentIntervalMs,

          fastestKnownSafeMs:
            profile.fastestKnownSafeMs,

          lastFailedIntervalMs:
            profile.lastFailedIntervalMs,

          consecutiveSuccesses:
            profile.consecutiveSuccesses,

          totalSuccesses:
            profile.totalSuccesses,

          rateLimitHits:
            profile.rateLimitHits,

          recentRequests:
            profile.recentRequests,

          lastRequestAt:
            profile.lastRequestAt,

          lastRateLimitAt:
            profile.lastRateLimitAt,

          updatedAt:
            now()
        }
      },
      {
        upsert:
          true
      }
    );


  profile.unsavedSuccesses =
    0;
}
// ============================================================
// KEY POOL STATUS
// ============================================================

function isKeyAvailable(profile) {

  if (!profile) {
    return false;
  }

  if (
    profile.keyStatus === 'disabled'
  ) {
    return false;
  }

  const cooldownUntil =
    profile.cooldownUntil
      ? new Date(
          profile.cooldownUntil
        ).getTime()
      : 0;

  if (
    cooldownUntil &&
    cooldownUntil > Date.now()
  ) {
    return false;
  }

  // Automatically recover expired cooldowns.
  if (
    cooldownUntil &&
    cooldownUntil <= Date.now() &&
    profile.keyStatus === 'cooldown'
  ) {

    profile.keyStatus =
      'available';

    profile.cooldownUntil =
      null;
  }

  return true;
}


function getKeyCooldownRemaining(profile) {

  if (
    !profile?.cooldownUntil
  ) {
    return 0;
  }

  return Math.max(
    0,
    new Date(
      profile.cooldownUntil
    ).getTime() -
    Date.now()
  );
}


function getKeyScore(profile) {

  if (!isKeyAvailable(profile)) {
    return -Infinity;
  }

  let score =
    100000;

  // Faster learned interval = better.
  score -=
    profile.currentIntervalMs * 10;

  // Recent rate limits reduce preference.
  score -=
    profile.rateLimitHits * 500;

  // Consecutive successful requests increase confidence.
  score +=
    Math.min(
      profile.consecutiveSuccesses,
      RATE_LEARNING.SUCCESS_THRESHOLD
    ) * 20;

  // Recently successful keys get a small preference.
  if (
    profile.lastUsedAt &&
    Date.now() -
    new Date(profile.lastUsedAt).getTime()
      < 5 * 60 * 1000
  ) {
    score += 100;
  }

  return score;
}


// ============================================================
// SELECT BEST API KEY
// ============================================================

async function selectBestLLMKey({
  batch
}) {

  const keys =
    getConfiguredLLMKeys(batch);

  if (!keys.length) {

    const error =
      new Error(
        'No LLM API keys configured'
      );

    error.code =
      'NO_API_KEYS';

    throw error;
  }

  const candidates =
    [];

  for (
    const apiKey of keys
  ) {

    const profile =
      await getRateProfile({
        llmUrl:
          batch.llmUrl,

        model:
          batch.model,

        apiKey
      });

    profile.keyFingerprint =
      getKeyFingerprint(apiKey);

    candidates.push({
      apiKey,
      profile
    });
  }

  const available =
    candidates.filter(
      candidate =>
        isKeyAvailable(
          candidate.profile
        )
    );

  if (!available.length) {

    const earliestRecovery =
      candidates
        .map(
          candidate =>
            getKeyCooldownRemaining(
              candidate.profile
            )
        )
        .filter(Boolean)
        .sort(
          (a, b) => a - b
        )[0] || null;

    const error =
      new Error(
        'All configured LLM API keys are unavailable'
      );

    error.code =
      'ALL_KEYS_UNAVAILABLE';

    error.retryAfterMs =
      earliestRecovery;

    throw error;
  }

  available.sort(
    (a, b) =>
      getKeyScore(
        b.profile
      ) -
      getKeyScore(
        a.profile
      )
  );

  const selected =
    available[0];

  selected.profile.keyStatus =
    'active';

  selected.profile.lastUsedAt =
    now();

  return selected;
}

// ============================================================
// WAIT FOR LLM SLOT
// ============================================================
//
// This replaces the old fixed 600ms limiter.
//
// Every request:
//   1. Loads learned profile.
//   2. Waits according to current learned interval.
//   3. Records exact request timestamp.
//   4. Returns the profile for later success/failure learning.
// ============================================================

function waitForLLMSlot({
  batch
}) {

  const next =
    llmThrottle.then(
      async () => {

        const selected =
          await selectBestLLMKey({
            batch
          });

        const {
          apiKey,
          profile
        } =
          selected;

        const lastRequestTime =
          Number(
            profile.lastRequestAt || 0
          );

        const elapsed =
          Date.now() -
          lastRequestTime;

        const wait =
          Math.max(
            0,
            profile.currentIntervalMs -
            elapsed
          );

        if (wait > 0) {

          console.log(
            `[RATE LEARNER] Key ${profile.keyFingerprint} ` +
            `waiting ${wait}ms ` +
            `(interval ${profile.currentIntervalMs}ms)`
          );

          await sleep(wait);
        }

        const sentAt =
          Date.now();

        profile.lastRequestAt =
          sentAt;

        profile.lastUsedAt =
          now();

        profile.recentRequests.push(
          sentAt
        );

        pruneRateHistory(
          profile
        );

        console.log(
          `[RATE LEARNER] key=${profile.keyFingerprint} | ` +
          `interval=${profile.currentIntervalMs}ms | ` +
          `1m=${getRequestsInWindow(profile, 60 * 1000)} | ` +
          `30m=${profile.recentRequests.length}`
        );

        return {
          apiKey,
          profile
        };
      }
    );

  llmThrottle =
    next.catch(
      () => {}
    );

  return next;
}

// ============================================================
// COUNT REQUESTS IN WINDOW
// ============================================================

function getRequestsInWindow(
  profile,
  windowMs
) {

  const cutoff =
    Date.now() -
    windowMs;


  return profile.recentRequests.filter(
    timestamp =>
      Number(
        timestamp
      ) >= cutoff
  ).length;
}


// ============================================================
// RECORD SUCCESS
// ============================================================
//
// Learning strategy:
//
// Every success:
//   consecutiveSuccesses++
//
// Every 50 consecutive successes:
//   speed up by 3%
//
// Example:
//
// 1000ms -> 970ms -> 941ms -> 913ms
// ============================================================

async function recordLLMSuccess(
  profile
) {

  if (!profile) {
    return;
  }


  profile.consecutiveSuccesses +=
    1;

  profile.totalSuccesses +=
    1;

  profile.unsavedSuccesses +=
    1;

  // ----------------------------------------------------------
  // DISCOVER FASTER SAFE ZONE
  // ----------------------------------------------------------

  if (
    profile.currentIntervalMs <
    profile.fastestKnownSafeMs
  ) {

    profile.fastestKnownSafeMs =
      profile.currentIntervalMs;
  }


  // ----------------------------------------------------------
  // CAUTIOUS SPEED INCREASE
  // ----------------------------------------------------------

  if (
    profile.consecutiveSuccesses >=
    RATE_LEARNING.SUCCESS_THRESHOLD
  ) {

    const previousInterval =
      profile.currentIntervalMs;


    const fasterInterval =
      clampInterval(
        previousInterval *
        RATE_LEARNING.SUCCESS_SPEEDUP_FACTOR
      );


    // Only change if it actually produces a new integer value.
    if (
      fasterInterval <
      previousInterval
    ) {

      profile.currentIntervalMs =
        fasterInterval;


      console.log(
        `[RATE LEARNER] ${RATE_LEARNING.SUCCESS_THRESHOLD} successes. ` +
        `Speeding up: ${previousInterval}ms -> ` +
        `${fasterInterval}ms`
      );
    }


    profile.consecutiveSuccesses =
      0;


    // Save immediately when the learned speed changes.
    await saveRateProfile(
      profile
    );

    return;
  }


  // ----------------------------------------------------------
  // PERIODIC PERSISTENCE
  // ----------------------------------------------------------

  if (
    profile.unsavedSuccesses >=
    RATE_LEARNING.SAVE_EVERY_SUCCESSES
  ) {

    await saveRateProfile(
      profile
    );
  }
}


// ============================================================
// RECORD RATE LIMIT
// ============================================================
//
// Learning strategy:
//
// Rate limit:
//   1. Record the interval that failed.
//   2. Reset consecutive successes.
//   3. Slow down aggressively.
//   4. Persist immediately.
//
// Example:
//
// 600ms -> rate limit
//
// New interval:
//
// 600 * 1.5 = 900ms
// ============================================================

async function recordLLMRateLimit(
  profile,
  error
) {

  if (!profile) {
    return;
  }


  const failedInterval =
    profile.currentIntervalMs;


  const newInterval =
    clampInterval(
      failedInterval *
      RATE_LEARNING.RATE_LIMIT_SLOWDOWN_FACTOR
    );


  profile.lastFailedIntervalMs =
    failedInterval;

  profile.currentIntervalMs =
    newInterval;

  profile.consecutiveSuccesses =
    0;

  profile.rateLimitHits +=
    1;

  profile.lastRateLimitAt =
    now();


  pruneRateHistory(
    profile
  );


  console.error(
    `[RATE LEARNER] RATE LIMIT DETECTED | ` +
    `failed=${failedInterval}ms | ` +
    `new=${newInterval}ms | ` +
    `1m=${getRequestsInWindow(profile, 60 * 1000)} | ` +
    `10m=${profile.recentRequests.length} | ` +
    `error=${errorText(error)}`
  );


  // Rate limits are critical learning events.
  // Persist immediately.
  await saveRateProfile(
    profile
  );
            }


// ============================================================
// RECORD KEY FAILURE
// ============================================================

async function recordKeyFailure(
  profile,
  error
) {

  if (!profile) {
    return;
  }

  const code =
    String(
      error?.code || ''
    ).toUpperCase();

  profile.lastErrorCode =
    code || 'UNKNOWN';

  profile.lastErrorAt =
    now();

  if (
    code === 'RATE_LIMIT'
  ) {

    await recordLLMRateLimit(
      profile,
      error
    );

    profile.keyStatus =
      'cooldown';

    profile.cooldownUntil =
      new Date(
        Date.now() +
        KEY_POOL.RATE_LIMIT_COOLDOWN_MS
      );

  } else if (
    code === 'QUOTA'
  ) {

    profile.quotaHits =
      (profile.quotaHits || 0) + 1;

    profile.keyStatus =
      'cooldown';

    profile.cooldownUntil =
      new Date(
        Date.now() +
        KEY_POOL.QUOTA_COOLDOWN_MS
      );

  } else if (
    code === 'INVALID_KEY'
  ) {

    profile.invalidKeyHits =
      (profile.invalidKeyHits || 0) + 1;

    profile.keyStatus =
      'disabled';

    profile.cooldownUntil =
      new Date(
        Date.now() +
        KEY_POOL.INVALID_KEY_COOLDOWN_MS
      );

  } else if (
    code === 'PROVIDER_ERROR'
  ) {

    profile.providerErrorHits =
      (profile.providerErrorHits || 0) + 1;

    profile.keyStatus =
      'cooldown';

    profile.cooldownUntil =
      new Date(
        Date.now() +
        KEY_POOL.PROVIDER_ERROR_COOLDOWN_MS
      );
  }

  await saveRateProfile(
    profile
  );

  console.warn(
    `[KEY POOL] ${profile.keyFingerprint} ` +
    `status=${profile.keyStatus} ` +
    `reason=${code} ` +
    `cooldown=${profile.cooldownUntil || 'none'}`
  );
}


// ============================================================
// FETCH CONTRACT SOURCE
// ============================================================


async function fetchContractSource({
  address,
  chainId,
  etherscanKey
}) {

  const apiKey =
    etherscanKey ||
    process.env.ETHERSCAN_API_KEY;

  if (!apiKey) {
    throw new Error(
      'No Etherscan API key configured'
    );
  }


  // ============================================================
  // HELPER: FETCH ONE ADDRESS FROM ETHERSCAN
  // ============================================================

  async function fetchOne(contractAddress) {

    const chain =
      encodeURIComponent(chainId || '1');

    const url =
      `https://api.etherscan.io/v2/api` +
      `?chainid=${chain}` +
      `&module=contract` +
      `&action=getsourcecode` +
      `&address=${encodeURIComponent(contractAddress)}` +
      `&apikey=${encodeURIComponent(apiKey)}`;

    const response = await fetch(url);

    const raw = await response.text();


    // ----------------------------------------------------------
    // RATE LIMIT
    // ----------------------------------------------------------

    if (response.status === 429) {

      const error = new Error(
        'Explorer API rate limit reached'
      );

      error.code = 'RATE_LIMIT';
      error.httpStatus = 429;

      throw error;
    }


    // ----------------------------------------------------------
    // HTTP ERROR
    // ----------------------------------------------------------

    if (!response.ok) {

      throw new Error(
        `Explorer returned HTTP ${response.status}: ` +
        raw.slice(0, 500)
      );
    }


    // ----------------------------------------------------------
    // PARSE RESPONSE
    // ----------------------------------------------------------

    let data;

    try {

      data = JSON.parse(raw);

    } catch {

      throw new Error(
        'Explorer returned invalid JSON'
      );
    }


    const result =
      Array.isArray(data.result)
        ? data.result[0]
        : null;


    // ----------------------------------------------------------
    // NOT VERIFIED
    // ----------------------------------------------------------

    if (
      !result ||
      !result.SourceCode ||
      !String(result.SourceCode).trim()
    ) {

      return {
        verified: false,

        reason:
          data.message ||
          'Contract source not verified'
      };
    }


    return {
      verified: true,

      address: contractAddress,

      source:
        result.SourceCode,

      contractName:
        result.ContractName ||
        'Unknown',

      compilerVersion:
        result.CompilerVersion ||
        null,

      isProxy:
        result.Proxy === '1',

      implementation:
        result.Implementation ||
        null
    };
  }


  // ============================================================
  // STEP 1: FETCH ORIGINAL ADDRESS
  // ============================================================

  const base =
    await fetchOne(address);


  // Normal unverified contract → skip
  if (!base.verified) {

    return base;
  }


  // ============================================================
  // STEP 2: CHECK IF IT IS A PROXY
  // ============================================================

  const implementationAddress =
    base.implementation;


  const isValidImplementation =
    base.isProxy &&
    implementationAddress &&
    /^0x[a-fA-F0-9]{40}$/.test(
      implementationAddress
    );


  // ------------------------------------------------------------
  // NORMAL CONTRACT
  // ------------------------------------------------------------

  if (!isValidImplementation) {

    return {
      verified: true,

      address,

      auditedAddress: address,

      source:
        base.source,

      contractName:
        base.contractName,

      compilerVersion:
        base.compilerVersion,

      isProxy: false,

      implementation: null
    };
  }


  // ============================================================
  // STEP 3: FETCH IMPLEMENTATION
  // ============================================================

  console.log(
    `[PROXY DETECTED] ${address}`
  );

  console.log(
    `[IMPLEMENTATION] ${implementationAddress}`
  );


  const implementation =
    await fetchOne(
      implementationAddress
    );


  // ============================================================
  // STEP 4: IMPLEMENTATION NOT VERIFIED → SKIP
  // ============================================================

  if (!implementation.verified) {

    console.log(
      `[PROXY SKIPPED] Implementation not verified: ` +
      `${implementationAddress}`
    );

    return {
      verified: false,

      isProxy: true,

      proxyAddress: address,

      implementation:
        implementationAddress,

      reason:
        'Proxy implementation source is not verified'
    };
  }


  // ============================================================
  // STEP 5: IMPLEMENTATION VERIFIED
  // SEND IMPLEMENTATION SOURCE TO LLM
  // ============================================================

  console.log(
    `[PROXY RESOLVED] ${address}`
  );

  console.log(
    `[AUDITING IMPLEMENTATION] ${implementationAddress}`
  );


  return {
    verified: true,

    // Original address submitted by user
    address,

    // Actual address whose code is audited
    auditedAddress:
      implementationAddress,

    // IMPORTANT:
    // THIS IS IMPLEMENTATION SOURCE
    source:
      implementation.source,

    contractName:
      implementation.contractName,

    compilerVersion:
      implementation.compilerVersion,

    isProxy: true,

    proxyAddress:
      address,

    implementation:
      implementationAddress
  };
  }

// ============================================================
// PROCESS ONE ITEM
// ============================================================

async function processBatchItem(
  batch,
  item
) {

  const db =
    await getDb();


  const items =
    db.collection(
      'batch_items'
    );


  const address =
    item.address;


  console.log(
    `[BATCH ${batch.batchId}] Processing #${item.index + 1}: ${address}`
  );


  // Mark running.
  await items.updateOne(
    {
      _id:
        item._id
    },
    {
      $set: {
        status:
          'running',

        startedAt:
          now(),

        error:
          null
      }
    }
  );


  // ==========================================================
  // FETCH SOURCE
  // ==========================================================

  let contract;


  try {

    contract =
      await fetchContractSource({
        address,

        chainId:
          batch.chainId,

        etherscanKey:
          batch.etherscanKey
      });

  } catch (error) {

    if (
      isPipelineStopError(
        error
      )
    ) {

      throw error;
    }


    await items.updateOne(
      {
        _id:
          item._id
      },
      {
        $set: {

          status:
            'failed',

          error:
            errorText(
              error
            ),

          finishedAt:
            now()
        }
      }
    );


    return {
      status:
        'failed'
    };
  }


  // ==========================================================
  // UNVERIFIED CONTRACT
  // ==========================================================

  if (
    !contract.verified
  ) {

    await items.updateOne(
      {
        _id:
          item._id
      },
      {
        $set: {

          status:
            'skipped',

          error:
            contract.reason ||
            'Contract source not verified',

          finishedAt:
            now()
        }
      }
    );


    return {
      status:
        'skipped'
    };
  }

// ==========================================================
// LLM AUDIT
// ==========================================================




// The adaptive rate profile used for this request.
//
// It is returned by waitForLLMSlot() and then used to teach
// the learner whether this request succeeded or hit a limit.


let audit;
let lastAuditError = null;
let attemptedKeys = new Set();

const maxKeyAttempts =
  Math.max(
    1,
    getConfiguredLLMKeys(batch).length
  );

for (
  let attempt = 1;
  attempt <= maxKeyAttempts;
  attempt++
) {

  let rateProfile = null;
  let selectedApiKey = null;

  try {

    const slot =
      await waitForLLMSlot({
        batch
      });

    rateProfile =
      slot.profile;

    selectedApiKey =
      slot.apiKey;

    // Prevent repeatedly selecting a key that
    // already failed during this same contract audit.
    if (
      attemptedKeys.has(
        rateProfile.keyFingerprint
      )
    ) {

      rateProfile.keyStatus =
        'cooldown';

      rateProfile.cooldownUntil =
        new Date(
          Date.now() + 1000
        );

      continue;
    }

    attemptedKeys.add(
      rateProfile.keyFingerprint
    );

    console.log(
      `[BATCH ${batch.batchId}] ` +
      `LLM key=${rateProfile.keyFingerprint} ` +
      `attempt ${attempt}/${maxKeyAttempts} ` +
      `interval=${rateProfile.currentIntervalMs}ms`
    );

    audit =
      await runLLMAudit({

        source:
          contract.source,

        systemPrompt:
          batch.systemPrompt,

        model:
          batch.model,

        contractName:
          contract.contractName,

        address,

        llmUrl:
          batch.llmUrl,

        apiKey:
          selectedApiKey
      });

    await recordLLMSuccess(
      rateProfile
    );

    rateProfile.keyStatus =
      'available';

    await saveRateProfile(
      rateProfile
    );

    break;

  } catch (error) {

    lastAuditError =
      error;

    const code =
      String(
        error?.code || ''
      ).toUpperCase();

    console.warn(
      `[KEY POOL] Request failed | ` +
      `code=${code} | ` +
      `key=${rateProfile?.keyFingerprint || 'unknown'}`
    );

    if (
      [
        'RATE_LIMIT',
        'QUOTA',
        'INVALID_KEY',
        'PROVIDER_ERROR'
      ].includes(code)
    ) {

      await recordKeyFailure(
        rateProfile,
        error
      );

      // Try another key.
      continue;
    }

    if (
      isEmptyAuditResponseError(error) &&
      attempt < maxKeyAttempts
    ) {

      continue;
    }

    break;
  }
}


// ==========================================================
// ALL KEYS FAILED
// ==========================================================

if (!audit) {

  if (
    lastAuditError?.code ===
    'ALL_KEYS_UNAVAILABLE'
  ) {

    throw lastAuditError;
  }

  const allKeys =
    getConfiguredLLMKeys(batch);

  if (
    attemptedKeys.size >=
    allKeys.length
  ) {

    const error =
      new Error(
        'All LLM API keys failed or entered cooldown'
      );

    error.code =
      'ALL_KEYS_UNAVAILABLE';

    throw error;
  }
}

    

// ==========================================================
// HANDLE FINAL AUDIT FAILURE
// ==========================================================

if (
  !audit
) {

  await items.updateOne(
    {
      _id:
        item._id
    },
    {
      $set: {

        status:
          'failed',

        error:
          errorText(
            lastAuditError ||
            'LLM audit failed'
          ),

        contractName:
          contract.contractName,

        compilerVersion:
          contract.compilerVersion,

        implementation:
          contract.implementation,

        finishedAt:
          now()
      }
    }
  );

  return {
    status:
      'failed'
  };
}
  // ==========================================================
  // SAVE SUCCESS
  // ==========================================================

  await items.updateOne(
    {
      _id:
        item._id
    },
    {
     $set: {
  status: 'completed',

  // Address user originally submitted
  address: address,

  // Actual contract address audited
  auditedAddress:
    contract.auditedAddress || address,

  contractName:
    contract.contractName,

  compilerVersion:
    contract.compilerVersion,

  // Proxy metadata
  isProxy:
    contract.isProxy || false,

  implementation:
    contract.implementation || null,

  // IMPORTANT:
  // This is implementation source for proxies
  source:
    contract.source,

  audit:
    audit.result,

  truncated:
    audit.truncated,

  finishedAt:
    now()
     }
    }
  );


  return {
    status:
      'completed'
  };
}


// ============================================================
// BATCH WORKER
// ============================================================

async function startBatchWorker(
  batchId
) {

  if (
    activeWorkers.has(
      batchId
    )
  ) {

    console.log(
      `[BATCH ${batchId}] Worker already active`
    );

    return;
  }


  activeWorkers.add(
    batchId
  );


  console.log(
    `[BATCH ${batchId}] Worker started`
  );


  try {

    const db =
      await getDb();


    const batches =
      db.collection(
        'batches'
      );


    const items =
      db.collection(
        'batch_items'
      );


    // Do not overwrite a manually paused batch.
    const initialBatch =
      await batches.findOne({
        batchId
      });


    if (
      !initialBatch
    ) {

      return;
    }


    if (
      [
        'paused',
        'paused_rate_limit',
        'paused_quota',
        'cancelled',
        'completed'
      ].includes(
        initialBatch.status
      )
    ) {

      return;
    }


    await batches.updateOne(
      {
        batchId
      },
      {
        $set: {

          status:
            'running',

          updatedAt:
            now(),

          lastError:
            null
        }
      }
    );


    while (true) {

      // Always reload state.
      const batch =
        await batches.findOne({
          batchId
        });


      if (
        !batch
      ) {

        break;
      }


      // --------------------------------------------------------
      // STOP STATES
      // --------------------------------------------------------

      if (
        [
          'paused',
          'paused_rate_limit',
          'paused_quota',
          'cancelled'
        ].includes(
          batch.status
        )
      ) {

        console.log(
          `[BATCH ${batchId}] Worker stopped: ${batch.status}`
        );

        break;
      }


      // --------------------------------------------------------
      // NEXT ITEM
      // --------------------------------------------------------

      const item =
        await items.findOne(
          {
            batchId,

            status: {
              $in: [
                'pending',
                'running'
              ]
            }
          },
          {
            sort: {
              index: 1
            }
          }
        );


      if (
        !item
      ) {

        await batches.updateOne(
          {
            batchId
          },
          {
            $set: {

              status:
                'completed',

              completedAt:
                now(),

              updatedAt:
                now(),

              currentIndex:
                batch.total
            }
          }
        );


        console.log(
          `[BATCH ${batchId}] Batch completed`
        );


        break;
      }


      // --------------------------------------------------------
      // PROCESS
      // --------------------------------------------------------

      try {

        const outcome =
          await processBatchItem(
            batch,
            item
          );


        const updates = {

          currentIndex:
            item.index + 1,

          updatedAt:
            now()
        };


        if (
          outcome.status ===
          'completed'
        ) {

          updates.completed =
            (batch.completed || 0) +
            1;
        }


        if (
          outcome.status ===
            'failed' ||
          outcome.status ===
            'skipped'
        ) {

          updates.failed =
            (batch.failed || 0) +
            1;
        }


        await batches.updateOne(
          {
            batchId
          },
          {
            $set:
              updates
          }
        );


      } catch (error) {

        // ------------------------------------------------------
        // RATE LIMIT / QUOTA
        // ------------------------------------------------------

        if (
          isPipelineStopError(
            error
          )
        ) {

          const pauseStatus =
            pipelineStopType(
              error
            );


          await items.updateOne(
            {
              _id:
                item._id
            },
            {
              $set: {

                status:
                  'pending',

                error:
                  errorText(
                    error
                  ),

                startedAt:
                  null
              }
            }
          );


          await batches.updateOne(
            {
              batchId
            },
            {
              $set: {

                status:
                  pauseStatus,

                lastError:
                  errorText(
                    error
                  ),

                pausedAt:
                  now(),

                updatedAt:
                  now()
              }
            }
          );


          console.error(
            `[BATCH ${batchId}] ${pauseStatus.toUpperCase()} - PIPELINE PAUSED`
          );


          break;
        }


        // ------------------------------------------------------
        // UNEXPECTED ERROR
        // ------------------------------------------------------

        await items.updateOne(
          {
            _id:
              item._id
          },
          {
            $set: {

              status:
                'failed',

              error:
                errorText(
                  error
                ),

              finishedAt:
                now()
            }
          }
        );


        await batches.updateOne(
          {
            batchId
          },
          {
            $inc: {
              failed:
                1
            },

            $set: {

              currentIndex:
                item.index + 1,

              updatedAt:
                now()
            }
          }
        );
      }


      // Small delay after each item.
      await sleep(
        ITEM_DELAY
      );
    }


  } catch (error) {

    console.error(
      `[BATCH ${batchId}] Worker crashed:`,
      error
    );


    try {

      const db =
        await getDb();


      await db
        .collection(
          'batches'
        )
        .updateOne(
          {
            batchId
          },
          {
            $set: {

              status:
                'interrupted',

              lastError:
                errorText(
                  error
                ),

              updatedAt:
                now()
            }
          }
        );

    } catch (
      dbError
    ) {

      console.error(
        `[BATCH ${batchId}] Could not save crash state:`,
        dbError
      );
    }


  } finally {

    activeWorkers.delete(
      batchId
    );


    console.log(
      `[BATCH ${batchId}] Worker released`
    );
  }
}


// ============================================================
// CREATE BATCH
// ============================================================

async function createBatch(
  req,
  res
) {

  try {

    const {
      addresses,
      rawInput,
      chainId,
      systemPrompt,
      model,
      llmUrl
    } =
      req.body || {};


    const rawLLMKeys =
  req.headers['x-openai-keys'] ||
  req.headers['x-openai-key'] ||
  process.env.LLM_API_KEYS ||
  process.env.OPENAI_API_KEY ||
  '';

const llmKeys =
  String(rawLLMKeys)
    .split(',')
    .map(key => key.trim())
    .filter(Boolean);

if (!llmKeys.length) {
  return res
    .status(400)
    .json({
      error:
        'At least one LLM API key is required'
    });
}


    const etherscanKey =
      req.headers[
        'x-etherscan-key'
      ] ||
      process.env.ETHERSCAN_API_KEY;


  


    if (!etherscanKey) {

      return res
        .status(400)
        .json({
          error:
            'Etherscan API key is required'
        });
    }


    if (!systemPrompt) {

      return res
        .status(400)
        .json({
          error:
            'systemPrompt is required'
        });
    }


    const parsedAddresses =
      parseAddresses(
        addresses ||
        rawInput
      );


    if (
      !parsedAddresses.length
    ) {

      return res
        .status(400)
        .json({
          error:
            'No valid contract addresses found'
        });
    }


    const batchId =
      crypto.randomUUID();


    const db =
      await getDb();


    const batches =
      db.collection(
        'batches'
      );


    const items =
      db.collection(
        'batch_items'
      );


    await batches.insertOne({

      batchId,

      status:
        'queued',

      chainId:
        String(
          chainId ||
          '1'
        ),

      total:
        parsedAddresses.length,

      currentIndex:
        0,

      completed:
        0,

      failed:
        0,

      model:
        model ||
        'gpt-4o-mini',

      systemPrompt,

      llmUrl:
        llmUrl ||
        'https://api.openai.com/v1/chat/completions',

      // Retained for the current personal-tool architecture.
      // Do not use this storage approach for a public multi-user
      // production application.
     llmApiKeys:
        llmKeys,

      etherscanKey:
        etherscanKey,

      createdAt:
        now(),

      updatedAt:
        now()
    });


    await items.insertMany(
      parsedAddresses.map(
        (
          address,
          index
        ) => ({

          batchId,

          index,

          address,

          status:
            'pending',

          contractName:
            null,

          compilerVersion:
            null,

          implementation:
            null,

          source:
            null,

          audit:
            null,

          truncated:
            false,

          error:
            null,

          createdAt:
            now(),

          startedAt:
            null,

          finishedAt:
            null
        })
      )
    );


    console.log(
      `[BATCH ${batchId}] Created with ${parsedAddresses.length} addresses`
    );


    startBatchWorker(
      batchId
    ).catch(
      error => {
        console.error(
          `[BATCH ${batchId}] Background start error:`,
          error
        );
      }
    );


    return res
      .status(202)
      .json({

        batchId,

        status:
          'queued',

        total:
          parsedAddresses.length
      });


  } catch (error) {

    console.error(
      '[BATCH] Create error:',
      error
    );


    return res
      .status(500)
      .json({
        error:
          errorText(
            error
          )
      });
  }
}


// ============================================================
// GET BATCH
// ============================================================

async function getBatch(
  req,
  res
) {

  try {

    const db =
      await getDb();


    const batch =
      await db
        .collection(
          'batches'
        )
        .findOne(
          {
            batchId:
              req.params.batchId
          },
          {
            projection: {

              openaiKey:
                0,

              etherscanKey:
                0,

              systemPrompt:
                0
            }
          }
        );


    if (
      !batch
    ) {

      return res
        .status(404)
        .json({
          error:
            'Batch not found'
        });
    }


    const items =
      await db
        .collection(
          'batch_items'
        )
        .find(
          {
            batchId:
              batch.batchId
          },
          {
            projection: {

              source:
                0,

              audit:
                0
            }
          }
        )
        .sort({
          index:
            1
        })
        .limit(500)
        .toArray();


    return res.json({

      ...batch,

      items
    });


  } catch (error) {

    return res
      .status(500)
      .json({
        error:
          errorText(
            error
          )
      });
  }
}


// ============================================================
// GET BATCH ITEMS
// ============================================================

async function getBatchItems(
  req,
  res
) {

  try {

    const db =
      await getDb();


    const limit =
      Math.min(
        Number(
          req.query.limit
        ) || 100,

        500
      );


    const skip =
      Math.max(
        Number(
          req.query.skip
        ) || 0,

        0
      );


    const items =
      await db
        .collection(
          'batch_items'
        )
        .find(
          {
            batchId:
              req.params.batchId
          },
          {
            projection: {

              source:
                0,

              audit:
                0
            }
          }
        )
        .sort({
          index:
            1
        })
        .skip(
          skip
        )
        .limit(
          limit
        )
        .toArray();


    return res.json({

      items,

      skip,

      limit
    });


  } catch (error) {

    return res
      .status(500)
      .json({
        error:
          errorText(
            error
          )
      });
  }
}


// ============================================================
// PAUSE
// ============================================================

async function pauseBatch(
  req,
  res
) {

  try {

    const db =
      await getDb();


    const result =
      await db
        .collection(
          'batches'
        )
        .updateOne(
          {
            batchId:
              req.params.batchId,

            status: {
              $in: [
                'queued',
                'running'
              ]
            }
          },
          {
            $set: {

              status:
                'paused',

              pausedAt:
                now(),

              updatedAt:
                now()
            }
          }
        );


    if (
      !result.matchedCount
    ) {

      return res
        .status(404)
        .json({
          error:
            'Batch is not currently running'
        });
    }


    return res.json({

      ok:
        true,

      status:
        'paused'
    });


  } catch (error) {

    return res
      .status(500)
      .json({
        error:
          errorText(
            error
          )
      });
  }
}


// ============================================================
// RESUME
// ============================================================

async function resumeBatch(
  req,
  res
) {

  try {

    const db =
      await getDb();


    const batches =
      db.collection(
        'batches'
      );


    const batch =
      await batches.findOne({
        batchId:
          req.params.batchId
      });


    if (
      !batch
    ) {

      return res
        .status(404)
        .json({
          error:
            'Batch not found'
        });
    }


    if (
      batch.status ===
      'completed'
    ) {

      return res
        .status(400)
        .json({
          error:
            'Batch is already completed'
        });
    }


    await batches.updateOne(
      {
        batchId:
          batch.batchId
      },
      {
        $set: {

          status:
            'queued',

          resumedAt:
            now(),

          updatedAt:
            now(),

          lastError:
            null
        }
      }
    );


    startBatchWorker(
      batch.batchId
    ).catch(
      console.error
    );


    return res.json({

      ok:
        true,

      status:
        'queued'
    });


  } catch (error) {

    return res
      .status(500)
      .json({
        error:
          errorText(
            error
          )
      });
  }
}


// ============================================================
// RESTART FROM BEGINNING
// ============================================================
//
// Keeps the same batch and same addresses.
// Deletes all previous results and resets every address.
// ============================================================

async function restartBatch(
  req,
  res
) {

  try {

    const db =
      await getDb();


    const batches =
      db.collection(
        'batches'
      );


    const items =
      db.collection(
        'batch_items'
      );


    const batch =
      await batches.findOne({
        batchId:
          req.params.batchId
      });


    if (
      !batch
    ) {

      return res
        .status(404)
        .json({
          error:
            'Batch not found'
        });
    }


    // Ask the current worker to stop at its next state check.
    await batches.updateOne(
      {
        batchId:
          batch.batchId
      },
      {
        $set: {

          status:
            'paused',

          updatedAt:
            now(),

          lastError:
            null
        }
      }
    );


    // Reset every item.
    await items.updateMany(
      {
        batchId:
          batch.batchId
      },
      {
        $set: {

          status:
            'pending',

          contractName:
            null,

          compilerVersion:
            null,

          implementation:
            null,

          source:
            null,

          audit:
            null,

          truncated:
            false,

          error:
            null,

          startedAt:
            null,

          finishedAt:
            null
        }
      }
    );


    await batches.updateOne(
      {
        batchId:
          batch.batchId
      },
      {
        $set: {

          status:
            'queued',

          currentIndex:
            0,

          completed:
            0,

          failed:
            0,

          restartedAt:
            now(),

          updatedAt:
            now(),

          lastError:
            null
        }
      }
    );


    startBatchWorker(
      batch.batchId
    ).catch(
      console.error
    );


    return res.json({

      ok:
        true,

      status:
        'queued',

      currentIndex:
        0
    });


  } catch (error) {

    return res
      .status(500)
      .json({
        error:
          errorText(
            error
          )
      });
  }
}


// ============================================================
// CLEAR BATCH
// ============================================================
//
// Permanently deletes the batch and all its items.
// ============================================================

async function clearBatch(
  req,
  res
) {

  try {

    const db =
      await getDb();


    const batches =
      db.collection(
        'batches'
      );


    const items =
      db.collection(
        'batch_items'
      );


    const batch =
      await batches.findOne({
        batchId:
          req.params.batchId
      });


    if (
      !batch
    ) {

      return res
        .status(404)
        .json({
          error:
            'Batch not found'
        });
    }


    // Change state first so a running worker notices it.
    await batches.updateOne(
      {
        batchId:
          batch.batchId
      },
      {
        $set: {

          status:
            'cancelled',

          updatedAt:
            now()
        }
      }
    );


    await items.deleteMany({
      batchId:
        batch.batchId
    });


    await batches.deleteOne({
      batchId:
        batch.batchId
    });


    console.log(
      `[BATCH ${batch.batchId}] Batch cleared`
    );


    return res.json({

      ok:
        true,

      cleared:
        true
    });


  } catch (error) {

    return res
      .status(500)
      .json({
        error:
          errorText(
            error
          )
      });
  }
}


// ============================================================
// DOWNLOAD REPORT
// ============================================================

async function downloadReport(
  req,
  res
) {

  try {

    const db =
      await getDb();


    const item =
      await db
        .collection(
          'batch_items'
        )
        .findOne({
          batchId:
            req.params.batchId,

          address:
            req.params.address
        });


    if (
      !item
    ) {

      return res
        .status(404)
        .json({
          error:
            'Report not found'
        });
    }


    if (
      item.status !==
      'completed'
    ) {

      return res
        .status(400)
        .json({
          error:
            'Audit is not completed'
        });
    }


    const report = `# SMART CONTRACT SECURITY AUDIT REPORT

Generated: ${item.finishedAt || new Date().toISOString()}

## Contract Information

Address: ${item.address}

Contract Name: ${item.contractName || 'Unknown'}

Compiler: ${item.compilerVersion || 'Unknown'}

Implementation: ${item.implementation || 'N/A'}

---

# AUDIT FINDINGS

${item.audit}

---

## Disclaimer

This automated report is generated using AI-assisted analysis.
It should not be considered a replacement for a professional
manual smart contract security audit.
`;


    const safeAddress =
      item.address.replace(
        /[^a-zA-Z0-9]/g,
        '_'
      );


    res.setHeader(
      'Content-Type',
      'text/markdown; charset=utf-8'
    );


    res.setHeader(
      'Content-Disposition',
      `attachment; filename="audit-${safeAddress}.md"`
    );


    return res.send(
      report
    );


  } catch (error) {

    return res
      .status(500)
      .json({
        error:
          errorText(
            error
          )
      });
  }
}


// ============================================================
// AUTO RESUME AFTER RENDER RESTART
// ============================================================
//
// IMPORTANT:
//
// We intentionally do NOT auto-resume paused / rate-limited /
// quota-paused batches.
//
// If Render restarts while the batch was actively running,
// it resumes.
//
// If YOU paused it, it remains paused.
//
// If the provider rate-limited you, it remains paused until
// you explicitly press Resume.
// ============================================================

async function resumePendingBatches() {

  try {

    const db =
      await getDb();


    const batches =
      await db
        .collection(
          'batches'
        )
        .find({
          status: {
            $in: [
              'running',
              'queued',
              'interrupted'
            ]
          }
        })
        .toArray();


    console.log(
      `[BATCH] Found ${batches.length} resumable batch(es)`
    );


    for (
      const batch
      of batches
    ) {

      console.log(
        `[BATCH ${batch.batchId}] Resuming after server startup`
      );


      startBatchWorker(
        batch.batchId
      ).catch(
        console.error
      );
    }


  } catch (error) {

    console.error(
      '[BATCH] Auto-resume failed:',
      error
    );
  }
}

// ============================================================
// LLM RATE INTELLIGENCE STATUS
// ============================================================

async function getLLMRateStatus(
  req,
  res
) {

  try {

    const db =
      await getDb();

    const profiles =
      await db
        .collection(
          'llm_rate_profiles'
        )
        .find({})
        .sort({
          updatedAt: -1
        })
        .toArray();

    const normalized =
      profiles.map(
        raw => {

          const profile =
            normalizeRateProfile(
              raw,
              {
                profileId:
                  raw._id,

                llmUrl:
                  raw.llmUrl,

                model:
                  raw.model,

                apiKey:
                  ''
              }
            );

          pruneRateHistory(
            profile
          );

          const cooldownRemaining =
            getKeyCooldownRemaining(
              profile
            );

          let status =
            profile.keyStatus ||
            'available';

          if (
            status === 'cooldown' &&
            cooldownRemaining <= 0
          ) {
            status =
              'available';
          }

          let activity =
            'stable';

          let nextAction =
            'Maintain current interval';

          if (
            status === 'cooldown'
          ) {

            activity =
              'recovering';

            nextAction =
              'Waiting for cooldown';

          } else if (
            status === 'disabled'
          ) {

            activity =
              'disabled';

            nextAction =
              'Replace invalid key';

          } else if (
            profile.consecutiveSuccesses > 0 &&
            profile.consecutiveSuccesses <
            RATE_LEARNING.SUCCESS_THRESHOLD
          ) {

            activity =
              'learning';

            nextAction =
              'Collect more successful requests';

          } else if (
            profile.consecutiveSuccesses >=
            RATE_LEARNING.SUCCESS_THRESHOLD - 5
          ) {

            activity =
              'approaching_speedup';

            nextAction =
              'Preparing to test faster interval';
          }

          return {

            keyId:
              profile.keyFingerprint ||
              String(profile._id).slice(-12),

            status,

            activity,

            nextAction,

            currentIntervalMs:
              profile.currentIntervalMs,

            requestsPerSecond:
              Number(
                (
                  1000 /
                  profile.currentIntervalMs
                ).toFixed(2)
              ),

            fastestKnownSafeMs:
              profile.fastestKnownSafeMs,

            lastFailedIntervalMs:
              profile.lastFailedIntervalMs,

            consecutiveSuccesses:
              profile.consecutiveSuccesses,

            successThreshold:
              RATE_LEARNING.SUCCESS_THRESHOLD,

            successesUntilSpeedup:
              Math.max(
                0,
                RATE_LEARNING.SUCCESS_THRESHOLD -
                profile.consecutiveSuccesses
              ),

            totalSuccesses:
              profile.totalSuccesses,

            rateLimitHits:
              profile.rateLimitHits,

            quotaHits:
              profile.quotaHits || 0,

            providerErrorHits:
              profile.providerErrorHits || 0,

            requestsLastMinute:
              getRequestsInWindow(
                profile,
                60 * 1000
              ),

            requestsLast30Minutes:
              getRequestsInWindow(
                profile,
                30 * 60 * 1000
              ),

            cooldownRemainingMs:
              cooldownRemaining,

            lastRequestAt:
              profile.lastRequestAt || null,

            lastRateLimitAt:
              profile.lastRateLimitAt || null,

            lastErrorCode:
              profile.lastErrorCode || null
          };
        }
      );

    const availableKeys =
      normalized.filter(
        key =>
          key.status === 'available' ||
          key.status === 'active'
      ).length;

    const coolingKeys =
      normalized.filter(
        key =>
          key.status === 'cooldown'
      ).length;

    const disabledKeys =
      normalized.filter(
        key =>
          key.status === 'disabled'
      ).length;

    return res.json({

      ok: true,

      updatedAt:
        now(),

      global: {

        queueActive:
          activeWorkers.size > 0,

        activeWorkers:
          activeWorkers.size,

        totalKeys:
          normalized.length,

        availableKeys,

        coolingKeys,

        disabledKeys,

        telemetryWindowMinutes:
          30
      },

      keys:
        normalized
    });

  } catch (error) {

    return res
      .status(500)
      .json({
        error:
          errorText(error)
      });
  }
}

// ============================================================
// EXPRESS ROUTER
// ============================================================

const router =
  express.Router();


router.post(
  '/',
  createBatch
);
router.get(
  '/llm-rate-status',
  getLLMRateStatus
);

router.get(
  '/:batchId',
  getBatch
);


router.get(
  '/:batchId/items',
  getBatchItems
);


router.post(
  '/:batchId/pause',
  pauseBatch
);


router.post(
  '/:batchId/resume',
  resumeBatch
);


router.post(
  '/:batchId/restart',
  restartBatch
);


router.delete(
  '/:batchId',
  clearBatch
);


router.get(
  '/:batchId/report/:address',
  downloadReport
);


// ============================================================
// EXPORTS
// ============================================================

module.exports = {

  router,

  resumePendingBatches
};
