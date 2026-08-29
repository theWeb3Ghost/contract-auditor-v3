
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
  60000;

// Minimum time between LLM requests.
//
// 600ms ~= 1.67 requests/sec.
// This stays below a 2 req/sec free-tier ceiling.
const LLM_REQUEST_INTERVAL =
  600;

// How long to wait between completed contracts.
const ITEM_DELAY =
  200;


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
    code === 'QUOTA'
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
// GLOBAL LLM THROTTLE
// ============================================================
//
// One Node process = one Render worker.
//
// This guarantees only one LLM request at a time and at least
// 600ms between LLM requests.
// ============================================================

let lastLLMRequestAt =
  0;

let llmThrottle =
  Promise.resolve();


function waitForLLMSlot() {

  const next =
    llmThrottle.then(
      async () => {

        const elapsed =
          Date.now() -
          lastLLMRequestAt;


        const wait =
          Math.max(
            0,
            LLM_REQUEST_INTERVAL -
            elapsed
          );


        if (
          wait > 0
        ) {

          await sleep(
            wait
          );
        }


        lastLLMRequestAt =
          Date.now();
      }
    );


  llmThrottle =
    next.catch(
      () => {}
    );


  return next;
}


// ============================================================
// FETCH CONTRACT SOURCE
// ============================================================

async function fetchContractSource({
  address,
  chainId,
  etherscanKey
}) {

  const chain =
    encodeURIComponent(
      chainId || '1'
    );


  const apiKey =
    etherscanKey ||
    process.env.ETHERSCAN_API_KEY;


  if (!apiKey) {

    throw new Error(
      'No Etherscan API key configured'
    );
  }


  const url =
    `https://api.etherscan.io/v2/api` +
    `?chainid=${chain}` +
    `&module=contract` +
    `&action=getsourcecode` +
    `&address=${encodeURIComponent(address)}` +
    `&apikey=${encodeURIComponent(apiKey)}`;


  const response =
    await fetch(
      url
    );


  const raw =
    await response.text();


  if (
    response.status ===
    429
  ) {

    const error =
      new Error(
        'Explorer API rate limit reached'
      );


    error.code =
      'RATE_LIMIT';

    error.httpStatus =
      429;

    error.responseText =
      raw;


    throw error;
  }


  if (
    !response.ok
  ) {

    throw new Error(
      `Explorer returned HTTP ${response.status}: ${raw.slice(0, 500)}`
    );
  }


  let data;


  try {

    data =
      JSON.parse(
        raw
      );

  } catch {

    throw new Error(
      'Explorer returned invalid JSON'
    );
  }


  const result =
    Array.isArray(
      data.result
    )
      ? data.result[0]
      : null;


  if (
    !result ||
    !result.SourceCode
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

    source:
      result.SourceCode,

    contractName:
      result.ContractName ||
      'Unknown',

    compilerVersion:
      result.CompilerVersion ||
      null,

    implementation:
      result.Implementation ||
      null
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

  let audit;


  try {

    // Proactive free-tier throttle.
    await waitForLLMSlot();


    console.log(
      `[BATCH ${batch.batchId}] LLM slot acquired for ${address}`
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
          batch.openaiKey
      });

  } catch (error) {

    if (
      isPipelineStopError(
        error
      )
    ) {

      throw error;
    }


    // Individual audit error:
    // mark this address failed and continue.
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

        status:
          'completed',

        contractName:
          contract.contractName,

        compilerVersion:
          contract.compilerVersion,

        implementation:
          contract.implementation,

        source:
          contract.source,

        audit:
          audit.result,

        truncated:
          audit.truncated,

        finishedAt:
          now(),

        error:
          null
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


    const apiKey =
      req.headers[
        'x-openai-key'
      ] ||
      process.env.OPENAI_API_KEY;


    const etherscanKey =
      req.headers[
        'x-etherscan-key'
      ] ||
      process.env.ETHERSCAN_API_KEY;


    if (!apiKey) {

      return res
        .status(400)
        .json({
          error:
            'LLM API key is required'
        });
    }


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
      openaiKey:
        apiKey,

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
// EXPRESS ROUTER
// ============================================================

const router =
  express.Router();


router.post(
  '/',
  createBatch
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
