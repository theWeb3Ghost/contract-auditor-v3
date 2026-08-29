
// api/batch.js

const crypto = require('crypto');
const { getDb } = require('./db');
const { runLLMAudit, isRateLimitError } = require('./llm');


// ============================================================
// WORKER STATE
// ============================================================
//
// This only prevents two workers in THIS Node process from
// processing the same batch simultaneously.
//
// The actual batch state lives in MongoDB.
//

const activeWorkers = new Set();


// ============================================================
// HELPERS
// ============================================================

const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;


function now() {
  return new Date();
}


function cleanAddress(address) {
  if (typeof address !== 'string') return null;

  const value = address.trim();

  return ADDRESS_RE.test(value)
    ? value
    : null;
}


function parseAddresses(input) {

  const addresses = [];
  const seen = new Set();


  function add(value) {

    const address = cleanAddress(value);

    if (!address) return;

    const normalized = address.toLowerCase();

    if (seen.has(normalized)) return;

    seen.add(normalized);

    addresses.push(address);
  }


  // Array of addresses.
  if (Array.isArray(input)) {

    for (const item of input) {

      if (typeof item === 'string') {
        add(item);
      }

      if (
        item &&
        typeof item === 'object'
      ) {
        add(
          item.address ||
          item.contractAddress
        );
      }
    }

    return addresses;
  }


  // Object containing addresses.
  if (
    input &&
    typeof input === 'object'
  ) {

    if (Array.isArray(input.addresses)) {
      return parseAddresses(input.addresses);
    }

    if (input.address) {
      add(input.address);
    }

    return addresses;
  }


  // Raw JSONL / pasted text.
  if (typeof input === 'string') {

    const lines = input.split(/\r?\n/);

    for (const line of lines) {

      const trimmed = line.trim();

      if (!trimmed) continue;


      // Try JSONL first.
      if (
        trimmed.startsWith('{') ||
        trimmed.startsWith('[')
      ) {

        try {

          const parsed = JSON.parse(trimmed);

          const nested =
            parseAddresses(parsed);

          for (const address of nested) {
            add(address);
          }

          continue;

        } catch {
          // Fall through to text parsing.
        }
      }


      // Split CSV / whitespace.
      const parts =
        trimmed.split(/[\s,;]+/);

      for (const part of parts) {
        add(part);
      }
    }
  }


  return addresses;
}


function isRateLimit(error) {

  if (!error) return false;

  if (error.code === 'RATE_LIMIT') {
    return true;
  }

  if (error.httpStatus === 429) {
    return true;
  }

  const message =
    String(
      error.message ||
      error.responseText ||
      error
    ).toLowerCase();

  return (
    message.includes('rate limit') ||
    message.includes('rate_limit') ||
    message.includes('too many requests') ||
    message.includes('http 429')
  );
}


// ============================================================
// FETCH CONTRACT SOURCE
// ============================================================
//
// Calls your existing etherscan route logic indirectly through
// the Etherscan API.
//
// We keep this independent from Express so the batch worker can
// run without making HTTP requests back into itself.
//

async function fetchContractSource({
  address,
  chainId,
  etherscanKey
}) {

  const chain =
    encodeURIComponent(chainId || '1');

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


  const response = await fetch(url);

  const raw = await response.text();


  if (response.status === 429) {

    const error = new Error(
      'Explorer API rate limit reached'
    );

    error.code = 'RATE_LIMIT';
    error.httpStatus = 429;

    throw error;
  }


  if (!response.ok) {

    throw new Error(
      `Explorer returned HTTP ${response.status}: ${raw.slice(0, 500)}`
    );
  }


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

    source: result.SourceCode,

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

  const db = await getDb();


  const items =
    db.collection('batch_items');


  const address = item.address;


  console.log(
    `[BATCH ${batch.batchId}] Processing #${item.index}: ${address}`
  );


  // Mark item running.
  await items.updateOne(
    {
      _id: item._id
    },
    {
      $set: {
        status: 'running',
        startedAt: now()
      }
    }
  );


  // ----------------------------------------------------------
  // FETCH SOURCE
  // ----------------------------------------------------------

  let contract;

  try {

    contract =
      await fetchContractSource({
        address,
        chainId: batch.chainId,
        etherscanKey: batch.etherscanKey
      });

  } catch (error) {

    if (isRateLimit(error)) {
      throw error;
    }


    await items.updateOne(
      { _id: item._id },
      {
        $set: {
          status: 'failed',
          error: String(
            error.message || error
          ),
          finishedAt: now()
        }
      }
    );

    return {
      status: 'failed'
    };
  }


  if (!contract.verified) {

    await items.updateOne(
      { _id: item._id },
      {
        $set: {
          status: 'skipped',
          error:
            contract.reason ||
            'Contract source not verified',
          finishedAt: now()
        }
      }
    );

    return {
      status: 'skipped'
    };
  }


  // ----------------------------------------------------------
  // RUN LLM AUDIT
  // ----------------------------------------------------------

  let audit;

  try {

    audit =
      await runLLMAudit({
        source: contract.source,
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

    if (isRateLimit(error)) {
      throw error;
    }


    await items.updateOne(
      { _id: item._id },
      {
        $set: {
          status: 'failed',
          error: String(
            error.message || error
          ),
          contractName:
            contract.contractName,
          finishedAt: now()
        }
      }
    );

    return {
      status: 'failed'
    };
  }


  // ----------------------------------------------------------
  // SAVE SUCCESS
  // ----------------------------------------------------------

  await items.updateOne(
    { _id: item._id },
    {
      $set: {
        status: 'completed',

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
          now()
      }
    }
  );


  return {
    status: 'completed'
  };
}


// ============================================================
// BATCH WORKER
// ============================================================

async function startBatchWorker(batchId) {

  if (activeWorkers.has(batchId)) {

    console.log(
      `[BATCH ${batchId}] Worker already active`
    );

    return;
  }


  activeWorkers.add(batchId);


  console.log(
    `[BATCH ${batchId}] Worker started`
  );


  try {

    const db = await getDb();

    const batches =
      db.collection('batches');

    const items =
      db.collection('batch_items');


    // Mark running.
    await batches.updateOne(
      { batchId },
      {
        $set: {
          status: 'running',
          updatedAt: now(),
          lastError: null
        }
      }
    );


    while (true) {

      // Reload batch state every iteration.
      const batch =
        await batches.findOne({
          batchId
        });


      if (!batch) {
        break;
      }


      // Stop immediately if manually paused or rate limited.
      if (
        batch.status === 'paused' ||
        batch.status === 'paused_rate_limit' ||
        batch.status === 'cancelled'
      ) {

        console.log(
          `[BATCH ${batchId}] Worker stopped: ${batch.status}`
        );

        break;
      }


      // Find the next item.
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


      if (!item) {

        await batches.updateOne(
          { batchId },
          {
            $set: {
              status: 'completed',
              completedAt: now(),
              updatedAt: now()
            }
          }
        );


        console.log(
          `[BATCH ${batchId}] Batch completed`
        );

        break;
      }


      // ------------------------------------------------------
      // PROCESS ONE ADDRESS
      // ------------------------------------------------------

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
          outcome.status === 'completed'
        ) {
          updates.completed =
            (batch.completed || 0) + 1;
        }

        if (
          outcome.status === 'failed' ||
          outcome.status === 'skipped'
        ) {
          updates.failed =
            (batch.failed || 0) + 1;
        }


        // CHECKPOINT
        await batches.updateOne(
          { batchId },
          {
            $set: updates
          }
        );


      } catch (error) {

        // ----------------------------------------------------
        // RATE LIMIT = STOP EVERYTHING
        // ----------------------------------------------------

        if (isRateLimit(error)) {

          await items.updateOne(
            { _id: item._id },
            {
              $set: {
                status: 'pending',

                error:
                  'Paused because API rate limit was detected',

                startedAt:
                  null
              }
            }
          );


          await batches.updateOne(
            { batchId },
            {
              $set: {
                status:
                  'paused_rate_limit',

                lastError:
                  String(
                    error.message || error
                  ),

                pausedAt:
                  now(),

                updatedAt:
                  now()
              }
            }
          );


          console.error(
            `[BATCH ${batchId}] RATE LIMIT - PIPELINE PAUSED`
          );


          break;
        }


        // ----------------------------------------------------
        // UNEXPECTED ERROR
        // ----------------------------------------------------

        await items.updateOne(
          { _id: item._id },
          {
            $set: {
              status: 'failed',

              error:
                String(
                  error.message || error
                ),

              finishedAt:
                now()
            }
          }
        );


        await batches.updateOne(
          { batchId },
          {
            $inc: {
              failed: 1
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


      // Small delay prevents aggressive API hammering.
      await new Promise(
        resolve => setTimeout(resolve, 500)
      );
    }

  } catch (error) {

    console.error(
      `[BATCH ${batchId}] Worker crashed:`,
      error
    );

    try {

      const db = await getDb();

      await db
        .collection('batches')
        .updateOne(
          { batchId },
          {
            $set: {
              status: 'interrupted',

              lastError:
                String(
                  error.message || error
                ),

              updatedAt:
                now()
            }
          }
        );

    } catch (dbError) {

      console.error(
        `[BATCH ${batchId}] Could not save crash state:`,
        dbError
      );
    }

  } finally {

    activeWorkers.delete(batchId);

    console.log(
      `[BATCH ${batchId}] Worker released`
    );
  }
}


// ============================================================
// CREATE BATCH
// ============================================================

async function createBatch(req, res) {

  try {

    const {
      addresses,
      rawInput,
      chainId,
      systemPrompt,
      model,
      llmUrl
    } = req.body || {};


    const apiKey =
      req.headers['x-openai-key'] ||
      process.env.OPENAI_API_KEY;


    const etherscanKey =
      req.headers['x-etherscan-key'] ||
      process.env.ETHERSCAN_API_KEY;


    if (!apiKey) {

      return res.status(400).json({
        error:
          'LLM API key is required'
      });
    }


    if (!etherscanKey) {

      return res.status(400).json({
        error:
          'Etherscan API key is required'
      });
    }


    if (!systemPrompt) {

      return res.status(400).json({
        error:
          'systemPrompt is required'
      });
    }


    const parsedAddresses =
      parseAddresses(
        addresses || rawInput
      );


    if (!parsedAddresses.length) {

      return res.status(400).json({
        error:
          'No valid contract addresses found'
      });
    }


    const batchId =
      crypto.randomUUID();


    const db =
      await getDb();


    const batches =
      db.collection('batches');

    const items =
      db.collection('batch_items');


    // Save batch metadata.
    await batches.insertOne({

      batchId,

      status: 'queued',

      chainId:
        String(chainId || '1'),

      total:
        parsedAddresses.length,

      currentIndex: 0,

      completed: 0,

      failed: 0,

      model:
        model || 'gpt-4o-mini',

      systemPrompt,

      llmUrl:
        llmUrl ||
        'https://api.openai.com/v1/chat/completions',

      // NOTE:
      // For a production multi-user application,
      // do NOT permanently store user API keys like this.
      // This is retained here to allow background/resume
      // processing in the current personal-tool architecture.
      openaiKey:
        apiKey,

      etherscanKey:
        etherscanKey,

      createdAt:
        now(),

      updatedAt:
        now()
    });


    // Insert addresses individually.
    await items.insertMany(
      parsedAddresses.map(
        (address, index) => ({

          batchId,

          index,

          address,

          status: 'pending',

          contractName: null,

          source: null,

          audit: null,

          error: null,

          createdAt:
            now(),

          startedAt: null,

          finishedAt: null
        })
      )
    );


    console.log(
      `[BATCH ${batchId}] Created with ${parsedAddresses.length} addresses`
    );


    // Start worker without blocking response.
    startBatchWorker(batchId)
      .catch(error => {
        console.error(
          `[BATCH ${batchId}] Background start error:`,
          error
        );
      });


    return res.status(202).json({

      batchId,

      status: 'queued',

      total:
        parsedAddresses.length

    });

  } catch (error) {

    console.error(
      '[BATCH] Create error:',
      error
    );

    return res.status(500).json({
      error:
        error.message ||
        'Could not create batch'
    });
  }
}


// ============================================================
// GET BATCH STATUS
// ============================================================

async function getBatch(req, res) {

  try {

    const db =
      await getDb();


    const batch =
      await db
        .collection('batches')
        .findOne(
          {
            batchId:
              req.params.batchId
          },
          {
            projection: {
              openaiKey: 0,
              etherscanKey: 0,
              systemPrompt: 0
            }
          }
        );


    if (!batch) {

      return res.status(404).json({
        error:
          'Batch not found'
      });
    }


    const items =
      await db
        .collection('batch_items')
        .find(
          {
            batchId:
              batch.batchId
          },
          {
            projection: {
              source: 0,
              audit: 0
            }
          }
        )
        .sort({ index: 1 })
        .limit(100)
        .toArray();


    return res.json({
      ...batch,
      items
    });

  } catch (error) {

    return res.status(500).json({
      error:
        error.message
    });
  }
}


// ============================================================
// GET BATCH ITEMS
// ============================================================

async function getBatchItems(req, res) {

  try {

    const db =
      await getDb();


    const limit =
      Math.min(
        Number(req.query.limit) || 100,
        500
      );


    const skip =
      Math.max(
        Number(req.query.skip) || 0,
        0
      );


    const items =
      await db
        .collection('batch_items')
        .find(
          {
            batchId:
              req.params.batchId
          },
          {
            projection: {
              source: 0,
              audit: 0
            }
          }
        )
        .sort({ index: 1 })
        .skip(skip)
        .limit(limit)
        .toArray();


    return res.json({
      items,
      skip,
      limit
    });

  } catch (error) {

    return res.status(500).json({
      error:
        error.message
    });
  }
}


// ============================================================
// PAUSE
// ============================================================

async function pauseBatch(req, res) {

  try {

    const db =
      await getDb();


    await db
      .collection('batches')
      .updateOne(
        {
          batchId:
            req.params.batchId
        },
        {
          $set: {
            status: 'paused',
            pausedAt: now(),
            updatedAt: now()
          }
        }
      );


    return res.json({
      ok: true,
      status: 'paused'
    });

  } catch (error) {

    return res.status(500).json({
      error:
        error.message
    });
  }
}


// ============================================================
// RESUME
// ============================================================

async function resumeBatch(req, res) {

  try {

    const db =
      await getDb();


    const batch =
      await db
        .collection('batches')
        .findOne(
          {
            batchId:
              req.params.batchId
          }
        );


    if (!batch) {

      return res.status(404).json({
        error:
          'Batch not found'
      });
    }


    if (
      batch.status === 'completed'
    ) {

      return res.status(400).json({
        error:
          'Batch is already completed'
      });
    }


    await db
      .collection('batches')
      .updateOne(
        {
          batchId:
            batch.batchId
        },
        {
          $set: {
            status: 'queued',
            resumedAt: now(),
            updatedAt: now(),
            lastError: null
          }
        }
      );


    startBatchWorker(batch.batchId)
      .catch(console.error);


    return res.json({
      ok: true,
      status: 'queued'
    });

  } catch (error) {

    return res.status(500).json({
      error:
        error.message
    });
  }
}


// ============================================================
// DOWNLOAD INDIVIDUAL REPORT
// ============================================================

async function downloadReport(req, res) {

  try {

    const db =
      await getDb();


    const item =
      await db
        .collection('batch_items')
        .findOne({
          batchId:
            req.params.batchId,

          address:
            req.params.address
        });


    if (!item) {

      return res.status(404).json({
        error:
          'Report not found'
      });
    }


    if (
      item.status !== 'completed'
    ) {

      return res.status(400).json({
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


    return res.send(report);

  } catch (error) {

    return res.status(500).json({
      error:
        error.message
    });
  }
}


// ============================================================
// AUTO RESUME
// ============================================================

async function resumePendingBatches() {

  try {

    const db =
      await getDb();


    const batches =
      await db
        .collection('batches')
        .find(
          {
            status: {
              $in: [
                'running',
                'queued',
                'interrupted'
              ]
            }
          }
        )
        .toArray();


    console.log(
      `[BATCH] Found ${batches.length} resumable batch(es)`
    );


    for (const batch of batches) {

      console.log(
        `[BATCH ${batch.batchId}] Resuming after server startup`
      );


      startBatchWorker(batch.batchId)
        .catch(console.error);
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

const express =
  require('express');


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

router.get(
  '/:batchId/report/:address',
  downloadReport
);


module.exports = {
  router,
  resumePendingBatches
};
