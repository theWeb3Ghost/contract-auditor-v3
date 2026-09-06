// api/batch-config.js

const {
  getDb
} = require('./db');


// ============================================================
// UPDATE EXISTING BATCH CONFIGURATION
// ============================================================
//
// This ONLY changes:
//
//   model
//   systemPrompt
//   llmUrl
//
// It NEVER changes:
//
//   currentIndex
//   completed
//   failed
//   total
//   batch_items
//   audit results
//
// The batch must be paused before configuration can change.
// ============================================================

async function updateBatchConfig(
  req,
  res
) {

  try {

    const {
      model,
      systemPrompt,
      llmUrl,
      llmApiKeys,
      etherscanKey,
      mode,
      com
    } = req.body || {};


    // ----------------------------------------------------------
    // VALIDATION
    // ----------------------------------------------------------

    const cleanModel =
      String(
        model || ''
      ).trim();

    const cleanPrompt =
      String(
        systemPrompt || ''
      );

    const cleanLlmUrl =
      String(
        llmUrl || ''
      ).trim();

    // llmApiKeys can arrive as an array (already split by the
    // frontend) or as a raw comma-separated string. Support both.
    const cleanApiKeys =
      Array.isArray(llmApiKeys)
        ? llmApiKeys
            .map(key => String(key || '').trim())
            .filter(Boolean)
        : String(llmApiKeys || '')
            .split(',')
            .map(key => key.trim())
            .filter(Boolean);

    const cleanEtherscanKey =
      String(
        etherscanKey || ''
      ).trim();

    const cleanMode = mode === 'com' ? 'com' : 'normal';
    const cleanCom = com && typeof com === 'object' ? {
      enabled: cleanMode === 'com',
      llmA: {
        url: String(com.llmA?.url || '').trim(),
        model: String(com.llmA?.model || '').trim(),
        apiKeys: Array.isArray(com.llmA?.apiKeys)
          ? [...new Set(com.llmA.apiKeys.map(k => String(k || '').trim()).filter(Boolean))]
          : []
      },
      llmB: {
        url: String(com.llmB?.url || '').trim(),
        model: String(com.llmB?.model || '').trim(),
        apiKeys: Array.isArray(com.llmB?.apiKeys)
          ? [...new Set(com.llmB.apiKeys.map(k => String(k || '').trim()).filter(Boolean))]
          : []
      }
    } : null;

    if (!cleanPrompt.trim()) {
      return res.status(400).json({ ok: false, error: 'System prompt is required' });
    }

    if (cleanMode === 'normal') {
      if (!cleanModel) {
        return res.status(400).json({ ok: false, error: 'Normal model is required' });
      }
      if (!cleanLlmUrl) {
        return res.status(400).json({ ok: false, error: 'Normal LLM endpoint is required' });
      }
      if (!cleanApiKeys.length) {
        return res.status(400).json({ ok: false, error: 'Normal mode requires at least one API key from Normal settings' });
      }
    }

    if (cleanMode === 'com' && (
      !cleanCom ||
      !cleanCom.llmA.url || !cleanCom.llmA.model || !cleanCom.llmA.apiKeys.length ||
      !cleanCom.llmB.url || !cleanCom.llmB.model || !cleanCom.llmB.apiKeys.length
    )) {
      return res.status(400).json({
        ok: false,
        error: 'COM mode requires URL, model and at least one API key for both LLM A and LLM B from COM settings'
      });
    }

    // ----------------------------------------------------------
    // DATABASE
    // ----------------------------------------------------------

    const db =
      await getDb();

    const batches =
      db.collection(
        'batches'
      );


    // ----------------------------------------------------------
    // FIND BATCH
    // ----------------------------------------------------------

    const batch =
      await batches.findOne({
        batchId:
          req.params.batchId
      });


    if (!batch) {

      return res
        .status(404)
        .json({
          ok: false,
          error:
            'Batch not found'
        });

    }


    // ----------------------------------------------------------
    // ONLY ALLOW CHANGES WHILE PAUSED
    // ----------------------------------------------------------
    //
    // Your existing pause route changes the status to "paused".
    //
    // "paused_rate_limit" is also supported because your pipeline
    // can intentionally stop itself there.
    //
    // "interrupted" is included so a recovered batch can have its
    // configuration changed before being resumed.
    // ----------------------------------------------------------

    const allowedStatuses = [
      'paused',
      'paused_rate_limit',
      'interrupted'
    ];


    if (
      !allowedStatuses.includes(
        batch.status
      )
    ) {

      return res
        .status(409)
        .json({
          ok: false,
          error:
            'Pause the batch before changing its model or prompt',
          status:
            batch.status
        });

    }


    // ----------------------------------------------------------
    // UPDATE ONLY CONFIGURATION
    // ----------------------------------------------------------

    const updatedAt =
      new Date();

    // Only overwrite credentials if the caller actually sent new
    // ones. An empty field means "leave this alone" — otherwise
    // every model/prompt tweak would force you to re-paste keys.
    const setFields = {

      model:
        cleanModel,

      systemPrompt:
        cleanPrompt,

      llmUrl:
        cleanLlmUrl,

      configUpdatedAt:
        updatedAt,

      updatedAt,

      mode: cleanMode

    };

    if (cleanCom) setFields.com = cleanCom;

    if (cleanApiKeys.length) {
      setFields.llmApiKeys = cleanApiKeys;
    }

    if (cleanEtherscanKey) {
      setFields.etherscanKey = cleanEtherscanKey;
    }


    const result =
      await batches.updateOne(
        {
          batchId:
            batch.batchId,

          status: {
            $in:
              allowedStatuses
          }
        },

        {
          $set: setFields
        }
      );


    // ----------------------------------------------------------
    // SAFETY CHECK
    // ----------------------------------------------------------

    if (
      !result.matchedCount
    ) {

      return res
        .status(409)
        .json({
          ok: false,
          error:
            'Batch changed state before configuration could be updated'
        });

    }


    // ----------------------------------------------------------
    // RETURN THE EXACT CONFIG NOW STORED
    // ----------------------------------------------------------

    return res.json({

      ok: true,

      batchId:
        batch.batchId,

      status:
        batch.status,

      config: {

        model:
          cleanModel,

        systemPrompt:
          cleanPrompt,

        llmUrl:
          cleanLlmUrl,

        mode: cleanMode,

        comConfigured: Boolean(cleanCom),

        // Never echo raw keys back to the client — just confirm
        // whether they were actually changed this call.
        apiKeysUpdated:
          cleanApiKeys.length > 0,

        etherscanKeyUpdated:
          Boolean(cleanEtherscanKey)

      },

      message:
        'Batch configuration updated. Resume the batch to continue with the new configuration.'

    });


  } catch (error) {

    console.error(
      '[BATCH CONFIG] Update error:',
      error
    );


    return res
      .status(500)
      .json({

        ok: false,

        error:
          String(
            error?.message ||
            error ||
            'Unknown error'
          )

      });

  }

}


module.exports = {
  updateBatchConfig
};
