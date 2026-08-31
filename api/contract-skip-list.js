// api/contract-skip-list.js

const express = require('express');

const {
  getDb
} = require('./db');


// ============================================================
// COLLECTION
// ============================================================

const COLLECTION =
  'contract_skip_names';


// ============================================================
// NORMALIZE CONTRACT NAME
// ============================================================
//
// Matching is:
// - case insensitive
// - ignores leading/trailing spaces
//
// Example:
//
// "GnosisSafe"
// "gnosissafe"
// "  GNOSISSAFE  "
//
// All become:
//
// "gnosissafe"
// ============================================================

function normalizeContractName(
  name
) {

  if (
    typeof name !== 'string'
  ) {
    return '';
  }

  return name
    .trim()
    .toLowerCase();
}


// ============================================================
// CHECK IF CONTRACT NAME SHOULD BE SKIPPED
// ============================================================
//
// This is used directly by batch.js.
//
// Returns:
//
// {
//   skip: true,
//   rule: {...}
// }
//
// or:
//
// {
//   skip: false,
//   rule: null
// }
// ============================================================

async function shouldSkipContractName(
  contractName
) {

  const normalizedName =
    normalizeContractName(
      contractName
    );

  // Empty names should never match.
  if (!normalizedName) {
    return {
      skip: false,
      rule: null
    };
  }

  const db =
    await getDb();

  const rule =
    await db
      .collection(
        COLLECTION
      )
      .findOne({
        normalizedName,
        enabled: true
      });

  return {
    skip: Boolean(rule),
    rule: rule || null
  };
}


// ============================================================
// LIST ALL SKIP RULES
// ============================================================

async function getSkipList(
  req,
  res
) {

  try {

    const db =
      await getDb();

    const rules =
      await db
        .collection(
          COLLECTION
        )
        .find(
          {}
        )
        .sort({
          createdAt: -1
        })
        .toArray();

    return res.json({
      ok: true,
      rules
    });

  } catch (error) {

    console.error(
      '[CONTRACT SKIP LIST] Failed to load:',
      error
    );

    return res
      .status(500)
      .json({
        ok: false,
        error:
          error.message ||
          'Failed to load contract skip list'
      });
  }
}


// ============================================================
// ADD CONTRACT NAME
// ============================================================

async function addSkipName(
  req,
  res
) {

  try {

    const contractName =
      String(
        req.body?.contractName ||
        ''
      ).trim();

    const normalizedName =
      normalizeContractName(
        contractName
      );

    if (!normalizedName) {

      return res
        .status(400)
        .json({
          ok: false,
          error:
            'Contract name is required'
        });
    }

    const db =
      await getDb();

    const collection =
      db.collection(
        COLLECTION
      );

    // Prevent duplicates.
    const existing =
      await collection.findOne({
        normalizedName
      });

    if (existing) {

      return res
        .status(409)
        .json({
          ok: false,
          error:
            `"${existing.contractName}" is already in the skip list`
        });
    }

    const rule = {

      contractName,

      normalizedName,

      enabled: true,

      createdAt:
        new Date(),

      updatedAt:
        new Date()
    };

    const result =
      await collection.insertOne(
        rule
      );

    console.log(
      `[CONTRACT SKIP LIST] Added: ${contractName}`
    );

    return res.json({
      ok: true,
      message:
        `"${contractName}" added to skip list`,
      rule: {
        ...rule,
        _id:
          result.insertedId
      }
    });

  } catch (error) {

    console.error(
      '[CONTRACT SKIP LIST] Add failed:',
      error
    );

    return res
      .status(500)
      .json({
        ok: false,
        error:
          error.message ||
          'Failed to add contract name'
      });
  }
}


// ============================================================
// REMOVE CONTRACT NAME
// ============================================================
//
// We remove using normalizedName.
//
// Example:
//
// DELETE
// /api/contract-skip-list/gnosissafe
// ============================================================

async function removeSkipName(
  req,
  res
) {

  try {

    const normalizedName =
      normalizeContractName(
        decodeURIComponent(
          req.params.name || ''
        )
      );

    if (!normalizedName) {

      return res
        .status(400)
        .json({
          ok: false,
          error:
            'Contract name is required'
        });
    }

    const db =
      await getDb();

    const result =
      await db
        .collection(
          COLLECTION
        )
        .deleteOne({
          normalizedName
        });

    if (
      !result.deletedCount
    ) {

      return res
        .status(404)
        .json({
          ok: false,
          error:
            'Contract name not found'
        });
    }

    console.log(
      `[CONTRACT SKIP LIST] Removed: ${normalizedName}`
    );

    return res.json({
      ok: true,
      message:
        'Contract name removed from skip list'
    });

  } catch (error) {

    console.error(
      '[CONTRACT SKIP LIST] Remove failed:',
      error
    );

    return res
      .status(500)
      .json({
        ok: false,
        error:
          error.message ||
          'Failed to remove contract name'
      });
  }
}


// ============================================================
// CREATE INDEX
// ============================================================
//
// Prevents duplicate normalized names.
// ============================================================

async function ensureSkipListIndexes() {

  const db =
    await getDb();

  await db
    .collection(
      COLLECTION
    )
    .createIndex(
      {
        normalizedName: 1
      },
      {
        unique: true
      }
    );
}


// ============================================================
// EXPRESS ROUTER
// ============================================================

const router =
  express.Router();


// GET all rules

router.get(
  '/',
  getSkipList
);


// ADD a name

router.post(
  '/',
  addSkipName
);


// REMOVE a name

router.delete(
  '/:name',
  removeSkipName
);


// ============================================================
// EXPORTS
// ============================================================

module.exports = {

  router,

  shouldSkipContractName,

  normalizeContractName,

  ensureSkipListIndexes
};
