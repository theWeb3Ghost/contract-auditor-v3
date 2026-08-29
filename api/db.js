
// api/db.js

const { MongoClient } = require('mongodb');

let client = null;
let db = null;

async function getDb() {
  if (db) return db;

  const uri = process.env.MONGODB_URI;

  if (!uri) {
    throw new Error(
      'MONGODB_URI environment variable is not configured'
    );
  }

  client = new MongoClient(uri, {
    maxPoolSize: 5,
    minPoolSize: 1,
    serverSelectionTimeoutMS: 10000
  });

  await client.connect();

  db = client.db(
    process.env.MONGODB_DB || 'contract_auditor'
  );

  console.log('[DB] MongoDB connected');

  // Create useful indexes.
  await Promise.all([
    db.collection('batches').createIndex({ batchId: 1 }, { unique: true }),
    db.collection('batch_items').createIndex(
      { batchId: 1, index: 1 },
      { unique: true }
    ),
    db.collection('batch_items').createIndex(
      { batchId: 1, status: 1, index: 1 }
    )
  ]);

  return db;
}

async function closeDb() {
  if (client) {
    await client.close();
    client = null;
    db = null;
  }
}

module.exports = {
  getDb,
  closeDb
};
