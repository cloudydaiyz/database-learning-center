// @ts-check
import { MongoClient } from "mongodb";
import { MongoMemoryServer, MongoMemoryReplSet } from "mongodb-memory-server";

// Transactions need MongoMemoryReplSet, or else you would get the following error:
// `MongoServerError: Transaction numbers are only allowed on a replica set member or mongos`
// See: https://github.com/typegoose/mongodb-memory-server/issues/74

// From: https://www.mongodb.com/docs/manual/core/transactions-in-applications/
async function transactionExample() {

  /** 
   * Example 1: Callback API 
   * Starts a transaction, executes the specified operations, and commits (or aborts on error).
   * Automatically incorporates error handling logic for TransientTransactionError and UnknownTransactionCommitResult.
   */
  const server = await MongoMemoryReplSet.create();

  // For a replica set, include the replica set name and a seedlist of the members in the URI string; e.g.
  // const uri = 'mongodb://mongodb0.example.com:27017,mongodb1.example.com:27017/?replicaSet=myRepl'
  // For a sharded cluster, connect to the mongos instances; e.g.
  // const uri = 'mongodb://mongos0.example.com:27017,mongos1.example.com:27017/'

  const client = new MongoClient(server.getUri());
  await client.connect();

  // Prereq: Create collections.
  await client
    .db('mydb1')
    .collection('foo')
    .insertOne({ abc: 0 }, { writeConcern: { w: 'majority' } });

  await client
    .db('mydb2')
    .collection('bar')
    .insertOne({ xyz: 0 }, { writeConcern: { w: 'majority' } });

  // Step 1: Start a Client Session
  const session = client.startSession();

  // Step 2: Optional. Define options to use for the transaction
  // ts-check is being weird about this so it's omitted for now
  // const transactionOptions = {
  //   readPreference: 'primary',
  //   readConcern: { level: 'local' },
  //   writeConcern: { w: 'majority' }
  // };

  // Step 3: Use withTransaction to start a transaction, execute the callback, and commit (or abort on error)
  // Note: The callback for withTransaction MUST be async and/or return a Promise.
  try {
    await session.withTransaction(async () => {
      const coll1 = client.db('mydb1').collection('foo');
      const coll2 = client.db('mydb2').collection('bar');

      // Important:: You must pass the session to the operations

      await coll1.insertOne({ abc: 1 }, { session });
      await coll2.insertOne({ xyz: 999 }, { session });
    }, {
      readPreference: 'primary',
      readConcern: { level: 'local' },
      writeConcern: { w: 'majority' }
    });
  } finally {
    await session.endSession();
    // await client.close();
  }

  /** 
   * Example 2: Core API
   * Requires explicit call to start the transaction and commit the transaction.
   * Does not incorporate error handling logic for TransientTransactionError and 
   * UnknownTransactionCommitResult, and instead provides the flexibility to incorporate 
   * custom error handling for these errors.
   */ 
  async function commitWithRetry(session) {
    try {
      await session.commitTransaction();
      console.log('Transaction committed.');
    } catch (error) {
      if (error.hasErrorLabel('UnknownTransactionCommitResult')) {
        console.log('UnknownTransactionCommitResult, retrying commit operation ...');
        await commitWithRetry(session);
      } else {
        console.log('Error during commit ...');
        throw error;
      }
    }
  }
  
  async function runTransactionWithRetry(txnFunc, client, session) {
    try {
      await txnFunc(client, session);
    } catch (error) {
      console.log('Transaction aborted. Caught exception during transaction.');
  
      // If transient error, retry the whole transaction
      if (error.hasErrorLabel('TransientTransactionError')) {
        console.log('TransientTransactionError, retrying transaction ...');
        await runTransactionWithRetry(txnFunc, client, session);
      } else {
        throw error;
      }
    }
  }
  
  async function updateEmployeeInfo(client, session) {
    session.startTransaction({
      readConcern: { level: 'snapshot' },
      writeConcern: { w: 'majority' },
      readPreference: 'primary'
    });
  
    const employeesCollection = client.db('hr').collection('employees');
    const eventsCollection = client.db('reporting').collection('events');
    await employeesCollection.updateOne(
      { employee: 3 },
      { $set: { status: 'Inactive' } },
      { session }
    );
    await eventsCollection.insertOne(
      {
        employee: 3,
        status: { new: 'Inactive', old: 'Active' }
      },
      { session }
    );
  
    try {
      await commitWithRetry(session);
    } catch (error) {
      await session.abortTransaction();
      throw error;
    }
  }
  
  return client.withSession(session =>
    runTransactionWithRetry(updateEmployeeInfo, client, session)
  ).then(() => client.close()).then(() => server.stop());
}

transactionExample().then();