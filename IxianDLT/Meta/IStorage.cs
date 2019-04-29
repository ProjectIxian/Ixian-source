﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace DLT
{
    namespace Meta
    {
        public abstract class IStorage
        {
            public string pathBase;
            // Threading
            private Thread thread = null;
            private bool running = false;
            private ThreadLiveCheck TLC;

            protected enum QueueStorageCode
            {
                insertTransaction,
                insertBlock,
                updateTxAppliedFlag

            }
            protected struct QueueStorageMessage
            {
                public QueueStorageCode code;
                public object data;
            }

            // Maintain a queue of sql statements
            protected readonly List<QueueStorageMessage> queueStatements = new List<QueueStorageMessage>();

            protected IStorage()
            {
                pathBase = Config.dataFolderBlocks;
            }


            public virtual bool prepareStorage()
            {
                if (!prepareStorageInternal())
                {
                    return false;
                }
                // Start thread
                TLC = new ThreadLiveCheck();
                running = true;
                thread = new Thread(new ThreadStart(threadLoop));
                thread.Name = "Storage_Thread";
                thread.Start();

                return true;
            }

            public virtual void stopStorage()
            {
                running = false;
            }
            protected virtual void threadLoop()
            {
                QueueStorageMessage active_message = new QueueStorageMessage();

                bool pending_statements = false;

                while (running || pending_statements == true)
                {
                    pending_statements = false;
                    TLC.Report();
                    try
                    {
                        bool message_found = false;

                        lock (queueStatements)
                        {
                            int statements_count = queueStatements.Count();
                            if (statements_count > 0)
                            {
                                if (statements_count > 1)
                                {
                                    pending_statements = true;
                                }
                                QueueStorageMessage candidate = queueStatements[0];
                                active_message = candidate;
                                queueStatements.Remove(candidate);
                                message_found = true;
                            }
                        }

                        if (message_found)
                        {
                            if (active_message.code == QueueStorageCode.insertTransaction)
                            {
                                insertTransactionInternal((Transaction)active_message.data);
                            }
                            else if (active_message.code == QueueStorageCode.insertBlock)
                            {
                                insertBlockInternal((Block)active_message.data);
                            }
                        }
                        else
                        {
                            // Sleep for 10ms to prevent cpu waste
                            Thread.Sleep(10);
                        }
                    }
                    catch (Exception e)
                    {
                        Logging.error("Exception occured in storage thread loop: " + e);
                    }
                    Thread.Yield();
                }
                cleanupCache();
            }

            public virtual bool redactBlockStorage(ulong removeBlocksBelow)
            {
                // Only redact on non-history nodes
                if (Config.storeFullHistory == true)
                {
                    return false;
                }

                ulong lowestBlock = getLowestBlockInStorage();
                for (ulong b = lowestBlock; b < removeBlocksBelow; b++)
                {
                    removeBlock(b, true);
                }
                return true;
            }
    

            public virtual int getQueuedQueryCount()
            {
                lock (queueStatements)
                {
                    return queueStatements.Count;
                }
            }

            public virtual void insertBlock(Block block)
            {
                // Make a copy of the block for the queue storage message processing
                QueueStorageMessage message = new QueueStorageMessage
                {
                    code = QueueStorageCode.insertBlock,
                    data = new Block(block)
                };

                lock (queueStatements)
                {
                    queueStatements.Add(message);
                }
            }


            public virtual void insertTransaction(Transaction transaction)
            {
                // Make a copy of the transaction for the queue storage message processing
                QueueStorageMessage message = new QueueStorageMessage
                {
                    code = QueueStorageCode.insertTransaction,
                    data = new Transaction(transaction)
                };

                lock (queueStatements)
                {

                    queueStatements.Add(message);
                }
            }

            // Used when on-disk storage must be upgraded
            public virtual bool needsUpgrade() { return false; }
            public virtual bool isUpgrading() { return false; }
            public virtual int upgradePercentage() { return 0; }
            public virtual ulong upgradeBlockNum() { return 0; }
            //
            // Insert
            protected abstract bool insertBlockInternal(Block block);
            protected abstract bool insertTransactionInternal(Transaction transaction);
            //
            public abstract ulong getLowestBlockInStorage();
            public abstract ulong getHighestBlockInStorage();
            // Get - Block
            /// <summary>
            /// Retrieves a Block by its block height from the underlying storage (database).
            /// </summary>
            /// <param name="blocknum">Block height of the block you wish to retrieve.</param>
            /// <returns>Null if the Block does not exist in storage.</returns>
            public abstract Block getBlock(ulong blocknum);
            /// <summary>
            /// Retrieves a Block by its blockChecksum from the underlying storage (database).
            /// </summary>
            /// <param name="checksum">Block Checksum of the Block you wish to retrieve.</param>
            /// <returns>Null if the Block with the specified hash does not exist in storage.</returns>
            public abstract Block getBlockByHash(byte[] checksum);
            /// <summary>
            /// Retrieves a SuperBlock which has the specified lastSuperblockChecksum from the underlying storage (database).
            /// </summary>
            /// <param name="checksum">Block checksum of the previous Superblock.</param>
            /// <returns>Null if the bloud could not be found in storage.</returns>
            public abstract Block getBlocksByLastSBHash(byte[] checksum);
            /// <summary>
            /// Retrieves all Blocks between two block heights, inclusive on both ends.
            /// Note: If `from` is larger than `to`, or both parameters are 0, an empty collection is returned.
            /// </summary>
            /// <param name="from">Minimum block height.</param>
            /// <param name="to">Maximum block height</param>
            /// <returns>IEnumerable with the resulting Blocks.</returns>
            public abstract IEnumerable<Block> getBlocksByRange(ulong from, ulong to);
            // Get - Transaction
            /// <summary>
            /// Retrieves a Transaction by its txid.
            /// </summary>
            /// <param name="txid">Transaction ID of the required Transaction.</param>
            /// <param name="block_num">Block height of the Block where the Transaction can be found. This parameter may be 0, in which case all storage will be searched.</param>
            /// <returns>Null if this transaction can't be found in storage.</returns>
            public abstract Transaction getTransaction(string txid, ulong block_num = 0);
            /// <summary>
            /// Retrieves all Transactions with the specified type from the given block range.
            /// Note: If `block_to` is 0, only Block `block_from` will be searched. If both parameters are 0, all Blocks will be searched.
            /// If both parameters are specified, the search is performed on the blocks [`block_from` - `block_to`] (inclusive).
            /// </summary>
            /// <param name="type">Transaction type to retrieve.</param>
            /// <param name="block_from">Starting block for the transaction search.</param>
            /// <param name="block_to">Ending block to search.</param>
            /// <returns>Collection of matching transactions.</returns>
            public abstract IEnumerable<Transaction> getTransactionsByType(Transaction.Type type, ulong block_from = 0, ulong block_to = 0);
            /// <summary>
            /// Retrieves all transactions which have the specified address in their `from` field.
            /// Note: If `block_to` is 0, only Block `block_from` will be searched. If both parameters are 0, all Blocks will be searched.
            /// If both parameters are specified, the search is performed on the blocks [`block_from` - `block_to`] (inclusive).
            /// </summary>
            /// <param name="from_addr">The wallet address you are searching.</param>
            /// <param name="block_from">Starting block for the transaction search.</param>
            /// <param name="block_to">Ending block to search.</param>
            /// <returns>Collection of matching Transactions.</returns>
            public abstract IEnumerable<Transaction> getTransactionsFromAddress(byte[] from_addr, ulong block_from = 0, ulong block_to = 0);
            /// <summary>
            /// Retrieves all transactions which have the specified address in their `to` field.
            /// Note: If `block_to` is 0, only Block `block_from` will be searched. If both parameters are 0, all Blocks will be searched.
            /// If both parameters are specified, the search is performed on the blocks [`block_from` - `block_to`] (inclusive).
            /// </summary>
            /// <param name="to_addr">The wallet address you are searching.</param>
            /// <param name="block_from">Starting block for the transaction search.</param>
            /// <param name="block_to">Ending block to search.</param>
            /// <returns>Collection of matching Transactions.</returns>
            public abstract IEnumerable<Transaction> getTransactionsToAddress(byte[] to_addr, ulong block_from = 0, ulong block_to = 0);
            /// <summary>
            /// Retrieves all Transactions from the specified block.
            /// </summary>
            /// <param name="block_num">Block from which to read Transactions.</param>
            /// <returns>Collection with matching Transactions.</returns>
            public abstract IEnumerable<Transaction> getTransactionsInBlock(ulong block_num);
            /// <summary>
            /// Retrieve all Transactions between the given timestamps (inclusive).
            /// Note: If `time_from` is larger than `time_to`, or both parameters are 0, an empty collection will be returned.
            /// The timestamp is in the format returned by `Core.getCurrentTimestamp()`.
            /// </summary>
            /// <param name="time_from">Starting timestamp for the search.</param>
            /// <param name="time_to">Ending timestamp for the search.</param>
            /// <returns>Collection of all Transactions between the specified timestamps.</returns>
            public abstract IEnumerable<Transaction> getTransactionsByTime(long time_from, long time_to);
            /// <summary>
            /// Retrieve all Transactions which were applied in the specified block range (inclusive).
            /// Note: if `block_from` is larger than `block_to`, or both parameters are 0, an empty collection will be returned.
            /// </summary>
            /// <param name="block_from">Starting block from which applied transactions will be returned.</param>
            /// <param name="block_to">Ending block until which applied transactions will be returned.</param>
            /// <returns>Collection of Transactions which match the criteria.</returns>
            public abstract IEnumerable<Transaction> getTransactionsApplied(ulong block_from, ulong block_to);
            //
            // Remove
            public abstract bool removeBlock(ulong block_num, bool remove_transactions);
            public abstract bool removeTransaction(string txid, ulong block_num = 0);
            //
            // Prepare and cleanup
            protected abstract bool prepareStorageInternal();
            protected abstract void shutdown();
            protected abstract void cleanupCache();
            public abstract void deleteData();


            // instantiation for the proper implementation class
            public static IStorage create(string name)
            {
                Logging.info("Block storage provider: {0}", name);
                switch(name)
                {
                    //case "SQLite": return new SQLiteStorage();
                    case "RocksDB": return new RocksDBStorage();
                    default: throw new Exception(String.Format("Unknown blocks storage provider: {0}", name));
                }
            }
        }
    }
}
