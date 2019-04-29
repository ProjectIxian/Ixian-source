﻿using DLT.Meta;
using DLT.Network;
using IXICore;
using IXICore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;

namespace DLT
{
    enum BlockVerifyStatus
    {
        Valid,
        Invalid,
        Indeterminate,
        IndeterminateFutureBlock,
        IndeterminatePastBlock,
        AlreadyProcessed,
        PotentiallyForkedBlock,
        IndeterminateVersionUpgradeBlock
    }
    class BlockProcessor
    {
        public bool operating { get; private set; }
        public ulong firstSplitOccurence { get; private set; }

        Block localNewBlock; // Block being worked on currently
        public readonly object localBlockLock = new object(); // used because localNewBlock can change while this lock should be held.
        DateTime lastBlockStartTime;
        DateTime currentBlockStartTime;

        long lastUpgradeTry = 0;

        int blockGenerationInterval = CoreConfig.blockGenerationInterval;

        public bool firstBlockAfterSync;

        private static string[] splitter = { "::" };

        private SortedList<ulong, long> fetchingTxForBlocks = new SortedList<ulong, long>();
        private SortedList<ulong, long> fetchingBulkTxForBlocks = new SortedList<ulong, long>();

        private Thread block_thread = null;

        public ulong highestNetworkBlockNum = 0;

        Dictionary<ulong, Dictionary<byte[], DateTime>> blockBlacklist = new Dictionary<ulong, Dictionary<byte[], DateTime>>();

        Dictionary<ulong, Block> pendingSuperBlocks = new Dictionary<ulong, Block>();

        public bool networkUpgraded = false;

        private ThreadLiveCheck TLC;

        public BlockProcessor()
        {
            lastBlockStartTime = DateTime.UtcNow;
            localNewBlock = null;
            operating = false;
            firstBlockAfterSync = false;
        }

        public void resumeOperation()
        {
            Logging.info("BlockProcessor resuming normal operation.");
            operating = true;

            lock (localBlockLock)
            {

                // Abort the thread if it's already created
                if (block_thread != null)
                    block_thread.Abort();

                TLC = new ThreadLiveCheck();
                // Start the thread
                block_thread = new Thread(onUpdate);
                block_thread.Name = "Block_Processor_Update_Thread";
                block_thread.Start();
            }
        }

        public void stopOperation()
        {
            operating = false;
            Logging.info("BlockProcessor stopped.");
        }

        // Check passed time since last block generation and if needed generate a new block
        public void onUpdate()
        {
            lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);

            while (operating)
            {
                TLC.Report();
                bool sleep = false;
                try
                {

                    // check if it is time to generate a new block
                    TimeSpan timeSinceLastBlock = DateTime.UtcNow - lastBlockStartTime;

                    if (timeSinceLastBlock.TotalSeconds < 0)
                    {
                        // edge case, system time apparently changed
                        lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                        timeSinceLastBlock = DateTime.UtcNow - lastBlockStartTime;
                        lock (blockBlacklist)
                        {
                            blockBlacklist.Clear();
                        }
                        // TODO TODO check if there's anything else that we should clear in such scenario - perhaps add a global handler for this edge case
                    }

                    int block_version = 3;

                    bool generateNextBlock = Node.forceNextBlock;
                    Random rnd = new Random();

                    lock (localBlockLock)
                    {
                        if (generateNextBlock)
                        {
                            localNewBlock = null;
                        }
                        else
                        {
                            if (localNewBlock == null)
                            {
                                if (timeSinceLastBlock.TotalSeconds > (blockGenerationInterval * 15) + rnd.Next(1000)) // no block for 15 block times + random seconds, we don't want all nodes sending at once
                                {
                                    generateNextBlock = true;
                                    block_version = Node.blockChain.getLastBlockVersion();
                                }
                                else
                                {
                                    if ((Node.isElectedToGenerateNextBlock(getElectedNodeOffset()) && timeSinceLastBlock.TotalSeconds >= blockGenerationInterval) || Node.blockChain.getLastBlockNum() < 10)
                                    {
                                        generateNextBlock = true;
                                    }
                                }
                            }

                            // if the node is stuck on the same block for too long, discard the block
                            if (localNewBlock != null && timeSinceLastBlock.TotalSeconds > (blockGenerationInterval * 20))
                            {
                                blacklistBlock(localNewBlock);
                                localNewBlock = null;
                                lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                                block_version = Node.blockChain.getLastBlockVersion();
                                generateNextBlock = true;
                            }
                        }


                        //Logging.info(String.Format("Waiting for {0} to generate the next block #{1}. offset {2}", Node.blockChain.getLastElectedNodePubKey(getElectedNodeOffset()), Node.blockChain.getLastBlockNum()+1, getElectedNodeOffset()));
                        if (generateNextBlock)
                        {
                            if (lastUpgradeTry > 0 && Clock.getTimestamp() - lastUpgradeTry < blockGenerationInterval * 120)
                            {
                                block_version = Node.blockChain.getLastBlockVersion();
                            }
                            else
                            {
                                lastUpgradeTry = 0;
                            }

                            if (Node.forceNextBlock)
                            {
                                Logging.info("Forcing new block generation");
                                Node.forceNextBlock = false;
                            }

                            generateNewBlock(block_version);
                        }
                        else
                        {
                            if (localNewBlock != null)
                            {
                                if (Node.isMasterNode())
                                {
                                    if (localNewBlock.signatures.Count() < Node.blockChain.getRequiredConsensus())
                                    {
                                        ProtocolMessage.broadcastNewBlock(localNewBlock);
                                        Logging.info(String.Format("Local block #{0} hasn't reached consensus yet {1}/{2}, resending.", localNewBlock.blockNum, localNewBlock.signatures.Count, Node.blockChain.getRequiredConsensus()));
                                        sleep = true;
                                    }
                                }
                                if (localNewBlock.version > Node.blockChain.getLastBlockVersion())
                                {
                                    lastUpgradeTry = Clock.getTimestamp();
                                }
                            }
                        }
                    }
                }catch(Exception e)
                {
                    Logging.error("Exception occured in blockProcessor onUpdate() {0}", e);
                }
                // Sleep until next iteration
                if (sleep)
                {
                    Thread.Sleep(2000);
                }
                else
                {
                    Thread.Sleep(1000);
                }
            }
            Thread.Yield();
            return;
        }

        // returns offset depending on time since last block and block generation interval. This function will return -1 if more than 10 block generation intervals have passed
        public int getElectedNodeOffset()
        {
            TimeSpan timeSinceLastBlock = DateTime.UtcNow - lastBlockStartTime;
            if(timeSinceLastBlock.TotalSeconds < 0)
            {
                return -1;
            }
            if(timeSinceLastBlock.TotalSeconds > blockGenerationInterval * 10) // edge case, if network is stuck for more than 10 blocks always return -1 as the node offset.
            {
                return -1;
            }
            return (int)(timeSinceLastBlock.TotalSeconds / (blockGenerationInterval*3));
        }

        public List<byte[][]> getSignaturesWithoutPlEntry(Block b)
        {
            List<byte[][]> sigs = new List<byte[][]>();

            for (int i = 0; i < b.signatures.Count; i++)
            {
                byte[][] sig = b.signatures[i];

                lock (PresenceList.presences)
                {
                    Presence p = null;
                    // Check if we have a public key instead of an address
                    if (sig[1].Length > 70)
                    {
                        p = PresenceList.presences.Find(x => x.pubkey.SequenceEqual(sig[1]));
                    }
                    else
                    {
                        p = PresenceList.presences.Find(x => x.wallet.SequenceEqual(sig[1]));
                    }

                    if(p != null)
                    {
                        bool masterEntryFound = false;
                        foreach(PresenceAddress pa in p.addresses)
                        {
                            if(pa.type == 'M' || pa.type == 'H')
                            {
                                masterEntryFound = true;
                                break;
                            }
                        }
                        if(!masterEntryFound)
                        {
                            p = null;
                        }
                    }

                    //Logging.info(String.Format("Searching for {0}", parts[1]));                 
                    if (p == null)
                    {
                        sigs.Add(sig);
                        continue;
                    }
                }
            }
            return sigs;
        }

        public bool removeSignaturesWithoutPlEntry(Block b)
        {
            List<byte[][]> sigs = getSignaturesWithoutPlEntry(b);
            for (int i = 0; i < sigs.Count; i++)
            {
                b.signatures.Remove(sigs[i]);
            }
            if (sigs.Count > 0)
            {
                return true;
            }
            return false;
        }

        public List<byte[][]> getSignaturesWithLowBalance(Block b)
        {
            List<byte[][]> sigs = new List<byte[][]>();

            for (int i = 0; i < b.signatures.Count; i++)
            {
                byte[] address = b.signatures[i][1];
                // Check if we have a public key instead of an address
                if (address.Length > 70)
                {
                    address = (new Address(address)).address;
                }

                if (Node.walletState.getWalletBalance(address) < CoreConfig.minimumMasterNodeFunds)
                {
                    Logging.error(String.Format("LOW BALANCE ADDRESS: {0} FUNDS: {1}", Base58Check.Base58CheckEncoding.EncodePlain(address), Node.walletState.getWalletBalance(address)));
                    sigs.Add(b.signatures[i]);
                    continue;
                }
                
            }
            return sigs;
        }

        public bool removeSignaturesWithLowBalance(Block b)
        {
            List<byte[][]> sigs = getSignaturesWithLowBalance(b);
            for (int i = 0; i < sigs.Count; i++)
            {
                b.signatures.Remove(sigs[i]);
            }
            if (sigs.Count > 0)
            {
                return true;
            }
            return false;
        }

        // Checks if the block has been sigFreezed and if all the hashes match, returns false if the block shouldn't be processed further
        public bool handleSigFreezedBlock(Block b, RemoteEndpoint endpoint = null, RemoteEndpoint skipEndpoint = null)
        {
            Block sigFreezingBlock = Node.blockChain.getBlock(b.blockNum + 5);
            byte[] sigFreezeChecksum = null;
            lock (localBlockLock)
            {
                if (sigFreezingBlock == null && localNewBlock != null && localNewBlock.blockNum == b.blockNum + 5)
                {
                    sigFreezingBlock = localNewBlock;
                }
                if (sigFreezingBlock != null)
                {
                    sigFreezeChecksum = sigFreezingBlock.signatureFreezeChecksum;
                    // this block already has a sigfreeze, don't tamper with the signatures
                    Block targetBlock = Node.blockChain.getBlock(b.blockNum);
                    if (targetBlock != null && sigFreezeChecksum.SequenceEqual(targetBlock.calculateSignatureChecksum()))
                    {
                        // we already have the correct block
                        if (!b.calculateSignatureChecksum().SequenceEqual(sigFreezeChecksum))
                        {
                            // we already have the correct block but the sender does not, broadcast our block
                            ProtocolMessage.broadcastNewBlock(targetBlock, null, endpoint);
                        }
                        acceptLocalNewBlock();
                        return false;
                    }
                    if (sigFreezeChecksum.SequenceEqual(b.calculateSignatureChecksum()))
                    {
                        Logging.warn(String.Format("Received block #{0} ({1}) which was sigFreezed with correct checksum, force updating signatures locally!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                        if (b.getUniqueSignatureCount() >= Node.blockChain.getRequiredConsensus(b.blockNum))
                        {
                            // this is likely the correct block, update and broadcast to others
                            Node.blockChain.refreshSignatures(b, true);
                            //ProtocolMessage.broadcastNewBlock(targetBlock, skipEndpoint);
                            if (sigFreezingBlock == localNewBlock)
                            {
                                acceptLocalNewBlock();
                            }
                        }
                        else
                        {
                            Logging.warn("Target block " + b.blockNum + " does not have the required consensus.");
                            // the block is invalid, we should disconnect, most likely a malformed block - somebody removed signatures
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.blockInvalidNoConsensus, "Block #" + b.blockNum + " is invalid", b.blockNum.ToString());
                            localNewBlock = null;
                        }
                        return false;
                    }
                    else
                    {
                        Logging.warn(String.Format("Received block #{0} ({1}) which was sigFreezed but is different than our own block (probably forked), re-requesting the block from the network!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                        ProtocolMessage.broadcastGetBlock(b.blockNum, skipEndpoint, endpoint);
                        return false;
                    }
                }
            }
            return true;
        }

        public bool verifySigFreezedBlock(Block b)
        {
            Block sigFreezingBlock = Node.blockChain.getBlock(b.blockNum + 5);
            byte[] sigFreezeChecksum = null;
            lock (localBlockLock)
            {
                if (sigFreezingBlock == null && localNewBlock != null && localNewBlock.blockNum == b.blockNum + 5)
                {
                    sigFreezingBlock = localNewBlock;
                }
                if (sigFreezingBlock != null)
                {
                    sigFreezeChecksum = sigFreezingBlock.signatureFreezeChecksum;
                    if (sigFreezeChecksum.SequenceEqual(b.calculateSignatureChecksum()))
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        public Block getPendingSuperBlock(byte[] block_checksum)
        {
            return pendingSuperBlocks.Where(x => x.Value.blockChecksum.SequenceEqual(block_checksum)).First().Value;
        }

        private bool onSuperBlockReceived(Block b, RemoteEndpoint endpoint = null)
        {
            if(b.version < 4) // super blocks were supported with block v4
            {
                return true;
            }

            var local_block_list = pendingSuperBlocks.Where(x => x.Value.blockChecksum.SequenceEqual(b.blockChecksum));
            if (local_block_list.Count() > 0)
            {
                b = local_block_list.First().Value;
            }

            if (b.lastSuperBlockChecksum == null)
            {
                // block is not a superblock
                if(b.blockNum % CoreConfig.superblockInterval == 0)
                {
                    // block was supposed to be a superblock
                    Logging.warn("Received a normal block {0}, which was supposed to be a super block.", b.blockNum);
                    return false;
                }
                return true;
            }

            // block is a superblock
            if (b.blockNum % CoreConfig.superblockInterval != 0)
            {
                // block was not supposed to be a superblock
                Logging.warn("Received a super block {0}, which was supposed to be a normal block.", b.blockNum);
                return false;
            }

            Block last_super_block = Node.blockChain.getSuperBlock(b.lastSuperBlockNum);
            if(last_super_block != null)
            {
                if (!last_super_block.blockChecksum.SequenceEqual(b.lastSuperBlockChecksum))
                {
                    Logging.warn("Received a forked super block {0}.", b.blockNum);
                    return false;
                }else if(last_super_block.lastSuperBlockChecksum == null && last_super_block.blockNum > 1)
                {
                    Logging.warn("Received a forked superblock that points to a last superblock, which isn't a superblock {0}.", b.blockNum);
                    return false;
                }
            }
            else
            {
                byte[] last_accepted_super_block_checksum = Node.blockChain.getLastSuperBlockChecksum();
                if (getPendingSuperBlock(last_accepted_super_block_checksum) == null)
                {
                    Logging.info("Received a future super block {0}.", b.blockNum);
                    ProtocolMessage.broadcastGetNextSuperBlock(Node.blockChain.getLastSuperBlockNum(), last_accepted_super_block_checksum, 0, null, null);
                }
                return false;
            }


            return true;
        }

        public void onBlockReceived(Block b, RemoteEndpoint endpoint = null)
        {
            if (operating == false) return;
            //Logging.info(String.Format("Received block #{0} {1} ({2} sigs) from the network.", b.blockNum, Crypto.hashToString(b.blockChecksum), b.getUniqueSignatureCount()));

            if(isBlockBlacklisted(b))
            {
                return;
            }

            if (!onSuperBlockReceived(b, endpoint))
            {
                return;
            }

            // if historic block, only the sigs should be updated if not older than 5 blocks in history
            if (b.blockNum <= Node.blockChain.getLastBlockNum())
            {
                if (b.blockNum + 5 > Node.blockChain.getLastBlockNum())
                {
                    Logging.info(String.Format("Already processed block #{0}, doing basic verification and collecting only sigs if relevant!", b.blockNum));
                    Block localBlock = Node.blockChain.getBlock(b.blockNum);
                    lock (localBlock)
                    {
                        BlockVerifyStatus block_status = verifyBlockBasic(b, true, endpoint);
                        if (b.blockChecksum.SequenceEqual(localBlock.blockChecksum) && block_status == BlockVerifyStatus.Valid)
                        {
                            if (handleSigFreezedBlock(b, endpoint))
                            {
                                if (b.blockNum + 4 > Node.blockChain.getLastBlockNum())
                                {
                                    Block block_to_update = Node.blockChain.getBlock(b.blockNum);
                                    if (!block_to_update.calculateSignatureChecksum().SequenceEqual(b.calculateSignatureChecksum()))
                                    {
                                        removeSignaturesWithoutPlEntry(b);
                                        if (Node.blockChain.refreshSignatures(b))
                                        {
                                            // if refreshSignatures returns true, it means that new signatures were added. re-broadcast to make sure the entire network gets this change.
                                            ProtocolMessage.broadcastNewBlock(block_to_update);
                                        }
                                    }
                                }
                            }
                        }
                        else if(!b.blockChecksum.SequenceEqual(localBlock.blockChecksum) && block_status == BlockVerifyStatus.Valid)
                        {
                            // the block is valid but block checksum is different, meaning lastBlockChecksum passes, check sig count, if it passes, it's forked, if not, resend our block
                            if (b.getUniqueSignatureCount() < Node.blockChain.getRequiredConsensus(b.blockNum))
                            {
                                ProtocolMessage.broadcastNewBlock(localBlock, null, endpoint);
                            }
                            else
                            {
                                // the block is invalid, we should disconnect the node as it is likely on a forked network
                                CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.blockInvalidForked, "Block #" + b.blockNum + " is invalid, you are possibly on a forked network", b.blockNum.ToString());
                            }
                        }
                        else if(block_status == BlockVerifyStatus.Invalid || block_status == BlockVerifyStatus.PotentiallyForkedBlock)
                        {
                            Logging.info("Block is invalid");
                            // the block is invalid, we should disconnect the node as it is likely on a forked network
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.blockInvalidForked, "Block #" + b.blockNum + " is invalid, you are possibly on a forked network", b.blockNum.ToString());
                        }
                    }
                }else // b.blockNum < Node.blockChain.getLastBlockNum() - 5
                {
                    BlockVerifyStatus past_block_status = verifyBlock(b, true, null);
                    if(past_block_status == BlockVerifyStatus.AlreadyProcessed || past_block_status == BlockVerifyStatus.Valid)
                    {
                        // likely the node is missing sigs or has his very own custom block, let's send our block and also send the latest block, since he's obviously falling behind
                        Block block = Node.blockChain.getBlock(b.blockNum);
                        if (!b.Equals(block))
                        {
                            ProtocolMessage.broadcastNewBlock(block, null, endpoint);
                            ProtocolMessage.broadcastNewBlock(Node.blockChain.getBlock(Node.getLastBlockHeight()), null, endpoint);
                        }
                    }
                    else if(past_block_status == BlockVerifyStatus.IndeterminatePastBlock)
                    {
                        // the node seems to be way behind, send the current last block
                        ProtocolMessage.broadcastNewBlock(Node.blockChain.getBlock(Node.getLastBlockHeight()), null, endpoint);
                    }else if(past_block_status == BlockVerifyStatus.PotentiallyForkedBlock)
                    {
                        Block localBlock = Node.blockChain.getBlock(b.blockNum);
                        if (localBlock != null && b.lastBlockChecksum.SequenceEqual(localBlock.lastBlockChecksum) && b.getUniqueSignatureCount() < Node.blockChain.getRequiredConsensus(b.blockNum))
                        {
                            ProtocolMessage.broadcastNewBlock(localBlock, null, endpoint);
                        }else
                        {
                            // the block is different than our own, we should disconnect the node as it is likely on a forked network
                            CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.blockInvalidChecksum, "Block #"+b.blockNum+", has a different checksum, you are possibly on a forked network", b.blockNum.ToString());
                        }
                    }
                    else
                    {
                        // the block is invalid, we should disconnect the node as it is likely on a forked network
                        CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.blockInvalidForked, "Block #" + b.blockNum + " is invalid, you are possibly on a forked network", b.blockNum.ToString());
                    }
                }
                return;
            }

            b.powField = null;

            BlockVerifyStatus b_status;

            lock (localBlockLock)
            {
                if (localNewBlock != null && localNewBlock.blockChecksum.SequenceEqual(b.blockChecksum))
                {
                    b_status = verifyBlockBasic(b, true, endpoint);
                }else
                {
                    b_status = verifyBlock(b, false, endpoint);
                }
            }

            if(b_status == BlockVerifyStatus.Invalid)
            {
                // the block is invalid, we should disconnect the node as it is likely on a forked network
                CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.blockInvalidForked, "Block #" + b.blockNum + " is invalid, you are possibly on a forked network", b.blockNum.ToString());
                return;
            }else if (b_status != BlockVerifyStatus.Valid)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which was not valid!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                // TODO: Blacklisting point
                return;
            }


            // remove signatures without PL entry but not if we're catching up with the network
            if (b.blockNum > Node.getHighestKnownNetworkBlockHeight() - 5 && removeSignaturesWithoutPlEntry(b))
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that wasn't found in the PL!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                // TODO: Blacklisting point
            }
            if (b.signatures.Count == 0)
            {
                Logging.warn(String.Format("Received block #{0} ({1}) which has no valid signatures!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                // TODO: Blacklisting point
                return;
            }

            // TODOBLOCK
            lock (localBlockLock)
            {
                if (b.blockNum > Node.blockChain.getLastBlockNum())
                {
                    onBlockReceived_currentBlock(b, endpoint);
                }
            }
        }

        public BlockVerifyStatus verifyBlockBasic(Block b, bool verify_sig = true, RemoteEndpoint endpoint = null)
        {
            if(b.version > Block.maxVersion)
            {
                Logging.error("Received block {0} with a version higher than this node can handle, discarding the block.", b.blockNum);
                if (b.getUniqueSignatureCount() >= Node.blockChain.getRequiredConsensus())
                {
                    networkUpgraded = true;
                }
                return BlockVerifyStatus.IndeterminateVersionUpgradeBlock;
            }

            if(b.version < Node.getLastBlockVersion())
            {
                return BlockVerifyStatus.PotentiallyForkedBlock;
            }

            // first check if lastBlockChecksum and previous block's checksum match, so we can quickly discard an invalid block (possibly from a fork)
            Block prevBlock = Node.blockChain.getBlock(b.blockNum - 1);

            if (prevBlock != null && !b.lastBlockChecksum.SequenceEqual(prevBlock.blockChecksum)) // block found but checksum doesn't match
            {
                Logging.warn(String.Format("Received block #{0} with invalid lastBlockChecksum!", b.blockNum));
                return BlockVerifyStatus.Invalid;
            }

            if (Node.blockChain.Count > 0 && b.blockNum + 5 <= Node.blockChain.getLastBlockNum())
            {
                Block tmpBlock = Node.blockChain.getBlock(b.blockNum);
                if (tmpBlock == null)
                {
                    Logging.info("Received an indeterminate past block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.IndeterminatePastBlock;
                }
                else if (tmpBlock.blockChecksum.SequenceEqual(b.blockChecksum))
                {
                    Logging.info("Already processed block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.AlreadyProcessed;
                }
                Logging.warn("Received a potentially forked block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                return BlockVerifyStatus.PotentiallyForkedBlock;
            }

            // Verify checksums
            byte[] checksum = b.calculateChecksum();
            if (!b.blockChecksum.SequenceEqual(checksum))
            {
                Logging.warn(String.Format("Block verification failed for #{0}. Checksum is {1}, but should be {2}.",
                    b.blockNum, Crypto.hashToString(b.blockChecksum), Crypto.hashToString(checksum)));
                return BlockVerifyStatus.Invalid;
            }

            if (verify_sig)
            {
                bool skip_sig_verification = false;
                if(pendingSuperBlocks.Count() > 0 && pendingSuperBlocks.OrderBy(x=> x.Key).Last().Key > b.blockNum)
                {
                    skip_sig_verification = true;
                }
                // Verify signatures
                if (!b.verifySignatures(skip_sig_verification))
                {
                    Logging.warn(String.Format("Block #{0} failed while verifying signatures. There are invalid signatures on the block.", b.blockNum));
                    return BlockVerifyStatus.Invalid;
                }
            }

            ulong lastBlockNum = Node.getLastBlockHeight();

            if (prevBlock == null && lastBlockNum > 1) // block not found but blockChain is not empty, request the missing blocks
            {
                if (removeSignaturesWithLowBalance(b))
                {
                    Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that had too low balance!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                }
                if (!Node.blockSync.synchronizing)
                {
                    // Don't request block 0
                    if (b.blockNum - 1 > 0 && highestNetworkBlockNum < b.blockNum)
                    {
                        if (removeSignaturesWithoutPlEntry(b))
                        {
                            Logging.warn(String.Format("Received block #{0} ({1}) which had a signature that wasn't found in the PL!", b.blockNum, Crypto.hashToString(b.blockChecksum)));
                        }
                        // blocknum is higher than the network's, switching to catch-up mode, but only if half of required consensus is reached on the block
                        if (b.blockNum > lastBlockNum + 1 && b.getUniqueSignatureCount() >= (Node.blockChain.getRequiredConsensus() / 2)) // if at least 2 blocks behind
                        {
                            highestNetworkBlockNum = b.blockNum;
                            if (b.lastSuperBlockChecksum != null && !generateSuperBlockTransactions(b, endpoint))
                            {
                                pendingSuperBlocks.AddOrReplace(b.blockNum, b);
                            }
                        }
                    }
                }
                if (b.blockNum > lastBlockNum + 1)
                {
                    if (!Node.blockSync.synchronizing)
                    {
                        ProtocolMessage.broadcastGetBlock(lastBlockNum + 1, null, null);
                    }
                    Logging.info("Received an indeterminate future block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.IndeterminateFutureBlock;
                }else if(b.blockNum <= lastBlockNum - CoreConfig.getRedactedWindowSize())
                {
                    Logging.info("Received an indeterminate past block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.IndeterminatePastBlock;
                }
            }

            // Verify sigfreeze
            if (b.blockNum <= lastBlockNum)
            {
                if (!verifySignatureFreezeChecksum(b, endpoint))
                {
                    return BlockVerifyStatus.Indeterminate;
                }
            }

            // verify difficulty
            if (lastBlockNum + 1 == b.blockNum)
            {
                if(Node.getLastBlockHeight() - (ulong)Node.blockChain.Count == 0 || Node.blockChain.Count >= (long)CoreConfig.getRedactedWindowSize())
                {
                    //Logging.info("Verifying difficulty for #" + b.blockNum);
                    ulong expectedDifficulty = calculateDifficulty(b.version);
                    if (b.difficulty != expectedDifficulty)
                    {
                        Logging.warn(String.Format("Received block #{0} ({1}) which had a difficulty {2}, expected difficulty: {3}", b.blockNum, Crypto.hashToString(b.blockChecksum), b.difficulty, expectedDifficulty));
                        return BlockVerifyStatus.Invalid;
                    }
                }
            }

            // TODO: blacklisting would happen here - whoever sent us an invalid block is problematic
            //  Note: This will need a change in the Network code to tag incoming blocks with sender info.
            return BlockVerifyStatus.Valid;
        }

        public BlockVerifyStatus verifyBlockTransactions(Block b, bool ignore_walletstate = false, RemoteEndpoint endpoint = null)
        {
            // Check all transactions in the block against our TXpool, make sure all is legal
            // Note: it is possible we don't have all the required TXs in our TXpool - in this case, request the missing ones and return Indeterminate
            bool hasAllTransactions = true;
            bool fetchTransactions = false;
            lock (fetchingTxForBlocks)
            {
                if (fetchingTxForBlocks.ContainsKey(b.blockNum))
                {
                    long tx_timeout = fetchingTxForBlocks[b.blockNum];
                    long cur_time = Clock.getTimestamp();
                    if (cur_time - tx_timeout > 10)
                    {
                        fetchingTxForBlocks[b.blockNum] = cur_time;
                        Logging.info("fetchingTxTimeout EXPIRED");
                        fetchTransactions = true;
                    }
                }
            }
            int txCount = 0;
            int missing = 0;

            Dictionary<byte[], IxiNumber> minusBalances = new Dictionary<byte[], IxiNumber>(new ByteArrayComparer());
            foreach (string txid in b.transactions)
            {
                // Skip fetching staking txids if we're not synchronizing
                if (txid.StartsWith("stk"))
                {
                    if (Node.blockSync.synchronizing == false
                        || (Node.blockSync.synchronizing == true && Config.recoverFromFile)
                        || (Node.blockSync.synchronizing == true && b.blockNum > Node.blockSync.wsSyncConfirmedBlockNum)
                        || (Node.blockSync.synchronizing == true && Config.fullStorageDataVerification == true))
                        continue;
                }

                Transaction t = TransactionPool.getTransaction(txid, b.blockNum);
                if (t == null)
                {
                    if (fetchTransactions)
                    {
                        Logging.info(String.Format("Missing transaction '{0}'. Requesting.", txid));
                        ProtocolMessage.broadcastGetTransaction(txid, b.blockNum, endpoint);
                        hasAllTransactions = false;
                        missing++;
                    }
                    else
                    {
                        hasAllTransactions = false;
                        missing++;
                    }
                    continue;
                }

                // lock transaction v1 with block v2
                if (b.version < 2)
                {
                    if (t.version > 1)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version was at most 1!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }
                else if (b.version == 2)
                {
                    if (t.version < 1 || t.version > 2)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version is 1 or 2!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }
                else if (b.version > 2)
                {
                    if (t.version < 2 || t.version > 3)
                    {
                        Logging.error("Block includes a tx version {{ {0} }} but expected tx version is 2 or 3!", t.version);
                        return BlockVerifyStatus.Invalid;
                    }
                }

                foreach (var entry in t.fromList)
                {
                    byte[] address = (new Address(t.pubKey, entry.Key)).address;
                    // TODO TODO TODO TODO plus balances should also be added (and be processed first) to prevent overspending false alarms
                    if (!minusBalances.ContainsKey(address))
                    {
                        minusBalances.Add(address, 0);
                    }

                    try
                    {
                        // TODO: check to see if other transaction types need additional verification
                        if (t.type != (int)Transaction.Type.Genesis
                            && t.type != (int)Transaction.Type.PoWSolution
                            && t.type != (int)Transaction.Type.StakingReward)
                        {
                            txCount++;
                            IxiNumber new_minus_balance = minusBalances[address] + entry.Value;
                            minusBalances[address] = new_minus_balance;
                        }
                    }
                    catch (OverflowException)
                    {
                        // someone is doing something bad with this transaction, so we invalidate the block
                        // TODO: Blacklisting for the transaction originator node
                        Logging.error(String.Format("Overflow caused by transaction {0}: amount: {1} from: {2}",
                            t.id, t.amount, Base58Check.Base58CheckEncoding.EncodePlain(address)));
                        return BlockVerifyStatus.Invalid;
                    }
                }
            }

            // Pass #2 verifications for multisigs after all transactions have been received
            if(hasAllTransactions)
            {
                foreach (string txid in b.transactions)
                {
                    Transaction t = TransactionPool.getTransaction(txid, b.blockNum);
                    if(t == null)
                    {
                        continue;
                    }
                    if (t.type == (int)Transaction.Type.MultisigTX || t.type == (int)Transaction.Type.ChangeMultisigWallet || t.type == (int)Transaction.Type.MultisigAddTxSignature)
                    {
                        object multisig_data = t.GetMultisigData();
                        string orig_txid = "";
                        if (multisig_data is Transaction.MultisigTxData)
                        {
                            orig_txid = ((Transaction.MultisigTxData)multisig_data).origTXId;
                        }
                        if (orig_txid == "")
                        {
                            orig_txid = t.id;
                        }
                        byte[] address = (new Address(t.pubKey, t.fromList.Keys.First())).address;
                        Wallet from_w = Node.walletState.getWallet(address);
                        int num_valid_multisigs = TransactionPool.getNumRelatedMultisigTransactions(orig_txid, b) + 1;
                        if (num_valid_multisigs < from_w.requiredSigs)
                        {
                            Logging.error(String.Format("Block includes a multisig transaction {{ {0} }} which does not have enough signatures to be processed! (Signatures: {1}, Required: {2}",
                                t.id, num_valid_multisigs, from_w.requiredSigs));
                            return BlockVerifyStatus.Invalid;
                        }
                    }
                }
            }

            if ((ulong)txCount > CoreConfig.maximumTransactionsPerBlock + 10)
            {
                Logging.warn(String.Format("Block has more transactions than the maximumTransactionsPerBlock setting {0}/{1}", txCount, CoreConfig.maximumTransactionsPerBlock + 10));
                return BlockVerifyStatus.Invalid;
            }
            //
            if (!hasAllTransactions)
            {
                lock (fetchingTxForBlocks)
                {
                    if (!fetchingBulkTxForBlocks.ContainsKey(b.blockNum))
                    {
                        long cur_time = Clock.getTimestamp();
                        if (missing > b.transactions.Count / 2)
                        {
                            cur_time = cur_time - 30;
                            fetchingBulkTxForBlocks.Add(b.blockNum, cur_time);
                            fetchingTxForBlocks.Add(b.blockNum, cur_time);
                            BlockVerifyStatus status = verifyBlockTransactions(b, ignore_walletstate, endpoint);
                            return status;
                        }
                        else
                        {
                            fetchingBulkTxForBlocks.Add(b.blockNum, cur_time);
                            fetchingTxForBlocks.Add(b.blockNum, cur_time);
                            byte includeTransactions = 2;
                            if (Node.blockSync.synchronizing == false
                                || (Node.blockSync.synchronizing == true && Config.recoverFromFile)
                                || (Node.blockSync.synchronizing == true && Config.storeFullHistory)
                                || (Node.blockSync.synchronizing == true && Config.fullStorageDataVerification == true))
                            {
                                includeTransactions = 1;
                            }
                            ProtocolMessage.broadcastGetBlock(b.blockNum, null, endpoint, includeTransactions);
                        }
                        Logging.info(String.Format("Block #{0} is missing {1} transactions, which have been requested from the network.", b.blockNum, missing));
                    }
                    if(fetchTransactions)
                    {
                        ProtocolMessage.broadcastGetBlock(b.blockNum, null, endpoint, 0);
                    }
                }
                Logging.info("Waiting for missing transactions for Block #{0}.", b.blockNum);
                return BlockVerifyStatus.Indeterminate;
            }
            lock (fetchingTxForBlocks)
            {
                fetchingBulkTxForBlocks.Remove(b.blockNum);
                fetchingTxForBlocks.Remove(b.blockNum);
            }

            if(ignore_walletstate == false)
            {
                // overspending
                foreach (byte[] addr in minusBalances.Keys)
                {
                    IxiNumber initial_balance = Node.walletState.getWalletBalance(addr);
                    if (initial_balance < minusBalances[addr])
                    {
                        Logging.error(String.Format("Address {0} is attempting to overspend: Balance: {1}, Total Outgoing: {2}.",
                            Base58Check.Base58CheckEncoding.EncodePlain(addr), initial_balance, minusBalances[addr]));
                        return BlockVerifyStatus.Invalid;
                    }
                }
            }

            return BlockVerifyStatus.Valid;
        }

        public BlockVerifyStatus verifyBlock(Block b, bool ignore_walletstate = false, RemoteEndpoint endpoint = null)
        {
            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            BlockVerifyStatus basicVerification = verifyBlockBasic(b, true, endpoint);

            if (basicVerification != BlockVerifyStatus.Valid)
            {
                return basicVerification;
            }

            if (Node.blockChain.Count > 0 && b.blockNum <= Node.blockChain.getLastBlockNum())
            {
                Block tmpBlock = Node.blockChain.getBlock(b.blockNum);
                if (tmpBlock == null)
                {
                    Logging.info("Received an indeterminate past block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.IndeterminatePastBlock;
                }
                else if (tmpBlock.blockChecksum.SequenceEqual(b.blockChecksum))
                {
                    Logging.info("Already processed block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                    return BlockVerifyStatus.AlreadyProcessed;
                }
                Logging.warn("Received a potentially forked block {0} ({1})", b.blockNum, Crypto.hashToString(b.blockChecksum));
                return BlockVerifyStatus.PotentiallyForkedBlock;
            }

            BlockVerifyStatus txVerification = verifyBlockTransactions(b, ignore_walletstate, endpoint);
            if (txVerification != BlockVerifyStatus.Valid)
            {
                return txVerification;
            }

            // Note: This part depends on no one else messing with WS while it runs.
            // Sometimes generateNewBlock is called from the other thread and this is invoked by network while
            // the generate thread is paused, so we need to lock
            // Note: This function is also called from BlockSync, which uses it to determine if the blocks it is syncing
            // from neighbors are OK.  However, BlockSync applies blocks before the current WS, so sometimes it doesn't
            // want to check WS checksums
            byte[] ws_checksum = null;
            if (ignore_walletstate == false)
            {

                lock (localBlockLock)
                {
                    // ignore wallet state check if it isn't the current block
                    if (b.blockNum < Node.blockChain.getLastBlockNum())
                    {
                        Logging.info(String.Format("Not verifying wallet state for old block {0}", b.blockNum));
                    }
                    else if (b.blockNum == Node.blockChain.getLastBlockNum())
                    {
                        ws_checksum = Node.walletState.calculateWalletStateChecksum(b.version);
                        // this should always be the same anyway, but just in case
                        if (!b.walletStateChecksum.SequenceEqual(ws_checksum))
                        {
                            Logging.error(String.Format("Incorrect current wallet state checksum for the last block #{0} Block's WS checksum: {1}, actual WS checksum: {2}", b.blockNum, Crypto.hashToString(b.walletStateChecksum), Crypto.hashToString(ws_checksum)));
                            return BlockVerifyStatus.Invalid;
                        }
                    }
                    else
                    {
                        Node.walletState.snapshot();
                        if (applyAcceptedBlock(b, true))
                        {
                            ws_checksum = Node.walletState.calculateWalletStateChecksum(b.version, true);
                        }
                        Node.walletState.revert();
                        if (ws_checksum == null || !ws_checksum.SequenceEqual(b.walletStateChecksum))
                        {
                            Logging.error(String.Format("Block #{0} failed while verifying transactions: Invalid wallet state checksum! Block's WS checksum: {1}, actual WS checksum: {2}", b.blockNum, Crypto.hashToString(b.walletStateChecksum), Crypto.hashToString(ws_checksum)));
                            return BlockVerifyStatus.Invalid;
                        }
                    }
                }
            }


            sw.Stop();
            TimeSpan elapsed = sw.Elapsed;
            //Logging.info(string.Format("VerifyBlock duration: {0}ms", elapsed.TotalMilliseconds));


            // TODO: blacklisting would happen here - whoever sent us an invalid block is problematic
            //  Note: This will need a change in the Network code to tag incoming blocks with sender info.
            return BlockVerifyStatus.Valid;
        }

        private void onBlockReceived_currentBlock(Block b, RemoteEndpoint endpoint)
        {
            if (b.blockNum != Node.blockChain.getLastBlockNum() + 1)
            {
                Logging.warn(String.Format("Received block #{0}, but next block should be #{1}.", b.blockNum, Node.blockChain.getLastBlockNum() + 1));
                return;
            }
            if(b.version < Node.blockChain.getLastBlockVersion())
            {
                Logging.warn(String.Format("A current block with a smaller version was received than the last block in the blockchain, rejecting block #{0} with version {1}.", b.blockNum, b.version));
                // TODO: keep a counter - if this happens too often, disconnect the node
                // TODO TODO TODO TODO: disconnect?
                return;
            }
            lock (localBlockLock)
            {
                if (localNewBlock != null)
                {
                    if(localNewBlock.blockChecksum.SequenceEqual(b.blockChecksum))
                    {
                        Logging.info(String.Format("Block #{0} ({1} sigs) received from the network is the block we are currently working on. Merging signatures  ({2} sigs).", b.blockNum, b.signatures.Count(), localNewBlock.signatures.Count()));
                        if(localNewBlock.addSignaturesFrom(b))
                        {
                            currentBlockStartTime = DateTime.UtcNow;
                            lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                            //if (!Node.isMasterNode())
                            //    return;
                            // if addSignaturesFrom returns true, that means signatures were increased, so we re-transmit
                            Logging.info(String.Format("Block #{0}: Number of signatures increased, re-transmitting. (total signatures: {1}).", b.blockNum, localNewBlock.getUniqueSignatureCount()));
                            //ProtocolMessage.broadcastNewBlock(localNewBlock);
                            acceptLocalNewBlock();
                        }
                        else if(localNewBlock.signatures.Count != b.signatures.Count)
                        {
                            if (!Node.isMasterNode())
                                return;
                            Logging.info(String.Format("Block #{0}: Received block has less signatures, re-transmitting local block. (total signatures: {1}).", b.blockNum, localNewBlock.getUniqueSignatureCount()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock, null, endpoint);
                        }
                    }
                    else
                    {
                        int blockSigCount = b.getUniqueSignatureCount();
                        int localBlockSigCount = localNewBlock.getUniqueSignatureCount();
                        if(blockSigCount > localBlockSigCount && b.blockNum == localNewBlock.blockNum)
                        {
                            Logging.info(String.Format("Incoming block #{0} has more signatures and is the same block height, accepting instead of our own. (total signatures: {1}, election offset: {2})", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                            localNewBlock = b;
                            currentBlockStartTime = DateTime.UtcNow;
                            lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                            acceptLocalNewBlock();
                        }
                        else
                        {
                            if (!Node.isMasterNode())
                                return;
                            // discard with a warning, likely spam, resend our local block
                            Logging.info(String.Format("Incoming block #{0} is different than our own and doesn't have more sigs, discarding and re-transmitting local block. (total signatures: {1}), election offset: {2}.", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                            ProtocolMessage.broadcastNewBlock(localNewBlock, null, endpoint);
                        }
                    }
                }
                else // localNewBlock == null
                {
                    bool hasNodeSig = true;
                    if(getElectedNodeOffset() != -1 && Node.getLastBlockHeight() + 2 > Node.getHighestKnownNetworkBlockHeight())
                    {
                        hasNodeSig = b.hasNodeSignature(Node.blockChain.getLastElectedNodePubKey(getElectedNodeOffset()));
                    }
                    if (hasNodeSig
                        || b.getUniqueSignatureCount() >= Node.blockChain.getRequiredConsensus()/2 // TODO TODO TODO think about /2 thing
                        || firstBlockAfterSync)
                    {
                        localNewBlock = b;
                        currentBlockStartTime = DateTime.UtcNow;
                        firstBlockAfterSync = false;
                        acceptLocalNewBlock();
                    }
                    else
                    {
                        Logging.warn(String.Format("Incoming block #{0} doesn't have elected node's sig, waiting for a new block. (total signatures: {1}), election offset: {2}.", b.blockNum, b.signatures.Count, getElectedNodeOffset()));
                    }
                }
            }
        }

        // Adds a block to the blacklist
        private void blacklistBlock(Block b)
        {
            lock (blockBlacklist)
            {
                Dictionary<byte[], DateTime> blacklistedBlocks = null;
                if (blockBlacklist.ContainsKey(b.blockNum))
                {
                    blacklistedBlocks = blockBlacklist[b.blockNum];
                }
                else
                {
                    blacklistedBlocks = new Dictionary<byte[], DateTime>(new ByteArrayComparer());
                }
                blacklistedBlocks.AddOrReplace(b.blockChecksum, DateTime.UtcNow);
                blockBlacklist.AddOrReplace(b.blockNum, blacklistedBlocks);
            }
        }

        // Returns true if block is blacklisted
        private bool isBlockBlacklisted(Block b)
        {
            lock (blockBlacklist)
            {
                if (blockBlacklist.ContainsKey(b.blockNum))
                {
                    Dictionary<byte[], DateTime> bbl = blockBlacklist[b.blockNum];
                    if (bbl.ContainsKey(b.blockChecksum))
                    {
                        DateTime dt = bbl[b.blockChecksum];
                        if ((DateTime.UtcNow - dt).TotalSeconds > blockGenerationInterval * 10)
                        {
                            blockBlacklist[b.blockNum].Remove(b.blockChecksum);
                            if (blockBlacklist[b.blockNum].Count() == 0)
                            {
                                blockBlacklist.Remove(b.blockNum);
                            }
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        // Removes blocks with older block height from the blacklist
        private void cleanupBlockBlacklist()
        {
            ulong blockNum = Node.blockChain.getLastBlockNum();
            lock (blockBlacklist)
            {
                Dictionary<ulong, Dictionary<byte[], DateTime>> tmpList = new Dictionary<ulong, Dictionary<byte[], DateTime>>(blockBlacklist);
                foreach (var i in tmpList)
                {
                    if (i.Key <= blockNum)
                    {
                        blockBlacklist.Remove(i.Key);
                    }
                }
            }
        }

        public bool acceptLocalNewBlock()
        {
            bool block_accepted = false;
            bool requestBlockAgain = false;
            ulong requestBlockNum = 0;

            var sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            lock (localBlockLock)
            {
                if (localNewBlock == null) return false;

                if (!verifySignatureFreezeChecksum(localNewBlock, null))
                {
                    Logging.warn(String.Format("Signature freeze checksum verification failed on current localNewBlock #{0}, waiting for the correct target block.", localNewBlock.blockNum));
                    TimeSpan current_block_processing_time = DateTime.UtcNow - currentBlockStartTime;
                    Random rnd = new Random();
                    if (current_block_processing_time.TotalSeconds > (blockGenerationInterval * 2) + rnd.Next(30)) // can't get target block for 2 block times + random seconds, we don't want all nodes sending at once
                    {
                        blacklistBlock(localNewBlock);
                        localNewBlock = null;
                    }
                    return false;
                }else
                {
                    if (highestNetworkBlockNum < localNewBlock.blockNum + 4)
                    {
                        if (Node.isMasterNode())
                        {
                            byte[][] signature_data = localNewBlock.applySignature(); // applySignature() will return signature_data, if signature was applied and null, if signature was already present from before
                            if (signature_data != null) 
                            {
                                //ProtocolMessage.broadcastNewBlock(localNewBlock);
                                ProtocolMessage.broadcastNewBlockSignature(localNewBlock.blockNum, localNewBlock.blockChecksum, signature_data[0], signature_data[1]);
                            }
                        }
                    }
                }
                // TODO: we will need an edge case here in the event that too many nodes dropped and consensus
                // can no longer be reached according to this number - I don't have a clean answer yet - MZ
                if (localNewBlock.signatures.Count() >= Node.blockChain.getRequiredConsensus())
                {
                    if (verifyBlock(localNewBlock) != BlockVerifyStatus.Valid)
                    {
                        if (Node.blockChain.getBlock(localNewBlock.blockNum) == null)
                        {
                            Logging.error(String.Format("We have an invalid block #{0} in verifyBlockAcceptance, requesting the block again.", localNewBlock.blockNum));
                            requestBlockNum = localNewBlock.blockNum;
                            localNewBlock = null;
                            requestBlockAgain = true;
                        }
                        else
                        {
                            localNewBlock = null;
                        }
                    }

                    if (localNewBlock.blockNum != Node.blockChain.getLastBlockNum() + 1)
                    {
                        Logging.warn(String.Format("Tried to apply an unexpected block #{0}, expected #{1}. Stack trace: {2}", localNewBlock.blockNum, Node.blockChain.getLastBlockNum() + 1, Environment.StackTrace));
                        // block has already been applied or ahead, waiting for new blocks
                        localNewBlock = null;
                        return false;
                    }
                    // accept this block, apply its transactions, recalc consensus, etc
                    if (applyAcceptedBlock(localNewBlock) == true)
                    {
                        byte[] wsChecksum = Node.walletState.calculateWalletStateChecksum(localNewBlock.version);
                        if (!wsChecksum.SequenceEqual(localNewBlock.walletStateChecksum))
                        {
                            Logging.error(String.Format("After applying block #{0}, walletStateChecksum is incorrect, rolling back transactions!. Block's WS: {1}, actualy WS: {2}", localNewBlock.blockNum,
                                Crypto.hashToString(localNewBlock.walletStateChecksum), Crypto.hashToString(wsChecksum)));
                            Logging.error(String.Format("Node reports block version: {0}", Node.getLastBlockVersion()));
                            rollBackAcceptedBlock(localNewBlock);
                            if (!Node.walletState.calculateWalletStateChecksum(localNewBlock.version).SequenceEqual(Node.blockChain.getBlock(Node.blockChain.getLastBlockNum()).walletStateChecksum))
                            {
                                Logging.error(String.Format("Fatal error occured while rolling back accepted block #{0}!.", localNewBlock.blockNum));
                                // TODO TODO TODO maybe do something else instead?
                                operating = false;
                                Node.stop();
                                return false;
                            }
                            localNewBlock.logBlockDetails();
                            requestBlockNum = localNewBlock.blockNum;
                            localNewBlock = null;
                            requestBlockAgain = true;
                        }
                        else
                        {
                            // append current block
                            Node.blockChain.appendBlock(localNewBlock);

                            pendingSuperBlocks.Remove(localNewBlock.blockNum);

                            if (localNewBlock.blockNum > 5)
                            {
                                // append sigfreezed block
                                Block tmp_block = Node.blockChain.getBlock(localNewBlock.blockNum - 5);
                                if (tmp_block != null)
                                {
                                    Node.blockChain.updateBlock(tmp_block);
                                }
                            }

                            block_accepted = true;

                            if (Node.miner.searchMode == BlockSearchMode.latestBlock)
                            {
                                Node.miner.forceSearchForBlock();
                            }

                            Logging.info(String.Format("Accepted block #{0}.", localNewBlock.blockNum));
                            lastBlockStartTime = DateTime.UtcNow;
                            localNewBlock.logBlockDetails();

                            // Reset transaction limits
                            //TransactionPool.resetSocketTransactionLimits();

                            if (highestNetworkBlockNum > Node.blockChain.getLastBlockNum())
                            {
                                ProtocolMessage.broadcastGetBlock(Node.blockChain.getLastBlockNum() + 1, null, null, 1);
                            }else
                            {
                                highestNetworkBlockNum = 0;
                            }

                            CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H', 'W' }, ProtocolMessageCode.newBlock, localNewBlock.getBytes(), BitConverter.GetBytes(localNewBlock.blockNum));
                            localNewBlock = null;

                            if (Node.miner.searchMode != BlockSearchMode.latestBlock)
                            {
                                Node.miner.checkActiveBlockSolved();
                            }

                            cleanupBlockBlacklist();
                            if (Node.blockChain.getLastBlockNum() % Config.saveWalletStateEveryBlock == 0)
                            {
                                WalletStateStorage.saveWalletState(Node.blockChain.getLastBlockNum());
                            }
                        }
                    }
                    else if(Node.blockChain.getBlock(localNewBlock.blockNum) == null)
                    {
                        // TODO TODO TODO Partial rollback may be needed here
                        Logging.error(String.Format("Couldn't apply accepted block #{0}.", localNewBlock.blockNum));
                        localNewBlock.logBlockDetails();
                        requestBlockNum = localNewBlock.blockNum;
                        localNewBlock = null;
                        requestBlockAgain = true;
                    }else
                    {
                        localNewBlock = null;
                    }
                }
            }

            sw.Stop();
            TimeSpan elapsed = sw.Elapsed;
            Logging.info(string.Format("VerifyBlockAcceptance took: {0}ms", elapsed.TotalMilliseconds));



            // Check if we should request the block again
            if (requestBlockAgain && requestBlockNum > 0)
            {
                // Show a notification
                Logging.error(string.Format("Requesting block {0} again due to previous mismatch.", requestBlockNum));
                // Request the block again
                ProtocolMessage.broadcastGetBlock(requestBlockNum);
            }

            return block_accepted;
        }

        public bool verifySignatureFreezeChecksum(Block b, RemoteEndpoint endpoint)
        {
            if(Node.blockChain.Count <= 5)
            {
                return true;
            }
            if (b.signatureFreezeChecksum != null)
            {
                Block targetBlock = Node.blockChain.getBlock(b.blockNum - 5);
                if (targetBlock == null)
                {
                    // this shouldn't be possible
                    ProtocolMessage.broadcastGetBlock(b.blockNum - 5, null, endpoint);
                    Logging.error(String.Format("Block verification can't be done since we are missing sigfreeze checksum target block {0}.", b.blockNum - 5));
                    return false;
                }
                byte[] sigFreezeChecksum = targetBlock.calculateSignatureChecksum();
                if (!b.signatureFreezeChecksum.SequenceEqual(sigFreezeChecksum))
                {
                    Logging.warn(String.Format("Block sigFreeze verification failed for #{0}. Checksum is {1}, but should be {2}. Requesting block #{3}",
                        b.blockNum, Crypto.hashToString(b.signatureFreezeChecksum), Crypto.hashToString(sigFreezeChecksum), b.blockNum - 5));
                    ProtocolMessage.broadcastGetBlock(b.blockNum - 5, null, endpoint);
                    return false;
                }
            }
            else if (b.blockNum > 7)
            {
                // this shouldn't be possible
                Block targetBlock = Node.blockChain.getBlock(b.blockNum - 5);
                Logging.error(String.Format("Block sigFreeze verification failed for #{0}. Checksum is empty but should be {1}. Requesting block #{2}",
                    b.blockNum, Crypto.hashToString(targetBlock.calculateSignatureChecksum()), b.blockNum - 5));
                ProtocolMessage.broadcastGetBlock(b.blockNum, endpoint);
                return false;
            }

            return true;
        }

        // Applies the block
        // Returns false if walletstate is not correct
        public bool applyAcceptedBlock(Block b, bool ws_snapshot = false)
        {
            if (Node.blockChain.getBlock(b.blockNum) != null)
            {
                Logging.warn(String.Format("Block #{0} has already been applied. Stack trace: {1}", b.blockNum, Environment.StackTrace));
                return false;
            }

            // Distribute staking rewards first
            distributeStakingRewards(b, b.version, ws_snapshot);

            // Apply transactions from block
            if (!TransactionPool.applyTransactionsFromBlock(b, ws_snapshot))
            {
                return false;
            }

            // Apply transaction fees
            applyTransactionFeeRewards(b, ws_snapshot);

            // Update wallet state public keys
            updateWalletStatePublicKeys(b.blockNum, ws_snapshot);

            // Broadcast blockheight only if the node is synchronized
            if (!Node.blockSync.synchronizing)
            {
                ProtocolMessage.broadcastBlockHeight(b.blockNum);
            }

            return true;
        }

        public bool rollBackAcceptedBlock(Block b, bool ws_snapshot = false)
        {
            return false; // TODO TODO TODO partially implemented

            /*for(int i = 0; i < b.transactions.Count; i++)
            {
                Transaction t = TransactionPool.getTransaction(b.transactions[i]);
                if(t == null)
                {
                    return false;
                }

                if (t.applied == b.blockNum)
                {
                    TransactionPool.rollBackNormalTransaction(t);
                    TransactionPool.rollBackPoWTransaction(t);
                    TransactionPool.rollBackStakingTransaction(t);
                    TransactionPool.rollBackTransactionFeeReward(t);
                    t.applied = 0;
                } // else the tx was either not applied - could be failed, or applied to some previous block
            }
            return true; */
        }

        public void applyTransactionFeeRewards(Block b, bool ws_snapshot = false)
        {
            byte[] sigfreezechecksum = null;
            lock (localBlockLock)
            {
                // Should never happen
                if (b == null)
                {
                    Logging.warn("Applying fee rewards: block is null.");
                    return;
                }
                if (b.blockNum > 1)
                {
                    sigfreezechecksum = Node.blockChain.getBlock(b.blockNum - 1).signatureFreezeChecksum;
                }
            }

            // Ignore blocks before #6
            if (b.blockNum < 6)
            {
                return;
            }

            if (sigfreezechecksum == null)
            {
                Logging.warn("Current block does not have sigfreeze checksum.");
                return;
            }

            // Obtain the 6th last block, aka target block
            Block targetBlock = null;

            targetBlock = Node.blockChain.getBlock(b.blockNum - 6);
            if (targetBlock == null)
                return;

            byte[] targetSigFreezeChecksum = targetBlock.calculateSignatureChecksum();

            if (sigfreezechecksum.SequenceEqual(targetSigFreezeChecksum) == false)
            {
                Logging.warn(string.Format("Signature freeze mismatch for block {0}. Current block height: {1}", targetBlock.blockNum, b.blockNum));
                // TODO: fetch the block again or re-sync
                return;
            }

            // Calculate the total transactions amount and number of transactions in the target block
            IxiNumber tAmount = 0;
            IxiNumber tFeeAmount = 0;

            ulong txcount = 0;
            foreach(string txid in targetBlock.transactions)
            {
                Transaction tx = TransactionPool.getTransaction(txid, b.blockNum);               
                if (tx != null)
                {
                    if (tx.type == (int)Transaction.Type.Normal)
                    {
                        tAmount += tx.amount;
                        tFeeAmount += tx.fee;
                        txcount++;
                    } else if (tx.type == (int)Transaction.Type.MultisigTX)
                    {
                        Transaction.MultisigTxData ms_data = (Transaction.MultisigTxData)tx.GetMultisigData();
                        if (ms_data.origTXId == "")
                        {
                            tAmount += tx.amount;
                        }
                        tFeeAmount += tx.fee;
                        txcount++;
                    }
                    else if (tx.type == (int)Transaction.Type.ChangeMultisigWallet || tx.type == (int)Transaction.Type.MultisigAddTxSignature)
                    {
                        tFeeAmount += tx.fee;
                        txcount++;
                    }
                }
            }

            // Check if there are any transactions processed in the target block
            if(txcount < 1)
            { 
                return;
            }

            // Check the amount
            if(tFeeAmount == (long) 0)
            {
                return;
            }

            // Calculate the total fee amount
            IxiNumber foundationAward = tFeeAmount * CoreConfig.foundationFeePercent / 100;

            // Award foundation fee
            Wallet foundation_wallet = Node.walletState.getWallet(CoreConfig.foundationAddress, ws_snapshot);
            IxiNumber foundation_balance_before = foundation_wallet.balance;
            IxiNumber foundation_balance_after = foundation_balance_before + foundationAward;
            Node.walletState.setWalletBalance(CoreConfig.foundationAddress, foundation_balance_after, ws_snapshot);
            //Logging.info(string.Format("Awarded {0} IXI to foundation", foundationAward.ToString()));

            // Subtract the foundation award from total fee amount
            tFeeAmount = tFeeAmount - foundationAward;

            ulong numSigs = (ulong)targetBlock.signatures.Count();
            if(numSigs < 1)
            {
                // Something is not right, there are no signers on this block
                Logging.error("Transaction fee: no signatures on block!");
                return;
            }

            // Calculate the award per signer
            IxiNumber sigs = new IxiNumber(numSigs);

            IxiNumber tAward = IxiNumber.divRem(tFeeAmount, sigs, out IxiNumber remainder);

            // Division of fee amount and sigs left a remainder, distribute that to the foundation wallet
            if (remainder > (long) 0)
            {
                foundation_balance_after = foundation_balance_after + remainder;
                Node.walletState.setWalletBalance(CoreConfig.foundationAddress, foundation_balance_after, ws_snapshot);
                //Logging.info(string.Format("Awarded {0} IXI to foundation from fee division remainder", foundationAward.ToString()));
            }

            // Go through each signature in the block
            foreach (byte[][] sig in targetBlock.signatures)
            {
                // Generate the corresponding Ixian address
                byte[] addressBytes =  (new Address(sig[1])).address;

                // Update the walletstate and deposit the award
                Wallet signer_wallet = Node.walletState.getWallet(addressBytes, ws_snapshot);
                IxiNumber balance_before = signer_wallet.balance;
                IxiNumber balance_after = balance_before + tAward;
                Node.walletState.setWalletBalance(addressBytes, balance_after, ws_snapshot);
                if(!ws_snapshot)
                {
                    if (signer_wallet.id.SequenceEqual(Node.walletStorage.getPrimaryAddress()))
                    {
                        SortedDictionary<byte[], IxiNumber> to_list = new SortedDictionary<byte[], IxiNumber>(new ByteArrayComparer());
                        to_list.Add(addressBytes, balance_after);
                        string address = Base58Check.Base58CheckEncoding.EncodePlain(Node.walletStorage.getPrimaryAddress());
                        Activity activity = new Activity(Node.walletStorage.getSeedHash(), address, Base58Check.Base58CheckEncoding.EncodePlain(CoreConfig.ixianInfiniMineAddress), to_list, (int)ActivityType.TxFeeReward, Encoding.UTF8.GetBytes("TXFEEREWARD-" + b.blockNum + "-" + address), tAward.ToString(), b.timestamp, (int)ActivityStatus.Final, b.blockNum);
                        ActivityStorage.insertActivity(activity);
                    }
                }
                //Logging.info(string.Format("Awarded {0} IXI to {1}", tAward.ToString(), addr.ToString()));
            }

            // Output stats for this block's fee distribution
            Logging.info(string.Format("Total block TX amount: {0} Total TXs: {1} Reward per Signer: {2} Foundation Reward: {3}", tAmount.ToString(), txcount, 
                tAward.ToString(), foundationAward.ToString()));
          
        }

        // returns false if this is a multisig transaction and not enough signatures - in this case, it should not be added to the block
        // returns true for all other transaction types
        private ulong includeMultisigTransactions(Transaction transaction, Dictionary<byte[], IxiNumber> minusBalances)
        {
            // NOTE: this function is called exclusively from generateNewBlock(), so we do not need to lock anything - 'localNewBlock' is alredy locked.
            // If this is called from anywhere else, add a lock here!
            // multisig transactions must be complete before they are added
            object multisig_data = transaction.GetMultisigData();
            string orig_txid = transaction.id;
            byte[] address = (new Address(transaction.pubKey, transaction.fromList.Keys.First())).address;
            Wallet from_w = Node.walletState.getWallet(address);
            List<string> related_tx_ids = TransactionPool.getRelatedMultisigTransactions(orig_txid, null);
            int num_valid_multisigs = related_tx_ids.Count() + 1;
            if (num_valid_multisigs >= from_w.requiredSigs)
            {
                localNewBlock.addTransaction(orig_txid);
                IxiNumber total_amount = transaction.amount + transaction.fee;
                foreach (string txid in related_tx_ids)
                {
                    Transaction tx = TransactionPool.getTransaction(txid);
                    if(!verifyFromListBalance(tx, minusBalances))
                    {
                        minusBalances[address] -= total_amount;
                        return 0;
                    }
                    total_amount += tx.amount + tx.fee;
                    localNewBlock.addTransaction(txid);
                }
                // include the multisig transaction
                return (ulong)related_tx_ids.Count() + 1;
            } else
            {
                // skip the multisig transaction
                return 0;
            }
        }

        public bool verifyFromListBalance(Transaction transaction, Dictionary<byte[], IxiNumber> minusBalances)
        {
            foreach (var entry in transaction.fromList)
            {
                byte[] address = (new Address(transaction.pubKey, entry.Key)).address;
                // TODO TODO TODO TODO plus balances should also be added (and be processed first) to prevent overspending false alarms
                if (!minusBalances.ContainsKey(address))
                {
                    minusBalances.Add(address, 0);
                }

                // prevent overspending
                if (transaction.type != (int)Transaction.Type.Genesis
                    && transaction.type != (int)Transaction.Type.PoWSolution
                    && transaction.type != (int)Transaction.Type.StakingReward)
                {
                    IxiNumber new_minus_balance = minusBalances[address] + entry.Value;
                    IxiNumber from_balance = Node.walletState.getWalletBalance(address);

                    if (from_balance < new_minus_balance)
                    {
                        // TODO TODO TODO TODO TODO, it might not be the best idea to remove overspent transaction here as the block isn't confirmed yet,
                        // we should do this after the block has been confirmed
                        TransactionPool.removeTransaction(transaction.id);
                        return false;
                    }
                    minusBalances[address] = new_minus_balance;
                }

            }
            return true;
        }

        private void generateNewBlockTransactions(int block_version)
        {
            ulong total_transactions = 1;
            IxiNumber total_amount = 0;

            List<Transaction> pool_transactions = TransactionPool.getUnappliedTransactions().ToList<Transaction>();
            pool_transactions.Sort((x, y) => x.blockHeight.CompareTo(y.blockHeight)); // TODO add fee/weight

            ulong normal_transactions = 0; // Keep a counter of normal transactions for the limiter

            Dictionary<byte[], IxiNumber> minusBalances = new Dictionary<byte[], IxiNumber>(new ByteArrayComparer());

            Dictionary<ulong, List<object[]>> blockSolutionsDictionary = new Dictionary<ulong, List<object[]>>();

            foreach (var transaction in pool_transactions)
            {
                // Check if we reached the transaction limit for this block
                if (normal_transactions >= CoreConfig.maximumTransactionsPerBlock)
                {
                    // Limit all other transactions
                    break;
                }

                // lock transaction v2 with block v3
                if (block_version >= 3 && transaction.version < 2)
                {
                    if (Node.blockChain.getLastBlockVersion() >= 3)
                    {
                        TransactionPool.removeTransaction(transaction.id);
                    }
                    continue;
                }

                // Verify that the transaction is actually valid at this point
                // no need as the tx is already in the pool and was verified when received
                //if (TransactionPool.verifyTransaction(transaction) == false)
                //    continue;

                // Skip adding staking rewards
                if (transaction.type == (int)Transaction.Type.StakingReward)
                {
                    TransactionPool.removeTransaction(transaction.id);
                    continue;
                }

                ulong minBh = 0;
                if (localNewBlock.blockNum > CoreConfig.getRedactedWindowSize(localNewBlock.version))
                {
                    minBh = localNewBlock.blockNum - CoreConfig.getRedactedWindowSize(localNewBlock.version);
                }
                // Check the block height
                if (minBh > transaction.blockHeight || transaction.blockHeight > localNewBlock.blockNum)
                {
                    TransactionPool.removeTransaction(transaction.id);
                    continue;
                }

                // Special case for PoWSolution transactions
                if (transaction.type == (int)Transaction.Type.PoWSolution)
                {
                    // TODO: pre-validate the transaction in such a way it doesn't affect performance
                    ulong powBlockNum = 0;
                    string nonce = "";
                    if (!TransactionPool.verifyPoWTransaction(transaction, out powBlockNum, out nonce, block_version))
                    {
                        TransactionPool.removeTransaction(transaction.id);
                        continue;
                    }
                    else
                    {
                        // Check if we already have a key matching the block number
                        if (blockSolutionsDictionary.ContainsKey(powBlockNum) == false)
                        {
                            blockSolutionsDictionary[powBlockNum] = new List<object[]>();
                        }
                        if (block_version >= 2)
                        {
                            byte[] tmp_address = (new Address(transaction.pubKey)).address;
                            if (!blockSolutionsDictionary[powBlockNum].Exists(x => ((byte[])x[0]).SequenceEqual(tmp_address) && (string)x[1] == nonce))
                            {
                                // Add the miner to the block number dictionary reward list
                                blockSolutionsDictionary[powBlockNum].Add(new object[3] { tmp_address, nonce, transaction });
                            }
                            else
                            {
                                TransactionPool.removeTransaction(transaction.id);
                                continue;
                            }
                        }
                    }
                }

                if (!verifyFromListBalance(transaction, minusBalances))
                {
                    continue;
                }

                IxiNumber total_tx_amount = transaction.amount + transaction.fee;

                if (transaction.type == (int)Transaction.Type.MultisigTX || transaction.type == (int)Transaction.Type.ChangeMultisigWallet)
                {
                    if (normal_transactions > 1500)
                    {
                        continue;
                    }
                    ulong ms_transactions = includeMultisigTransactions(transaction, minusBalances);
                    if (ms_transactions == 0)
                    {
                        continue;
                    }
                    total_transactions += ms_transactions;
                    normal_transactions += ms_transactions;
                }
                else if (transaction.type != (int)Transaction.Type.MultisigAddTxSignature)
                {
                    localNewBlock.addTransaction(transaction.id);
                    total_transactions++;
                    normal_transactions++;
                }

                total_amount += total_tx_amount;
            }


            Logging.info(String.Format("\t\t|- Transactions: {0} \t\t Amount: {1}", total_transactions, total_amount));
        }

        public bool generateSuperBlockTransactions(Block super_block, RemoteEndpoint endpoint = null)
        {
            ulong cur_block_height = super_block.blockNum;
            for (ulong i = cur_block_height - 1; i > 0; i--)
            {
                Block b = Node.blockChain.getBlock(i, true);
                if (b == null)
                {
                    Logging.error("Unable to find block {0} while creating superblock {1}.", i, super_block.blockNum);
                    ProtocolMessage.broadcastGetBlock(i, endpoint);
                    return false;
                }

                if(b.version > 3 && b.lastSuperBlockChecksum != null)
                {
                    super_block.lastSuperBlockNum = b.blockNum;
                    super_block.lastSuperBlockChecksum = b.blockChecksum;
                    break;
                }

                if (b.signatureFreezeChecksum != null && i > 5)
                {
                    Block target_block = Node.blockChain.getBlock(i - 5, true);
                    if (target_block == null)
                    {
                        Logging.error("Unable to find target block {0} while creating superblock {1}.", i - 5, super_block.blockNum);
                        ProtocolMessage.broadcastGetBlock(i - 5, endpoint);
                        return false;
                    }else if(!target_block.calculateSignatureChecksum().SequenceEqual(b.signatureFreezeChecksum))
                    {
                        Logging.error("Target block's {0} signatures don't match sigfreeze, while creating superblock {1}.", i - 5, super_block.blockNum);
                        ProtocolMessage.broadcastGetBlockSignatures(target_block.blockNum, target_block.blockChecksum, endpoint);
                        return false;
                    }
                }


                SuperBlockSegment seg = new SuperBlockSegment(b.blockNum, b.blockChecksum);

                super_block.superBlockSegments.Add(b.blockNum, seg);

            }

            return true;
        }

        // Generate a new block
        public void generateNewBlock(int block_version)
        {
            if (!Node.isMasterNode())
            {
                return;
            }

            lock (localBlockLock)
            {
                Logging.info("GENERATING NEW BLOCK");

                // Create a new block and add all the transactions in the pool
                localNewBlock = new Block();
                localNewBlock.timestamp = Core.getCurrentTimestamp();
                localNewBlock.blockNum = Node.blockChain.getLastBlockNum() + 1;

                localNewBlock.version = block_version;

                Logging.info(String.Format("\t\t|- Block Number: {0}", localNewBlock.blockNum));

                // Apply staking transactions to block. 
                List<Transaction> staking_transactions = generateStakingTransactions(localNewBlock.blockNum - 6, block_version, false, localNewBlock.timestamp);
                foreach (Transaction transaction in staking_transactions)
                {
                    localNewBlock.addTransaction(transaction.id);
                }
                staking_transactions.Clear();
                
                // Apply signature freeze
                localNewBlock.signatureFreezeChecksum = getSignatureFreeze();

                if (localNewBlock.version > 3 && localNewBlock.blockNum % CoreConfig.superblockInterval == 0)
                {
                    // superblock

                    // collect all txids up to last superblock (or genesis block if no superblock yet exists)
                    if(!generateSuperBlockTransactions(localNewBlock))
                    {
                        Logging.error("Error generating transactions for superblock {0}.", localNewBlock.blockNum);
                        localNewBlock = null;
                        return;
                    }

                    if (localNewBlock.lastSuperBlockChecksum == null)
                    {
                        Block b = Node.blockChain.getBlock(1);
                        if(b == null)
                        {
                            Logging.error("Unable to find genesis block for superblock {0}.", localNewBlock.blockNum);
                            localNewBlock = null;
                            return;
                        }
                        localNewBlock.lastSuperBlockNum = b.blockNum;
                        localNewBlock.lastSuperBlockChecksum = b.blockChecksum;
                    }
                }else
                {
                    generateNewBlockTransactions(block_version);
                }

                // Calculate mining difficulty
                localNewBlock.difficulty = calculateDifficulty(block_version);

                // Simulate applying a block to see what the walletstate would look like
                Node.walletState.snapshot();
                if(!applyAcceptedBlock(localNewBlock, true))
                {
                    Logging.error("Unable to apply a snapshot of a newly generated block {0}.", localNewBlock.blockNum);
                    localNewBlock = null;
                    Node.walletState.revert();
                    return;
                }
                localNewBlock.setWalletStateChecksum(Node.walletState.calculateWalletStateChecksum(localNewBlock.version, true));
                Logging.info(String.Format("While generating new block: WS Checksum: {0}", Crypto.hashToString(localNewBlock.walletStateChecksum)));
                Logging.info(String.Format("While generating new block: Node's blockversion: {0}", Node.getLastBlockVersion()));
                Node.walletState.revert();

                localNewBlock.lastBlockChecksum = Node.blockChain.getLastBlockChecksum();
                localNewBlock.blockChecksum = localNewBlock.calculateChecksum();
                localNewBlock.applySignature();

                localNewBlock.logBlockDetails();

                currentBlockStartTime = DateTime.UtcNow;

                // Broadcast the new block
                ProtocolMessage.broadcastNewBlock(localNewBlock);

                if(verifyBlock(localNewBlock) != BlockVerifyStatus.Valid)
                {
                    Logging.error("Error occured verifying the newly generated block {0}.", localNewBlock.blockNum);
                    localNewBlock = null;
                    return;
                }

                if (localNewBlock.blockNum < 8)
                {
                    acceptLocalNewBlock();
                }
            }
        }

        public static ulong calculateDifficulty(int version)
        {
            if (version == 0)
            {
                return calculateDifficulty_v0();
            }
            else if (version == 1)
            {
                return calculateDifficulty_v1();
            }else if(version == 2)
            {
                return calculateDifficulty_v2();
            }
            else // >= 3
            {
                return calculateDifficulty_v3();
            }
        }

        // Calculate the current mining difficulty
        public static ulong calculateDifficulty_v0()
        {
            ulong current_difficulty = 14;
            if (Node.blockChain.getLastBlockNum() > 1)
            {
                Block previous_block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
                if (previous_block != null)
                    current_difficulty = previous_block.difficulty;

                // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
                ulong solved_blocks = Node.blockChain.getSolvedBlocksCount(CoreConfig.getRedactedWindowSize(0));
                ulong window_size = CoreConfig.getRedactedWindowSize(0);

                // Special consideration for early blocks
                if (Node.blockChain.getLastBlockNum() < window_size)
                {
                    window_size = Node.blockChain.getLastBlockNum();
                }

                if (solved_blocks > window_size / 2)
                {
                    current_difficulty++;
                }
                else
                {
                    current_difficulty--;
                }

                // Set some limits
                if (current_difficulty > 256)
                    current_difficulty = 256;
                else if (current_difficulty < 14)
                    current_difficulty = 14;

            }

            return current_difficulty;
        }

        // Calculate the current mining difficulty
        public static ulong calculateDifficulty_v1()
        {
            ulong current_difficulty = 0xA2CB1211629F6141; // starting difficulty (requires approx 180 Khashes to find a solution)
            if (Node.blockChain.getLastBlockNum() > 1)
            {
                Block previous_block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
                if (previous_block != null)
                    current_difficulty = previous_block.difficulty;

                // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
                ulong solved_blocks = Node.blockChain.getSolvedBlocksCount(CoreConfig.getRedactedWindowSize(1));
                ulong window_size = CoreConfig.getRedactedWindowSize(1);

                // Special consideration for early blocks
                if (Node.blockChain.getLastBlockNum() < window_size)
                {
                    window_size = Node.blockChain.getLastBlockNum();
                }
                // 
                BigInteger target_hashes_per_block = Miner.getTargetHashcountPerBlock(current_difficulty);
                BigInteger actual_hashes_per_block = target_hashes_per_block * solved_blocks / (window_size / 2);
                ulong target_difficulty = 0;
                if (actual_hashes_per_block != 0)
                {
                    // find an appropriate difficulty for actual hashes:
                    target_difficulty = Miner.calculateTargetDifficulty(actual_hashes_per_block);
                }
                // we jump hafway to the target difficulty each time
                ulong next_difficulty = 0;
                if (target_difficulty > current_difficulty)
                {
                    next_difficulty = current_difficulty + (target_difficulty - current_difficulty) / 2;
                }
                else if (target_difficulty < current_difficulty)
                {
                    next_difficulty = current_difficulty - (current_difficulty - target_difficulty) / 2;
                }
                else
                {
                    //difficulties are equal
                    next_difficulty = current_difficulty;
                }
                // TODO: maybe pretty-fy the hashrate (ie: 15 MH/s, rather than 15000000 H/s) also could prettify the difficulty number
                Logging.info(String.Format("Estimated network hash rate is {0} H/s (previous was: {1} H/s). Difficulty adjusts from {2} -> {3}.",
                    (actual_hashes_per_block / 60).ToString(),
                    (target_hashes_per_block / 60).ToString(),
                    current_difficulty, next_difficulty));
                current_difficulty = next_difficulty;
            }

            return current_difficulty;
        }

        public static ulong calculateDifficulty_v2()
        {
            ulong current_difficulty = 0xA2CB1211629F6141; // starting difficulty (requires approx 180 Khashes to find a solution)
            if (Node.blockChain.getLastBlockNum() > 1)
            {
                Block previous_block = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum());
                if (previous_block != null)
                    current_difficulty = previous_block.difficulty;

                // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
                ulong solved_blocks = Node.blockChain.getSolvedBlocksCount(CoreConfig.getRedactedWindowSize(2));
                ulong window_size = CoreConfig.getRedactedWindowSize(2);

                // Special consideration for early blocks
                if (Node.blockChain.getLastBlockNum() < window_size)
                {
                    window_size = Node.blockChain.getLastBlockNum();
                }
                // 
                BigInteger target_hashes_per_block = Miner.getTargetHashcountPerBlock(current_difficulty);
                BigInteger actual_hashes_per_block = target_hashes_per_block * solved_blocks / (window_size / 2);
                ulong target_difficulty = 0;
                if (actual_hashes_per_block != 0)
                {
                    // find an appropriate difficulty for actual hashes:
                    target_difficulty = Miner.calculateTargetDifficulty(actual_hashes_per_block);
                }
                else
                {
                    // set our minimum difficulty
                    target_difficulty = 0xA2CB1211629F6141;
                }
                // we amortize the change by 32th of the redacted window
                // The reason behind this is:
                //   Whenever difficulty changes, old blocks in the redacted window retain their assigned difficulty from when they were accepted into the chain.
                //   Therefore, it is possible there are still window_size-1 *easier* blocks in the redacted window, ready to be solved. The new difficulty will only
                //   be valid for the currently-accepting-block.
                //   This means, that the number of solved blocks vs unsolved will keep rising for a while, even if we ramp up the difficulty significantly. This causes
                //   "spikes" and drops in the difficulty curve and we don't want that.
                ulong next_difficulty = 0;
                ulong amortization = window_size / 32;
                if (amortization == 0) amortization = 1;
                ulong delta = 0;
                if (target_difficulty > current_difficulty)
                {
                    delta = (target_difficulty - current_difficulty) / amortization;
                    next_difficulty = current_difficulty + delta;
                }
                else if (target_difficulty < current_difficulty)
                {
                    delta = (current_difficulty - target_difficulty) / amortization;
                    next_difficulty = current_difficulty - delta;
                }
                else
                {
                    //difficulties are equal
                    next_difficulty = current_difficulty;
                }
                // clamp to minimum
                if (next_difficulty < 0xA2CB1211629F6141)
                {
                    delta = 0;
                    next_difficulty = 0xA2CB1211629F6141;
                }
                // TODO: maybe pretty-fy the hashrate (ie: 15 MH/s, rather than 15000000 H/s) also could prettify the difficulty number
                Logging.info(String.Format("Estimated network hash rate is {0} H/s (previous was: {1} H/s). Difficulty adjusts from {2} -> {3}. (Delta: {4}{5})",
                    (actual_hashes_per_block / 60).ToString(),
                    (target_hashes_per_block / 60).ToString(),
                    current_difficulty, next_difficulty,
                    target_difficulty > current_difficulty ? "+" : "-", delta));
                current_difficulty = next_difficulty;
            }

            return current_difficulty;
        }

        private static BigInteger calculateEstimatedHashRate()
        {
            // to get the EHR, we'll take PoW solutions from last 10 block and calculate the total hashrate, in the event of ~45-55% of solved blocks, we should get a relatively accurate result
            ulong last_block_num = Node.getLastBlockHeight();
            BigInteger hash_rate = 0;
            uint i = 0;
            for (i = 0; i < 10; i++)
            {
                Block b = Node.blockChain.getBlock(last_block_num - i, false, true);
                List<Transaction> b_txs = TransactionPool.getFullBlockTransactions(b).FindAll(x => x.type == (int)Transaction.Type.PoWSolution);
                foreach (Transaction tx in b_txs)
                {
                    Block pow_b = Node.blockChain.getBlock(BitConverter.ToUInt64(tx.data, 0), false, false);
                    if(pow_b == null)
                    {
                        continue;
                    }
                    hash_rate += Miner.getTargetHashcountPerBlock(pow_b.difficulty);
                }
            }
            hash_rate = hash_rate / (i / 2); // i / 2 since every second block has to be full
            if(hash_rate == 0)
            {
                hash_rate = 1000;
            }
            return hash_rate;
        }

        // returns number of different solved blocks via PoW in last block
        private static long countLastBlockPowSolutions()
        {
            Block b = Node.blockChain.getLastBlock();
            List<Transaction> b_txs = TransactionPool.getFullBlockTransactions(b).FindAll(x => x.type == (int)Transaction.Type.PoWSolution);
            Dictionary<ulong, ulong> solved_blocks = new Dictionary<ulong, ulong>();
            foreach (Transaction tx in b_txs)
            {
                ulong pow_block_num = BitConverter.ToUInt64(tx.data, 0);
                solved_blocks.AddOrReplace(pow_block_num, pow_block_num);
            }
            return solved_blocks.LongCount();
        }

        public static ulong calculateDifficulty_v3()
        {
            ulong min_difficulty = 0xA2CB1211629F6141; // starting/min difficulty (requires approx 180 Khashes to find a solution)
            ulong current_difficulty = min_difficulty;

            if(Node.blockChain.getLastBlockNum() <= 10)
            {
                return current_difficulty;
            }
            Block previous_block = Node.blockChain.getLastBlock();
            if (previous_block != null)
                current_difficulty = previous_block.difficulty;

            // Increase or decrease the difficulty according to the number of solved blocks in the redacted window
            ulong solved_blocks = Node.blockChain.getSolvedBlocksCount(CoreConfig.getRedactedWindowSize(2));
            ulong window_size = CoreConfig.getRedactedWindowSize(2);

            // Special consideration for early blocks
            if (Node.blockChain.getLastBlockNum() < window_size)
            {
                window_size = Node.blockChain.getLastBlockNum();
            }

            ulong next_difficulty = min_difficulty;
            BigInteger current_hashes_per_block = 0;
            BigInteger previous_hashes_per_block = Miner.getTargetHashcountPerBlock(current_difficulty);

            // if there are more than 3/4 of solved blocks, max out the difficulty
            if (solved_blocks > window_size * 0.75f)
            {
                next_difficulty = ulong.MaxValue;
            }
            else if (solved_blocks < window_size * 0.25f)
            {
                // if there are less than 25% of solved blocks, set min difficulty
                next_difficulty = min_difficulty;
            }else
            {
                if (solved_blocks < window_size * 0.48f)
                {
                    // if there are between 25% and 48% of solved blocks, ideally use estimated hashrate * 0.7 for difficulty
                    current_hashes_per_block = calculateEstimatedHashRate() * 7 / 10; // * 0.7f
                    next_difficulty = Miner.calculateTargetDifficulty(current_hashes_per_block);
                }
                else if (solved_blocks < window_size * 0.53f)
                {
                    // if there are between 48% and 53% of solved blocks, ideally use estimated hashrate * 1.5 for difficulty
                    current_hashes_per_block = calculateEstimatedHashRate() * 15 / 10; // * 1.5f
                    next_difficulty = Miner.calculateTargetDifficulty(current_hashes_per_block);
                }
                else
                {
                    // otherwise there's between 53% and 75% solved blocks, use estimated hashrate * (10 + (n / 10)) for difficulty, where n is number of blocks solved over 50%
                    // to get estimated hashrate, use previous block's hashrate
                    long n = (long)solved_blocks - (long)(window_size * 0.50f);
                    long solutions_in_previous_block = countLastBlockPowSolutions();
                    long previous_n = 0;
                    if(window_size < CoreConfig.getRedactedWindowSize())
                    {
                        previous_n = (long)solved_blocks - solutions_in_previous_block - (long)((window_size - 1) * 0.50f);
                    }else
                    {
                        previous_n = (long)solved_blocks - solutions_in_previous_block - (long)(window_size * 0.50f);
                    }
                    BigInteger estimated_hash_rate = previous_hashes_per_block / (10 + (previous_n / 10));
                    next_difficulty = Miner.calculateTargetDifficulty(estimated_hash_rate * (10 + (n / 10)));
                }

            }
            
            // clamp to minimum
            if (next_difficulty < min_difficulty)
            {
                next_difficulty = min_difficulty;
            }

            // TODO: maybe pretty-fy the hashrate (ie: 15 MH/s, rather than 15000000 H/s) also could prettify the difficulty number
            Logging.info(String.Format("Estimated network hash rate is {0} H/s (previous was: {1} H/s). Difficulty adjusts from {2} -> {3}. (Delta: {4})",
                (current_hashes_per_block / 60).ToString(),
                (previous_hashes_per_block / 60).ToString(),
                current_difficulty, next_difficulty,
                current_difficulty - next_difficulty));
            current_difficulty = next_difficulty;

            return current_difficulty;
        }

        // Retrieve the signature freeze of the 5th last block
        public byte[] getSignatureFreeze()
        {
            // Prevent calculations if we don't have 5 fully generated blocks yet
            if(Node.blockChain.getLastBlockNum() < 5)
            {
                return null;
            }

            // Last block num - 4 gets us the 5th last block
            Block targetBlock = Node.blockChain.getBlock(Node.blockChain.getLastBlockNum() - 4);
            if (targetBlock == null)
            {
                return null;
            }

            // Calculate the signature checksum
            byte[] sigFreezeChecksum = targetBlock.calculateSignatureChecksum();
            return sigFreezeChecksum;
        }

        // Generate all the staking transactions for this block
        public List<Transaction> generateStakingTransactions(ulong targetBlockNum, int block_version, bool ws_snapshot = false, long block_timestamp = 0)
        {
            List<Transaction> transactions = new List<Transaction>();
            /// WARNING WARNING WARNING
            //return transactions;

            // Prevent distribution if we don't have 10 fully generated blocks yet
            if (Node.blockChain.getLastBlockNum() < 10)
            {
                return transactions;
            }

            Block targetBlock = Node.blockChain.getBlock(targetBlockNum);
            if (targetBlock == null)
            {
                return transactions;
            }

            IxiNumber totalIxis = Node.walletState.calculateTotalSupply();
            //Logging.info(String.Format("totalIxis = {0}", totalIxis.ToString()));
            IxiNumber inflationPA = new IxiNumber("0.1"); // 0.1% inflation per year for the first month

            if (targetBlockNum > 86400) // increase inflation to 5% after 1 month
            {
                inflationPA = new IxiNumber("5");
            }

            IxiNumber newIxis = 0;

            // Set the annual inflation to 1% after 50bn IXIs in circulation 
            if (totalIxis > new IxiNumber("50000000000") && totalIxis <= new IxiNumber("100000000000"))
            {
                inflationPA = new IxiNumber("1");
                newIxis = totalIxis * inflationPA / new IxiNumber("100000000"); // approximation of 2*60*24*365*100
            }
            else if(totalIxis > new IxiNumber("100000000000"))
            {
                newIxis = 1000;
            }
            else
            {
                // Calculate the amount of new IXIs to be minted
                newIxis = totalIxis * inflationPA / new IxiNumber("100000000"); // approximation of 2*60*24*365*100
            }
            //Logging.info(String.Format("inflationPA = {0}", inflationPA.ToString()));

            //Logging.info(String.Format("newIxis = {0}", newIxis.ToString()));
            //Console.ForegroundColor = ConsoleColor.Magenta;
            //Console.WriteLine("----STAKING REWARDS for #{0} TOTAL {1} IXIs----", targetBlock.blockNum, newIxis.ToString());
            // Retrieve the list of signature wallets
            List<byte[]> signatureWallets = targetBlock.getSignaturesWalletAddresses();

            IxiNumber totalIxisStaked = new IxiNumber(0);
            byte[][] stakeWallets = new byte[signatureWallets.Count][];
            BigInteger[] stakes = new BigInteger[signatureWallets.Count];
            BigInteger[] awards = new BigInteger[signatureWallets.Count];
            BigInteger[] awardRemainders = new BigInteger[signatureWallets.Count];
            // First pass, go through each wallet to find its balance
            int stakers = 0;
            foreach (byte[] wallet_addr in signatureWallets)
            {
                Wallet wallet = Node.walletState.getWallet(wallet_addr, ws_snapshot);
                if (wallet.balance.getAmount() > 0)
                {
                    totalIxisStaked += wallet.balance;
                    //Logging.info(String.Format("wallet {0} stakes {1} IXI", Base58Check.Base58CheckEncoding.EncodePlain(wallet_addr), wallet.balance.ToString()));
                    stakes[stakers] = wallet.balance.getAmount();
                    stakeWallets[stakers] = wallet_addr;
                    stakers += 1;
                }
            }
            //Logging.info(String.Format("Stakers: {0}, totalIxisStaked = {1}", stakers, totalIxisStaked.ToString()));

            if (totalIxisStaked.getAmount() <= 0)
            {
                Logging.warn(String.Format("No Ixis were staked or a logic error occured - total ixi staked returned: {0}", totalIxisStaked.getAmount()));
                return transactions;
            }

            // Second pass, determine awards by stake
            //Logging.info("Determining awards");

            BigInteger totalAwarded = 0;
            for (int i = 0; i < stakers; i++)
            {
                BigInteger p = (newIxis.getAmount() * stakes[i] * 100) / totalIxisStaked.getAmount();
                //Logging.info(String.Format("staker[{0}]: p = {1}", i, p.ToString()));
                awardRemainders[i] = p % 100;
                //Logging.info(String.Format("staker[{0}]: awardRemainder = {1}", i, awardRemainders[i].ToString()));
                p = p / 100;
                awards[i] = p;
                //Logging.info(String.Format("staker[{0}]: award = {1}", i, awards[i].ToString()));
                totalAwarded += p;
            }
            //Logging.info(String.Format("totalAwarded = {0}", totalAwarded.ToString()));

            // Third pass, distribute remainders, if any
            // This essentially "rounds up" the awards for the stakers closest to the next whole amount,
            // until we bring the award difference down to zero.
            //Logging.info("Determining remainders");
            BigInteger diffAward = newIxis.getAmount() - totalAwarded;
            //Logging.info(String.Format("diffAward = {0}", diffAward.ToString()));
            if (diffAward > 0)
            {
                int[] descRemaindersIndexes = awardRemainders
                    .Select((v, pos) => new KeyValuePair<BigInteger, int>(v, pos))
                    .OrderByDescending(x => x.Key)
                    .Select(x => x.Value).ToArray();
                int currRemainderAward = 0;
                while (diffAward > 0)
                {
                    awards[descRemaindersIndexes[currRemainderAward]] += 1;
                    //Logging.info(String.Format("Increasing reward {0} by 1, to: {1}", descRemaindersIndexes[currRemainderAward], awards[descRemaindersIndexes[currRemainderAward]].ToString()));
                    currRemainderAward += 1;
                    diffAward -= 1;
                }
            }

            if (block_version < 2)
            {
                for (int i = 0; i < stakers; i++)
                {
                    IxiNumber award = new IxiNumber(awards[i]);
                    //Logging.info(String.Format("Final reward for staker {0}: {1}", i, award.ToString()));
                    if (award > (long)0)
                    {
                        byte[] wallet_addr = stakeWallets[i];
                        //Console.WriteLine("----> Awarding {0} to {1}", award, wallet_addr);

                        Transaction tx = new Transaction((int)Transaction.Type.StakingReward, award, new IxiNumber(0), wallet_addr, CoreConfig.ixianInfiniMineAddress, BitConverter.GetBytes(targetBlock.blockNum), null, Node.blockChain.getLastBlockNum(), 0, block_timestamp);

                        transactions.Add(tx);

                    }

                }
            }else
            {
                SortedDictionary<byte[], IxiNumber> to_list = new SortedDictionary<byte[], IxiNumber>(new ByteArrayComparer());
                for (int i = 0; i < stakers; i++)
                {
                    IxiNumber award = new IxiNumber(awards[i]);
                    //Logging.info(String.Format("Final reward for staker {0}: {1}", i, award.ToString()));
                    if (award > (long)0)
                    {
                        byte[] wallet_addr = stakeWallets[i];
                        //Console.WriteLine("----> Awarding {0} to {1}", award, wallet_addr);
                        to_list.Add(wallet_addr, award);

                    }

                }
                Transaction tx = new Transaction((int)Transaction.Type.StakingReward, new IxiNumber(0), to_list, CoreConfig.ixianInfiniMineAddress, BitConverter.GetBytes(targetBlock.blockNum), null, Node.blockChain.getLastBlockNum(), 0, block_timestamp);

                transactions.Add(tx);
            }
            //Console.WriteLine("------");
            //Console.ResetColor();


            return transactions;
        }


        // Distribute the staking rewards according to the 5th last block signatures
        public bool distributeStakingRewards(Block b, int block_version, bool ws_snapshot = false)
        {
            // Prevent distribution if we don't have 10 fully generated blocks yet
            if (Node.blockChain.getLastBlockNum() < 10)
            {
                return false;
            }

            if (ws_snapshot == false)
            {
                List<Transaction> transactions = generateStakingTransactions(b.blockNum - 6, block_version, ws_snapshot, b.timestamp);
                foreach (Transaction transaction in transactions)
                {
                    TransactionPool.addTransaction(transaction, true);
                }
            }
            
            return true;
        }

        public bool hasNewBlock()
        {
            return localNewBlock != null;
        }

        public Block getLocalBlock()
        {
            return localNewBlock;
        }

        public bool addSignatureToBlock(ulong block_num, byte[] checksum, byte[] signature, byte[] address_or_pub_key, RemoteEndpoint endpoint)
        {
            ulong last_block_num = Node.blockChain.getLastBlockNum();
            if (block_num > last_block_num - 4 && block_num <= last_block_num)
            {
                Block b = Node.blockChain.getBlock(block_num, false, false);
                if (b != null && b.blockChecksum.SequenceEqual(checksum))
                {
                    return b.addSignature(signature, address_or_pub_key);
                }else
                {
                    ProtocolMessage.broadcastGetBlock(block_num, null, endpoint);
                }
            }
            else if (block_num == last_block_num + 1)
            {
                lock (Node.blockProcessor.localBlockLock)
                {
                    Block b = Node.blockProcessor.getLocalBlock();
                    if (b != null && b.blockChecksum.SequenceEqual(checksum))
                    {
                        bool sig_added = b.addSignature(signature, address_or_pub_key);
                        if (sig_added)
                        {
                            currentBlockStartTime = DateTime.UtcNow;
                            lastBlockStartTime = DateTime.UtcNow.AddSeconds(-blockGenerationInterval * 10);
                        }
                        return sig_added;
                    }
                    else
                    {
                        ProtocolMessage.broadcastGetBlock(block_num, null, endpoint);
                    }
                }
            }
            return false;
        }


        // Updates the walletstate public keys. Called from BlockProcessor applyAcceptedBlock()
        public bool updateWalletStatePublicKeys(ulong blockNum, bool ws_snapshot = false)
        {
            Block targetBlock = Node.blockChain.getBlock(blockNum - 6, false);
            if (targetBlock == null)
            {
                return false;
            }
            List<byte[][]> sigs = targetBlock.signatures;
            foreach (byte[][] sig in sigs)
            {
                byte[] signature = sig[0];
                byte[] signerPubkeyOrAddress = sig[1];

                if (signerPubkeyOrAddress.Length < 70)
                {
                    byte[] signerAddress = signerPubkeyOrAddress;
                    Wallet signerWallet = Node.walletState.getWallet(signerAddress);
                    if (signerWallet.publicKey == null)
                    {
                        Logging.error("Signer wallet's pubKey entry is null, expecting a non-null entry");
                        continue;
                    }
                }
                else
                {
                    byte[] signerPubKey = signerPubkeyOrAddress;
                    // Generate an address
                    Address p_address = new Address(signerPubKey);
                    Wallet signerWallet = Node.walletState.getWallet(p_address.address);
                    if (signerWallet.publicKey == null)
                    {
                        // Set the WS public key
                        Node.walletState.setWalletPublicKey(p_address.address, signerPubKey, ws_snapshot);
                    }
                }
            }

            return true;
        }
    }
}
