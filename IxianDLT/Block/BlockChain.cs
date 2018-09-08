﻿using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    class BlockChain
    {
        public ulong redactedWindow { get => redactedWindowSize; }

        public ulong redactedWindowSize = 3000; // approx 25 hours - this should be a network-calculable parameter at some point
        List<Block> blocks = new List<Block>();

        public int Count { get => blocks.Count; }

        public BlockChain()
        {
        }

        public void onUpdate() {
            lock (blocks)
            {
                // redaction
                int begin_size = blocks.Count();
                while ((ulong)blocks.Count() > redactedWindowSize)
                {
                    blocks.RemoveAt(0);
                }
                if (begin_size > blocks.Count())
                {
                    Console.WriteLine(String.Format("REDACTED {0} blocks to keep the chain length appropriate.", begin_size - blocks.Count()));
                }
            }
        }

        public bool appendBlock(Block b)
        {
            lock (blocks)
            {
                // special case when we are starting up and have an empty chain
                if (blocks.Count == 0)
                {
                    blocks.Add(b);
                    Storage.insertBlock(b);
                    return true;
                }
                // check for invalid block appending
                if (b.blockNum != blocks[blocks.Count - 1].blockNum + 1)
                {
                    Logging.warn(String.Format("Attempting to add non-sequential block #{0} after block #{1}.",
                        b.blockNum,
                        blocks[blocks.Count - 1].blockNum));
                    return false;
                }
                if(b.lastBlockChecksum != blocks[blocks.Count-1].blockChecksum)
                {
                    Logging.warn(String.Format("Attempting to add a block #{0}with invalid lastBlockChecksum!", b.blockNum));
                    return false;
                }
                blocks.Add(b);
                Storage.insertBlock(b);
                return true;
            }
        }


        public Block getBlock(ulong blocknum)
        {
            lock(blocks)
            {
                return blocks.Find(x => x.blockNum == blocknum);
            }
        }

        public ulong getLastBlockNum()
        {
            if (blocks.Count == 0) return 0;
            lock (blocks)
            {
                return blocks[blocks.Count - 1].blockNum;
            }
        }

        public int getRequiredConsensus()
        {
            int blockOffset = 5;
            if (blocks.Count < blockOffset + 1) return 1; // special case for first X blocks - since sigFreeze happens n-X blocks
            lock (blocks)
            {
                int totalConsensus = 0;
                int blockCount = blocks.Count - blockOffset < 10 ? blocks.Count - blockOffset : 10;
                for (int i = 0; i < blockCount; i++)
                {
                    totalConsensus += blocks[blocks.Count - i - blockOffset - 1].signatures.Count;
                }
                int consensus = (int)Math.Ceiling(totalConsensus / blockCount * Config.networkConsensusRatio);
                if (consensus < 2)
                {
                    consensus = 2;
                }
                return consensus;
            }
        }

        public string getLastBlockChecksum()
        {
            if (blocks.Count == 0) return "";
            lock(blocks)
            {
                return blocks[blocks.Count - 1].blockChecksum;
            }
        }

        public string getCurrentWalletState()
        {
            if (blocks.Count == 0) return "";
            lock(blocks)
            {
                return blocks[blocks.Count - 1].walletStateChecksum;
            }
        }

        public int getBlockSignaturesReverse(int index)
        {
            if (blocks.Count - index - 1 < 0) return 0;
            lock(blocks)
            {
                return blocks[blocks.Count - index - 1].signatures.Count();
            }
        }

        public bool refreshSignatures(Block b)
        {
            // we refuse to change sig numbers older than 5 blocks
            ulong sigLockHeight = getLastBlockNum() > 5 ? getLastBlockNum() - 5 : 1;
            if(b.blockNum <= sigLockHeight)
            {
                return false;
            }
            lock (blocks)
            {
                int idx = blocks.FindIndex(x => x.blockNum == b.blockNum && x.blockChecksum == b.blockChecksum);
                if (idx > 0)
                {
                    int beforeSigs = blocks[idx].signatures.Count;
                    blocks[idx].addSignaturesFrom(b);
                    int afterSigs = blocks[idx].signatures.Count;
                    if (beforeSigs != afterSigs)
                    {
                        Logging.info(String.Format("Refreshed block #{0}: Updated signatures {1} -> {2}", b.blockNum, beforeSigs, afterSigs));
                        return true;
                    }
                }
            }
            return false;
        }

        // Returns the last block that has a signatureFreezeChecksum set
        public Block getLastSigFreezedBlock()
        {
            for(int i = 1; i < blocks.Count; i++)
            {
                if(blocks[blocks.Count - i].signatureFreezeChecksum != "0")
                {
                    return blocks[blocks.Count - i];
                }
            }
            return null;
        }

        // Gets the elected node's pub key from the last sigFreeze; offset defines which entry to pick from the sigs
        public string getLastElectedNodePubKey(int offset = 0)
        {
            Block b = getLastSigFreezedBlock();
            if (b != null)
            {
                string sigFreezeChecksum = b.signatureFreezeChecksum;
                int sigNr = BitConverter.ToInt32(Encoding.UTF8.GetBytes(sigFreezeChecksum), 0) + offset;
                string sig = b.signatures[sigNr % b.signatures.Count];

                string[] parts = sig.Split(Block.splitter, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                {
                    return null;
                }
                return parts[1]; // signer pub key
            }
            return null;
        }

        // Get the number of PoW solved blocks
        public ulong getSolvedBlocksCount()
        {
            ulong solved_blocks = 0;

            lock (blocks)
            {
                foreach (Block block in blocks)
                {
                    if(block.powField.Length > 0)
                    {
                        // TODO: an additional verification would be nice
                        solved_blocks++;
                    }
                }
            }
            return solved_blocks;
        }
    }
}
