﻿using DLT.Meta;
using IXICore;
using SPIXI;
using SPIXI.Storage;
using System;
using System.IO;
using System.Linq;

namespace DLT.Network
{
    public class ProtocolMessage
    {
        // Unified protocol message parsing
        public static void parseProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint endpoint)
        {
            if (endpoint == null)
            {
                Logging.error("Endpoint was null. parseProtocolMessage");
                return;
            }
            try
            {
                switch (code)
                {
                    case ProtocolMessageCode.hello:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    if (CoreProtocolMessage.processHelloMessage(endpoint, reader))
                                    {
                                        CoreProtocolMessage.sendHelloMessage(endpoint, true);
                                        endpoint.helloReceived = true;
                                        return;
                                    }
                                }
                            }

                        }
                        break;


                    case ProtocolMessageCode.helloData:
                       using (MemoryStream m = new MemoryStream(data))
                        {
                            using (BinaryReader reader = new BinaryReader(m))
                            {
                                if (CoreProtocolMessage.processHelloMessage(endpoint, reader))
                                {
                                    ulong last_block_num = reader.ReadUInt64();

                                    int bcLen = reader.ReadInt32();
                                    byte[] block_checksum = reader.ReadBytes(bcLen);
                                    int wsLen = reader.ReadInt32();
                                    byte[] walletstate_checksum = reader.ReadBytes(wsLen);
                                    int consensus = reader.ReadInt32();

                                    endpoint.blockHeight = last_block_num;

                                    int block_version = reader.ReadInt32();

                                    Node.setLastBlock(last_block_num, block_checksum, walletstate_checksum, block_version);
                                    Node.setRequiredConsensus(consensus);

                                    // Check for legacy level
                                    ulong legacy_level = reader.ReadUInt64();

                                    // Check for legacy node
                                    if (Legacy.isLegacy(legacy_level))
                                    {
                                        // TODO TODO TODO TODO check this out
                                        //endpoint.setLegacy(true);
                                    }

                                    // Process the hello data
                                    endpoint.helloReceived = true;
                                    NetworkClientManager.recalculateLocalTimeDifference();
                                }
                                // Get presences
                                endpoint.sendData(ProtocolMessageCode.syncPresenceList, new byte[1]);
                            }
                        }
                        break;

                    case ProtocolMessageCode.s2data:
                        {
                            StreamProcessor.receiveData(data, endpoint);
                        }
                        break;

                    case ProtocolMessageCode.s2keys:
                        {
                            Console.WriteLine("NET: Receiving S2 keys!");
                    //        StreamProcessor.receivedKeys(data, socket);
                        }
                        break;

                    case ProtocolMessageCode.syncPresenceList:
                        {
                            byte[] pdata = PresenceList.getBytes();
                     //       byte[] ba = prepareProtocolMessage(ProtocolMessageCode.presenceList, pdata);
                     //       socket.Send(ba, SocketFlags.None);
                        }
                        break;

                    case ProtocolMessageCode.presenceList:
                        {
                            Logging.info("NET: Receiving complete presence list");
                            PresenceList.syncFromBytes(data);
                     //       NetworkClientManager.searchForStreamNode();
                        }
                        break;

                    case ProtocolMessageCode.updatePresence:
                        {
                            Console.WriteLine("NET: Receiving presence list update");
                            // Parse the data and update entries in the presence list
                            PresenceList.updateFromBytes(data);
                        }
                        break;


                    case ProtocolMessageCode.balance:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    int address_length = reader.ReadInt32();
                                    byte[] address = reader.ReadBytes(address_length);

                                    // Retrieve the latest balance
                                    IxiNumber balance = reader.ReadString();

                                    if(address.SequenceEqual(Node.walletStorage.getPrimaryAddress()))
                                    {
                                        Node.balance = balance;
                                    }

                                    // Retrieve the blockheight for the balance
                                    ulong blockheight = reader.ReadUInt64();
                                    Node.blockHeight = blockheight;
                                }
                            }
                        }
                        break;

                    case ProtocolMessageCode.transactionData:
                        {
                            // TODO: check for errors/exceptions
                            Transaction transaction = new Transaction(data);
                            TransactionCache.addTransaction(transaction);
                        }
                        break;

                    case ProtocolMessageCode.newTransaction:
                        {
                            // Forward the new transaction message to the DLT network
                            Logging.info("RECIEVED NEW TRANSACTION");

                            Transaction transaction = new Transaction(data);
                            if (transaction.toList.Keys.First().SequenceEqual(Node.walletStorage.getPrimaryAddress()))
                            {
                                TransactionCache.addTransaction(transaction);
                            }
                        }
                        break;

                    case ProtocolMessageCode.bye:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    // Retrieve the message
                                    string message = reader.ReadString();
                                    endpoint.stop();

                                    Logging.error(string.Format("Disconnected with message: {0}", message));
                                }
                            }
                        }
                        break;

                    case ProtocolMessageCode.ping:
                        {
                            endpoint.sendData(ProtocolMessageCode.pong, new byte[1]);
                        }
                        break;

                    case ProtocolMessageCode.pong:
                        {
                            // do nothing
                        }
                        break;

                    default:
                        break;

                }
            }
            catch (Exception e)
            {
                Logging.error(string.Format("Error parsing network message. Details: {0}", e.ToString()));
            }
        }
    }
}
