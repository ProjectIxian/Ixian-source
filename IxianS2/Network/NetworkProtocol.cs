using DLT.Meta;
using IXICore;
using S2;
using S2.Network;
using System;
using System.IO;
using System.Linq;
using System.Text;

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
                        break;


                    case ProtocolMessageCode.helloData:
                        using (MemoryStream m = new MemoryStream(data))
                        {
                            using (BinaryReader reader = new BinaryReader(m))
                            {
                                if (CoreProtocolMessage.processHelloMessage(endpoint, reader))
                                {
                                    ulong last_block_num = reader.ReadUInt64();

                                    // Update the node's block height
                                    if (Node.blockHeight < last_block_num)
                                        Node.blockHeight = last_block_num;

                                    int bcLen = reader.ReadInt32();
                                    byte[] block_checksum = reader.ReadBytes(bcLen);
                                    int wsLen = reader.ReadInt32();
                                    byte[] walletstate_checksum = reader.ReadBytes(wsLen);
                                    int consensus = reader.ReadInt32();

                                    endpoint.blockHeight = last_block_num;

                                    int block_version = 1;
                                    try
                                    {
                                        block_version = reader.ReadInt32();
                                    }
                                    catch (Exception)
                                    {
                                        block_version = 1;
                                    }

                                    // Check for legacy level
                                    ulong legacy_level = last_block_num;
                                    try
                                    {
                                        ulong level = reader.ReadUInt64();
                                        legacy_level = level;
                                    }
                                    catch (Exception)
                                    {
                                        legacy_level = 0;
                                    }

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
                            }
                        }
                        break;

                    case ProtocolMessageCode.s2data:
                        {
                            StreamProcessor.receiveData(data, endpoint);
                        }
                        break;

                    case ProtocolMessageCode.s2failed:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    Logging.error("Failed to send s2 data");
                                }
                            }
                        }
                        break;

                    case ProtocolMessageCode.s2signature:
                        {
                            StreamProcessor.receivedTransactionSignature(data, endpoint);
                        }
                        break;

                    case ProtocolMessageCode.newTransaction:
                        {
                            // Forward the new transaction message to the DLT network
                            CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.newTransaction, data);
                        }
                        break;

                    case ProtocolMessageCode.syncPresenceList:
                        {
                            byte[] pdata = PresenceList.getBytes();
                            byte[] ba = CoreProtocolMessage.prepareProtocolMessage(ProtocolMessageCode.presenceList, pdata);
                            endpoint.sendData(ProtocolMessageCode.presenceList, pdata);
                        }
                        break;

                    case ProtocolMessageCode.presenceList:
                        {
                            Logging.info("Receiving complete presence list");
                            PresenceList.syncFromBytes(data);
                        }
                        break;

                    case ProtocolMessageCode.updatePresence:
                        {
                            // Parse the data and update entries in the presence list
                            PresenceList.updateFromBytes(data);
                        }
                        break;


                    case ProtocolMessageCode.keepAlivePresence:
                        {
                            byte[] address = null;
                            bool updated = PresenceList.receiveKeepAlive(data, out address);

                            // If a presence entry was updated, broadcast this message again
                            if (updated)
                            {
                                CoreProtocolMessage.broadcastEventBasedMessage(ProtocolMessageCode.keepAlivePresence, data, address, endpoint);
                            }

                        }
                        break;

                    case ProtocolMessageCode.getPresence:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    int walletLen = reader.ReadInt32();
                                    byte[] wallet = reader.ReadBytes(walletLen);
                                    lock(PresenceList.presences)
                                    {
                                        // TODO re-verify this
                                        Presence p = PresenceList.presences.Find(x => x.wallet.SequenceEqual(wallet));
                                        if (p != null)
                                        {
                                            endpoint.sendData(ProtocolMessageCode.updatePresence, p.getBytes());
                                        }
                                        else
                                        {
                                            // TODO blacklisting point
                                            Logging.warn(string.Format("Node has requested presence information about {0} that is not in our PL.", Base58Check.Base58CheckEncoding.EncodePlain(wallet)));
                                        }
                                    }
                                }
                            }
                        }
                        break;


                    case ProtocolMessageCode.balance:
                        {
                            // TODO: make sure this is received from a DLT node only.
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    int address_length = reader.ReadInt32();
                                    byte[] address = reader.ReadBytes(address_length);

                                    // Retrieve the latest balance
                                    IxiNumber balance = reader.ReadString();

                                    if (address.SequenceEqual(Node.walletStorage.getPrimaryAddress()))
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

                    case ProtocolMessageCode.bye:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    // Retrieve the message
                                    string message = reader.ReadString();
                                    endpoint.stop();

                                    if (message.Length > 0)
                                        Logging.error(string.Format("Disconnected with message: {0}", message));
                                    else
                                        Logging.error("Disconnected");
                                }
                            }
                        }
                        break;

                    case ProtocolMessageCode.extend:
                        {
                            if(Config.isTestClient)
                            {
                                TestClientNode.handleExtendProtocol(data);
                            }
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