using DLT.Meta;
using IXICore;
using IXICore.Utils;
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
                                    byte[] challenge_response = null;
                                    try
                                    {
                                        // TODO TODO TODO TODO TODO try/catch wrapper will be removed when everybody upgrades
                                        int challenge_len = reader.ReadInt32();
                                        byte[] challenge = reader.ReadBytes(challenge_len);

                                        challenge_response = CryptoManager.lib.getSignature(challenge, Node.walletStorage.getPrimaryPrivateKey());
                                    }
                                    catch (Exception e)
                                    {

                                    }


                                    CoreProtocolMessage.sendHelloMessage(endpoint, true, challenge_response);
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
                                    char node_type = endpoint.presenceAddress.type;
                                    if (node_type != 'M' && node_type != 'H')
                                    {
                                        CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.expectingMaster, string.Format("Expecting master node."), "", true);
                                        return;
                                    }

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

                                    int challenge_response_len = reader.ReadInt32();
                                    byte[] challenge_response = reader.ReadBytes(challenge_response_len);
                                    if (!CryptoManager.lib.verifySignature(endpoint.challenge, endpoint.serverPubKey, challenge_response))
                                    {
                                        CoreProtocolMessage.sendBye(endpoint, ProtocolByeCode.authFailed, string.Format("Invalid challenge response."), "", true);
                                        return;
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
                            CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.newTransaction, data, null);
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
                                CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'R', 'H', 'W' }, ProtocolMessageCode.keepAlivePresence, data, address, endpoint);

                                // Send this keepalive message to all connected clients
                                CoreProtocolMessage.broadcastEventDataMessage(NetworkEvents.Type.keepAlive, address, ProtocolMessageCode.keepAlivePresence, data, address, endpoint);
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
                                            byte[][] presence_chunks = p.getByteChunks();
                                            int i = 0;
                                            foreach (byte[] presence_chunk in presence_chunks)
                                            {
                                                endpoint.sendData(ProtocolMessageCode.updatePresence, presence_chunk);
                                                i++;
                                            }
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
                                    endpoint.stop();

                                    bool byeV1 = false;
                                    try
                                    {
                                        ProtocolByeCode byeCode = (ProtocolByeCode)reader.ReadInt32();
                                        string byeMessage = reader.ReadString();
                                        string byeData = reader.ReadString();

                                        byeV1 = true;

                                        switch (byeCode)
                                        {
                                            case ProtocolByeCode.bye: // all good
                                                break;

                                            case ProtocolByeCode.forked: // forked node disconnected
                                                Logging.info(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));
                                                break;

                                            case ProtocolByeCode.deprecated: // deprecated node disconnected
                                                Logging.info(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));
                                                break;

                                            case ProtocolByeCode.incorrectIp: // incorrect IP
                                                if (IxiUtils.validateIPv4(byeData))
                                                {
                                                    if (NetworkClientManager.getConnectedClients().Length < 2)
                                                    {
                                                        Config.publicServerIP = byeData;
                                                        Logging.info("Changed internal IP Address to " + byeData + ", reconnecting");
                                                    }
                                                }
                                                break;

                                            case ProtocolByeCode.notConnectable: // not connectable from the internet
                                                Logging.error("This node must be connectable from the internet, to connect to the network.");
                                                Logging.error("Please setup uPNP and/or port forwarding on your router for port " + Config.serverPort + ".");
                                                NetworkServer.connectable = false;
                                                break;

                                            case ProtocolByeCode.insufficientFunds:
                                                break;

                                            default:
                                                Logging.warn(string.Format("Disconnected with message: {0} {1}", byeMessage, byeData));
                                                break;
                                        }
                                    }
                                    catch (Exception)
                                    {

                                    }
                                    if (byeV1)
                                    {
                                        return;
                                    }

                                    reader.BaseStream.Seek(0, SeekOrigin.Begin);

                                    // Retrieve the message
                                    string message = reader.ReadString();

                                    if (message.Length > 0)
                                        Logging.info(string.Format("Disconnected with message: {0}", message));
                                    else
                                        Logging.info("Disconnected");
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