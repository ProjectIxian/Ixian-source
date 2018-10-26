﻿using DLT;
using DLT.Meta;
using DLT.Network;
using IXICore;
using S2.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT.Network
{
    public class ProtocolMessage
    {

        // Broadcast a protocol message across clients and nodes
        // Returns true if it sent the message at least one endpoint. Returns false if the message couldn't be sent to any endpoints
        public static bool broadcastProtocolMessage(ProtocolMessageCode code, byte[] data, RemoteEndpoint skipEndpoint = null, bool sendToSingleRandomNode = false)
        {
            if (data == null)
            {
                Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                return false;
            }

            if (sendToSingleRandomNode)
            {
                int serverCount = NetworkClientManager.getConnectedClients().Count();
                int clientCount = 0;// NetworkStreamServer.getConnectedClients().Count();

                Random r = new Random();
                int rIdx = r.Next(serverCount + clientCount);

                RemoteEndpoint re = null;

                if (rIdx < serverCount)
                {
                    re = NetworkClientManager.getClient(rIdx);
                }
                else
                {
             //       re = NetworkStreamServer.getClient(rIdx - serverCount);
                }
                if (re != null && re.isConnected())
                {
                    re.sendData(code, data);
                    return true;
                }
                return false;
            }
            else
            {
                bool c_result = NetworkClientManager.broadcastData(code, data, skipEndpoint);
                bool s_result = false;// NetworkStreamServer.broadcastData(code, data, skipEndpoint);

                if (!c_result && !s_result)
                    return false;
            }



            return true;
        }


        public static bool processHelloMessage(RemoteEndpoint endpoint, BinaryReader reader)
        {
            // Node already has a presence
            if (endpoint.presence != null)
            {
                // Ignore the hello message in this case
                return false;
            }

            return true;
        }

        public static void sendHelloMessage(RemoteEndpoint endpoint, bool sendHelloData)
        {
            using (MemoryStream m = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(m))
                {
                    string publicHostname = string.Format("{0}:{1}", Config.publicServerIP, Config.serverPort);

                    // Send the node version
                    writer.Write(CoreConfig.protocolVersion);

                    // Send the public node address
                    byte[] address = Node.walletStorage.address;
                    writer.Write(address.Length);
                    writer.Write(address);

                    // Send the testnet designator
                    writer.Write(Config.isTestNet);

                    // Send the node type
                    char node_type = 'R'; // This is a Relay node

                    writer.Write(node_type);

                    // Send the version
                    writer.Write(Config.version);

                    // Send the node device id
                    writer.Write(Config.device_id);

                    // Send the wallet public key
                    writer.Write(Node.walletStorage.publicKey.Length);
                    writer.Write(Node.walletStorage.publicKey);

                    // Send listening port
                    writer.Write(Config.serverPort);

                    // Send timestamp
                    long timestamp = Core.getCurrentTimestamp();
                    writer.Write(timestamp);

                    // send signature
                    byte[] signature = CryptoManager.lib.getSignature(Encoding.UTF8.GetBytes(CoreConfig.ixianChecksumLockString + "-" + Config.device_id + "-" + timestamp + "-" + publicHostname), Node.walletStorage.privateKey);
                    writer.Write(signature.Length);
                    writer.Write(signature);


                    if (sendHelloData)
                    {
                        // Write the legacy level
                        writer.Write(Legacy.getLegacyLevel());

                        endpoint.sendData(ProtocolMessageCode.helloData, m.ToArray());

                    }
                    else
                    {
                        endpoint.sendData(ProtocolMessageCode.hello, m.ToArray());
                    }
                }
            }
        }






        // Read a protocol message from a byte array
        public static void readProtocolMessage(byte[] recv_buffer, RemoteEndpoint endpoint)
        {
            if (endpoint == null)
            {
                Logging.error("Endpoint was null. readProtocolMessage");
                return;
            }

            ProtocolMessageCode code = ProtocolMessageCode.hello;
            byte[] data = null;

            using (MemoryStream m = new MemoryStream(recv_buffer))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    // Check for multi-message packets. One packet can contain multiple network messages.
                    while (reader.BaseStream.Position < reader.BaseStream.Length)
                    {
                        byte[] data_checksum;
                        try
                        {
                            byte startByte = reader.ReadByte();

                            int message_code = reader.ReadInt32();
                            code = (ProtocolMessageCode)message_code;

                            int data_length = reader.ReadInt32();

                            // If this is a connected client, filter messages
                            if (endpoint.GetType() == typeof(RemoteEndpoint))
                            {
                                if (endpoint.presence == null)
                                {
                                    // Check for presence and only accept hello and syncPL messages if there is no presence.
                                    if (code == ProtocolMessageCode.hello || code == ProtocolMessageCode.syncPresenceList || code == ProtocolMessageCode.getBalance || code == ProtocolMessageCode.newTransaction)
                                    {

                                    }
                                    else
                                    {
                                        // Ignore anything else
                                        return;
                                    }
                                }
                            }




                            data_checksum = reader.ReadBytes(32); // sha256, 8 bits per byte
                            byte header_checksum = reader.ReadByte();
                            byte endByte = reader.ReadByte();
                            data = reader.ReadBytes(data_length);
                        }
                        catch (Exception e)
                        {
                            Logging.error(String.Format("NET: dropped packet. {0}", e));
                            return;
                        }
                        // Compute checksum of received data
                        byte[] local_checksum = Crypto.sha512sqTrunc(data);

                        // Verify the checksum before proceeding
                        if (local_checksum.SequenceEqual(data_checksum) == false)
                        {
                            Logging.error("Dropped message (invalid checksum)");
                            continue;
                        }

                        // For development purposes, output the proper protocol message
                        //Console.WriteLine(string.Format("NET: {0} | {1} | {2}", code, data_length, Crypto.hashToString(data_checksum)));

                        // Can proceed to parse the data parameter based on the protocol message code.
                        // Data can contain multiple elements.
                        //parseProtocolMessage(code, data, socket, endpoint);
                        NetworkQueue.receiveProtocolMessage(code, data, data_checksum, endpoint);
                    }
                }
            }




        }

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
                                if (processHelloMessage(endpoint, reader))
                                {
                                    sendHelloMessage(endpoint, true);
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
                                if (processHelloMessage(endpoint, reader))
                                {
                                    ulong last_block_num = reader.ReadUInt64();
                                    int bcLen = reader.ReadInt32();
                                    byte[] block_checksum = reader.ReadBytes(bcLen);
                                    int wsLen = reader.ReadInt32();
                                    byte[] walletstate_checksum = reader.ReadBytes(wsLen);
                                    int consensus = reader.ReadInt32();

                                    long myTimestamp = Core.getCurrentTimestamp();

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

                    case ProtocolMessageCode.s2prepareSend:
                        {
                            StreamProcessor.prepareSend(data, endpoint);
                        }
                        break;

                    case ProtocolMessageCode.s2generateKeys:
                        {
                            StreamProcessor.generateKeys(data, endpoint);
                        }
                        break;

                    case ProtocolMessageCode.newTransaction:
                        {
                            // Forward the new transaction message to the DLT network
                            broadcastProtocolMessage(ProtocolMessageCode.newTransaction, data);
                        }
                        break;

                    /*case ProtocolMessageCode.updateTransaction:
                        {
                            // Forward the update transaction message to the DLT network
                            broadcastProtocolMessage(ProtocolMessageCode.updateTransaction, data);
                        }
                        break;*/

                    case ProtocolMessageCode.syncPresenceList:
                        {
                            byte[] pdata = PresenceList.getBytes();
                            byte[] ba = CoreProtocolMessage.prepareProtocolMessage(ProtocolMessageCode.presenceList, pdata);
                            endpoint.sendData(ProtocolMessageCode.presenceList, pdata);
                        }
                        break;

                    case ProtocolMessageCode.presenceList:
                        {
                            Console.WriteLine("NET: Receiving complete presence list");
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
                            bool updated = PresenceList.receiveKeepAlive(data);
                            // If a presence entry was updated, broadcast this message again
                            if (updated)
                            {
                                broadcastProtocolMessage(ProtocolMessageCode.keepAlivePresence, data, endpoint);
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
                        break;

                    case ProtocolMessageCode.bye:
                        {
                            using (MemoryStream m = new MemoryStream(data))
                            {
                                using (BinaryReader reader = new BinaryReader(m))
                                {
                                    // Retrieve the message
                                    string message = reader.ReadString();
                                    Logging.error(string.Format("Disconnected with message: {0}", message));
                                }
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