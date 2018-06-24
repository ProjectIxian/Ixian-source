using DLT;
using DLT.Meta;
using DLT.Network;
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
        // Prepare a network protocol message. Works for both client-side and server-side
        public static byte[] prepareProtocolMessage(ProtocolMessageCode code, byte[] data)
        {
            byte[] result = null;

            // Prepare the protocol sections
            int data_length = data.Length;
            byte[] data_checksum = Crypto.sha256(data);

            using (MemoryStream m = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(m))
                {
                    // Protocol sections are code, length, checksum, data
                    // Write each section in binary, in that specific order
                    writer.Write((int)code);
                    writer.Write(data_length);
                    writer.Write(data_checksum);
                    writer.Write(data);
                }
                result = m.ToArray();
            }

            return result;
        }

        // Broadcast a protocol message across clients and nodes
        public static void broadcastProtocolMessage(ProtocolMessageCode code, byte[] data, Socket skipSocket = null)
        {
            if (data == null)
            {
                Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                return;
            }

            NetworkClientManager.broadcastData(code, data);
            NetworkStreamServer.broadcastData(code, data);
        }

        // Server-side protocol reading
        public static void readProtocolMessage(Socket socket, StreamClient client)
        {
            // Check for socket availability
            if (socket.Connected == false)
            {
                throw new Exception("Socket already disconnected at other end");
            }

            if (socket.Available < 1)
            {
                // Sleep a while to prevent cpu cycle waste
                Thread.Sleep(100);
                return;
            }
            // Read multi-packet messages
            // TODO: optimize this as it's not very efficient
            var big_buffer = new List<byte>();

            try
            {
                while (socket.Available > 0)
                {
                    var current_byte = new Byte[1];
                    var byteCounter = socket.Receive(current_byte, current_byte.Length, SocketFlags.None);

                    if (byteCounter.Equals(1))
                    {
                        big_buffer.Add(current_byte[0]);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("NET: endpoint disconnected " + e);
                throw e;
            }

            byte[] recv_buffer = big_buffer.ToArray();

            ProtocolMessageCode code = ProtocolMessageCode.hello;
            byte[] data = null;

            using (MemoryStream m = new MemoryStream(recv_buffer))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    // Check for multi-message packets. One packet can contain multiple network messages.
                    while (reader.BaseStream.Position < reader.BaseStream.Length)
                    {
                        int message_code = reader.ReadInt32();
                        code = (ProtocolMessageCode)message_code;

                        int data_length = reader.ReadInt32();
                        if (data_length < 0)
                            return;

                        // If this is a connected client
                        if (client != null)
                        {
                            // Check for presence and only accept hello messages if there is no presence.
                            if (code != ProtocolMessageCode.hello && client.presence == null)
                            {
                                return;
                            }
                        }

                        byte[] data_checksum;

                        try
                        {
                            data_checksum = reader.ReadBytes(32); // sha256, 8 bits per byte
                            data = reader.ReadBytes(data_length);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("NET: dropped packet. " + e);
                            return;
                        }
                        // Compute checksum of received data
                        byte[] local_checksum = Crypto.sha256(data);

                        // Verify the checksum before proceeding
                        if (Crypto.byteArrayCompare(local_checksum, data_checksum) == false)
                        {
                            Logging.warn("Dropped message (invalid checksum)");
                            continue;
                        }

                        // For development purposes, output the proper protocol message
                        Console.WriteLine(string.Format("S2NET: {0} | {1} | {2}", code, data_length, Crypto.hashToString(data_checksum)));

                        // Can proceed to parse the data parameter based on the protocol message code.
                        // Data can contain multiple elements.
                        parseProtocolMessage(code, data, socket, client);
                    }
                }
            }




        }

        // Unified protocol message parsing
        public static void parseProtocolMessage(ProtocolMessageCode code, byte[] data, Socket socket, StreamClient client)
        {
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
                                    // Check for hello messages that don't originate from Clients
                                    if (client == null)
                                    {
                                        return;
                                    }

                                    // Node already has a presence
                                    if (client != null && client.presence != null)
                                    {
                                        // Ignore the hello message in this case
                                        return;
                                    }

                                    string hostname = reader.ReadString();
                                    Console.WriteLine("Received IP: {0}", hostname);

                                    // Another layer to catch any incompatible node exceptions for the hello message
                                    try
                                    {
                                        string addr = reader.ReadString();
                                        char node_type = reader.ReadChar();
                                        string device_id = reader.ReadString();
                                        string pubkey = reader.ReadString();

                                        // Read the metadata and provide backward compatibility with older nodes
                                        string meta = " ";
                                        try
                                        {
                                            meta = reader.ReadString();
                                        }
                                        catch(Exception)
                                        {

                                        }

                                        Console.WriteLine("Received Address: {0} of type {1}", addr, node_type);

                                        if (node_type == 'C')
                                        {
                                            string publicHostname = string.Format("{0}:{1}", NetworkStreamServer.publicIPAddress, Config.serverPort);

                                            // If it's a client, set the hostname to this relay's network address
                                            hostname = publicHostname;
                                        }

                                        // Store the presence address for this remote endpoint
                                        client.presenceAddress = new PresenceAddress(device_id, hostname, node_type);

                                        // Create a temporary presence with the client's address and device id
                                        Presence presence = new Presence(addr, pubkey, meta, client.presenceAddress);

                                        
                                        // Retrieve the final presence entry from the list (or create a fresh one)
                                        client.presence = PresenceList.updateEntry(presence);

                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine("Non compliant node connected. {0}", e.ToString());
                                    }


                                    using (MemoryStream mw = new MemoryStream())
                                    {
                                        using (BinaryWriter writer = new BinaryWriter(mw))
                                        {
                                            // Send the node version
                                            writer.Write(Config.nodeVersion);

                                            byte[] ba = prepareProtocolMessage(ProtocolMessageCode.helloData, mw.ToArray());
                                            socket.Send(ba, SocketFlags.None);
                                        }
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
                                int node_version = reader.ReadInt32();

                                // Check for incompatible nodes
                                if (node_version < Config.nodeVersion)
                                {
                                    Console.WriteLine("Hello: Connected node version ({0}) is too old! Upgrade the node.", node_version);
                                    socket.Disconnect(true);
                                    return;
                                }

                                Console.WriteLine("Connected version : {0}", node_version);

                                // Get presences
                                socket.Send(prepareProtocolMessage(ProtocolMessageCode.syncPresenceList, new byte[1]), SocketFlags.None);
                            }
                        }
                        break;

                    case ProtocolMessageCode.s2data:
                        {
                            StreamProcessor.receiveData(data, socket, client);
                        }
                        break;

                    case ProtocolMessageCode.s2prepareSend:
                        {
                            StreamProcessor.prepareSend(data, socket, client);
                        }
                        break;

                    case ProtocolMessageCode.s2generateKeys:
                        {
                            StreamProcessor.generateKeys(data, socket, client);
                        }
                        break;

                    case ProtocolMessageCode.newTransaction:
                        {
                            // Forward the new transaction message to the DLT network
                            broadcastProtocolMessage(ProtocolMessageCode.newTransaction, data);
                        }
                        break;

                    case ProtocolMessageCode.updateTransaction:
                        {
                            // Forward the update transaction message to the DLT network
                            broadcastProtocolMessage(ProtocolMessageCode.updateTransaction, data);
                        }
                        break;

                    case ProtocolMessageCode.syncPresenceList:
                        {
                            byte[] pdata = PresenceList.getBytes();
                            byte[] ba = prepareProtocolMessage(ProtocolMessageCode.presenceList, pdata);
                            socket.Send(ba, SocketFlags.None);
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

                    case ProtocolMessageCode.removePresence:
                        {
                            // Parse the data and remove the entry from the presence list
                            Presence presence = new Presence(data);
                            if(presence.wallet.Equals(Node.walletStorage.address, StringComparison.Ordinal))
                            {
                                Console.WriteLine("[PL] Received removal of self from PL, ignoring.");
                                return;
                            }
                            PresenceList.removeEntry(presence);
                        }
                        break;

                    case ProtocolMessageCode.keepAlivePresence:
                        {
                            bool updated = PresenceList.receiveKeepAlive(data);
                            // If a presence entry was updated, broadcast this message again
                            if (updated)
                            {
                                broadcastProtocolMessage(ProtocolMessageCode.keepAlivePresence, data, socket);
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