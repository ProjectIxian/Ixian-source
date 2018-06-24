using DLT;
using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace S2.Network
{
    public struct NetOpsData
    {
        public IPEndPoint listenAddress;
    }

    public class StreamClient
    {
        public IPEndPoint remoteIP;
        public Socket clientSocket;
        public Thread thread;

        public Presence presence = null;
        public PresenceAddress presenceAddress = null;
    }

    public class NetworkStreamServer
    {
        public static string publicIPAddress = "127.0.0.1";

        // Send a keepalive packet 10 seconds after the last communication
        private static UInt32 keepalive_check_interval = 10000;

        private static bool continueRunning;
        private static Thread netControllerThread;
        private static TcpListener listener;
        private static List<StreamClient> connectedClients = new List<StreamClient>();

        private static Thread pingThread;

        static NetworkStreamServer()
        {
        }

        private NetworkStreamServer()
        {
        }

        public static void beginNetworkOperations()
        {
            if (netControllerThread != null)
            {
                // already running
                return;
            }
            pingThread = new Thread(pingLoop);
            pingThread.Start();

            netControllerThread = new Thread(networkOpsLoop);
            connectedClients = new List<StreamClient>();
            continueRunning = true;

            // Read the server port from the configuration
            NetOpsData nod = new NetOpsData();
            nod.listenAddress = new IPEndPoint(IPAddress.Any, Config.serverPort);
            netControllerThread.Start(nod);

            // Retrieve the public-accessible IP address
            publicIPAddress = Config.publicServerIP; //CoreNetworkUtils.GetLocalIPAddress();
            Logging.info(string.Format("Public S2 node address: {0} port {1}", publicIPAddress, Config.serverPort));
        }

        public static void stopNetworkOperations()
        {
            if (netControllerThread == null)
            {
                // not running
                return;
            }
            continueRunning = false;

            // Clear all neighbors
            lock (connectedClients)
            {
                // Immediately close all connected client sockets
                foreach (StreamClient client in connectedClients)
                {
                    //client.state = RemoteEndpointState.Closed;
                }
            }
        }

        // Restart the network server
        public static void restartNetworkOperations()
        {
            Logging.info("Stopping stream server...");
            stopNetworkOperations();
            Thread.Sleep(100);
            Logging.info("Restarting stream server...");
            beginNetworkOperations();
        }

        private static void networkOpsLoop(object data)
        {
            if (data is NetOpsData)
            {
                NetOpsData netOpsData = (NetOpsData)data;
                listener = new TcpListener(netOpsData.listenAddress);
                listener.Start();
            }
            else
            {
                throw new Exception(String.Format("NetworkServer.networkOpsLoop called with incorrect data object. Expected 'NetOpsData', got '{0}'", data.GetType().ToString()));
            }
            // housekeeping tasks
            while (continueRunning)
            {
                // check for new incoming connections
                if (listener.Pending())
                {
                    try
                    {
                        listener.BeginAcceptSocket(new AsyncCallback(doAcceptConnection), null);
                    }
                    catch (SocketException socketException)
                    {
                        // log error while accepting connection, then simply drop it
                        Logging.warn("Error while accepting stream client connection: " + socketException.Message);
                    }
                }
                else
                {
                    Thread.Sleep(100);
                }
            }
            Thread.Yield();
        }

        // Unified pinging thread for all connected stream clients
        private static void pingLoop()
        {
            
            // Only ping while networkops loop is active
            while(continueRunning)
            {
                // Wait x seconds before next ping round
                for (int i = 0; i < Config.keepAliveSecondsInterval; i++)
                {
                    if (continueRunning == false)
                    {
                        Thread.Yield();
                        return;
                    }
                    // Sleep for one second
                    Thread.Sleep(1000);
                }

                // Prepare the keepalive message
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {
                        try
                        {
                            string publicHostname = string.Format("{0}:{1}", NetworkStreamServer.publicIPAddress, Config.serverPort);
                            string wallet = Node.walletStorage.address;
                            writer.Write(wallet);
                            writer.Write(Config.device_id);
                            writer.Write(publicHostname);

                            // Add the unix timestamp
                            string timestamp = Clock.getTimestamp(DateTime.Now);
                            writer.Write(timestamp);

                            // Add a verifiable signature
                            string private_key = Node.walletStorage.privateKey;
                            string signature = CryptoManager.lib.getSignature(timestamp, private_key);
                            writer.Write(signature);
                        }
                        catch (Exception)
                        {
                            continue;

                        }
                        lock (connectedClients)
                        {
                            foreach (StreamClient client in connectedClients)
                            {
                                sendPing(client, m.ToArray());
                            }
                        }
                    }
                }

            }
            Thread.Yield();
        }

        private static void doAcceptConnection(IAsyncResult ar)
        {
            Socket clientSocket = listener.EndAcceptSocket(ar);
            IPEndPoint clientEndpoint = (IPEndPoint)clientSocket.RemoteEndPoint;
            Logging.info(String.Format("Stream client connection accepted: {0}", clientEndpoint.ToString()));
            lock (connectedClients)
            {
                var existing_clients = connectedClients.Where(re => re.remoteIP.Address == clientEndpoint.Address);
                if (existing_clients.Count() > 0)
                {
                    Logging.warn(String.Format("Stream client {0}:{1} already connected as {2}:{3}.",
                        clientEndpoint.Address.ToString(), clientEndpoint.Port, existing_clients.First().ToString()));
                }

                // Setup socket keepalive mechanism
                int size = sizeof(UInt32);
                UInt32 on = 1;
                UInt32 keepAliveInterval = keepalive_check_interval; // send a packet once every x seconds.
                UInt32 retryInterval = 1000; // if no response, resend every second.
                byte[] inArray = new byte[size * 3];
                Array.Copy(BitConverter.GetBytes(on), 0, inArray, 0, size);
                Array.Copy(BitConverter.GetBytes(keepAliveInterval), 0, inArray, size, size);
                Array.Copy(BitConverter.GetBytes(retryInterval), 0, inArray, size * 2, size);
                clientSocket.IOControl(IOControlCode.KeepAliveValues, inArray, null);

                StreamClient streamClient = new StreamClient();
                streamClient.remoteIP = clientEndpoint;
                streamClient.clientSocket = clientSocket;
                streamClient.presence = null;
                streamClient.presenceAddress = null;
                streamClient.thread = new Thread(streamClientLoop);
                streamClient.thread.Start(streamClient);

                connectedClients.Add(streamClient);

            }
        }

        // Forwards a network message to a specific presense address if it's in the client list
        public static bool forwardMessage(string address, ProtocolMessageCode code, byte[] message)
        {
            Console.WriteLine(">>>> Preparing to forward to {0}", address);

            lock (connectedClients)
            {
                foreach (StreamClient client in connectedClients)
                {
                    // Skip connections without presence information
                    if (client.presence == null)
                        continue;

                    string client_wallet = client.presence.wallet;

                    if(address.Equals(client_wallet, StringComparison.Ordinal))
                    {
                        Console.WriteLine(">>>> Sending message");
                        sendData(client, code, message);
                    }

                }
            }

            // TODO: broadcast to network if no connect clients found

            return false;
        }


        // Send data to all connected clients
        public static void broadcastData(ProtocolMessageCode code, byte[] data)
        {
            lock (connectedClients)
            {
                foreach (StreamClient endpoint in connectedClients)
                {
                    // TODO: filter messages based on presence node type

                    byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
                    try
                    {
                        endpoint.clientSocket.Send(ba, SocketFlags.None);
                    }
                    catch (Exception)
                    {
                        // Report any issues related to sockets
                        // Console.WriteLine("SRV: Socket exception for {0}. Info: {1}", endpoint.remoteIP, e.ToString());
                    }
                }
            }
        }

        // Sends data to a stream client
        public static void sendData(StreamClient client, ProtocolMessageCode code, byte[] data)
        {
            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
            try
            {
                client.clientSocket.Send(ba, SocketFlags.None);
            }
            catch (Exception e)
            {
                Console.WriteLine("SRV: Socket send exception, skipping: {0}", e.ToString());
            }
        }

        // Send a ping packet to verify the connection status
        private static void sendPing(StreamClient client, byte[] data)
        {
            if (client == null)
                return;

            // TODO: find a better way to detect near-instant disconnects
            byte[] tmp = new byte[1];
            client.clientSocket.Blocking = true;
            byte[] ba = ProtocolMessage.prepareProtocolMessage(ProtocolMessageCode.keepAlivePresence, data);

            try
            {
                client.clientSocket.Send(ba, SocketFlags.None);
                if (client.clientSocket.Connected == false)
                {
                    //Console.WriteLine("!!! Failed to ping streamclient.");
                }
            }
            catch (Exception)
            {
                //Console.WriteLine("Failed ping to stream client: {0}", e.ToString());
            }
        }

        private static void streamClientLoop(object data)
        {
            StreamClient client = null;
            if (data is StreamClient)
            {
                client = (StreamClient)data;
            }
            else
            {
                throw new Exception(String.Format("NetworkServer.clientLoop called with incorrect data object. Expected 'RemoteEndpoint', got '{0}'", data.GetType().ToString()));
            }

            bool clientActive = true;
            while (clientActive)
            {
                // Check if the socket is active
                {
                    // Let the protocol handler receive and handle messages
                    try
                    {
                        ProtocolMessage.readProtocolMessage(client.clientSocket, client);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Disconnected stream client: {0}", e.ToString());
                        clientActive = false;
                    }
                }
            }

            // Remove corresponding address from presence list
            if (client.presence != null && client.presenceAddress != null)
            {
                PresenceList.removeAddressEntry(client.presence.wallet, client.presenceAddress);
            }
            

            // Close the client socket
            if (client != null && client.clientSocket != null)
            {
                try
                {
                    client.clientSocket.Shutdown(SocketShutdown.Both);
                    client.clientSocket.Close();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Could not shutdown client socket: {0}", e.ToString());
                }
            }

            lock (connectedClients)
            {
                connectedClients.Remove(client);
            }

            Thread.Yield();
        }


    }
}
