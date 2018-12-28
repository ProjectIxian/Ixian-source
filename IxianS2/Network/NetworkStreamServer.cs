using DLT;
using DLT.Meta;
using DLT.Network;
using IXICore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

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


        private static bool continueRunning;
        private static Thread netControllerThread;
        private static TcpListener listener;
        private static List<RemoteEndpoint> connectedClients = new List<RemoteEndpoint>();

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

            netControllerThread = new Thread(networkOpsLoop);
            connectedClients = new List<RemoteEndpoint>();
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
                Logging.info("Network server thread was already halted.");
                return;
            }
            continueRunning = false;

            // Close blocking socket
            listener.Stop();

            // Clear all neighbors
            lock (connectedClients)
            {
                // Immediately close all connected client sockets
                foreach (RemoteEndpoint client in connectedClients)
                {
                    client.stop();
                }

                connectedClients.Clear();
            }

            netControllerThread.Abort();
            netControllerThread = null;
        }

        public static void handleDisconnectedClients()
        {
            List<RemoteEndpoint> netClients = null;
            lock (connectedClients)
            {
                netClients = new List<RemoteEndpoint>(connectedClients);
            }

            // Prepare a list of failed clients
            List<RemoteEndpoint> failed_clients = new List<RemoteEndpoint>();

            foreach (RemoteEndpoint client in netClients)
            {
                if (client.isConnected())
                {
                    continue;
                }
                failed_clients.Add(client);
            }

            // Go through the list of failed clients and remove them
            foreach (RemoteEndpoint client in failed_clients)
            {
                client.stop();
                lock (connectedClients)
                {
                    // Remove this endpoint from the network server
                    connectedClients.Remove(client);
                }
            }
        }

        // Restart the network server
        public static void restartNetworkOperations()
        {
            Logging.info("Stopping stream server...");
            stopNetworkOperations();
            Thread.Sleep(1000);
            Logging.info("Restarting stream server...");
            beginNetworkOperations();
        }

        private static void networkOpsLoop(object data)
        {
            if (data is NetOpsData)
            {
                try
                {
                    NetOpsData netOpsData = (NetOpsData)data;
                    listener = new TcpListener(netOpsData.listenAddress);
                    listener.Start();
                }
                catch (Exception e)
                {
                    Logging.error(string.Format("Exception starting stream server: {0}", e.ToString()));
                    return;
                }
            }
            else
            {
                throw new Exception(String.Format("NetworkStreamServer.networkOpsLoop called with incorrect data object. Expected 'NetOpsData', got '{0}'", data.GetType().ToString()));
            }
            // housekeeping tasks
            while (continueRunning)
            {
                handleDisconnectedClients();
                if (connectedClients.Count < Config.maximumStreamClients)
                {
                    // Use a blocking mechanism
                    try
                    {
                        Socket handlerSocket = listener.AcceptSocket();
                        acceptConnection(handlerSocket);
                    }
                    catch (SocketException)
                    {
                        // Could be an interupt request
                    }
                }

                // Sleep to prevent cpu usage
                Thread.Sleep(100);
            }
            Logging.info("Stream server listener thread ended.");
            Thread.Yield();
        }

        private static void acceptConnection(Socket clientSocket)
        {
            IPEndPoint clientEndpoint = (IPEndPoint)clientSocket.RemoteEndPoint;
            // Add timeouts and set socket options
            //clientSocket.ReceiveTimeout = 5000;
            //clientSocket.SendTimeout = 5000;
            clientSocket.LingerState = new LingerOption(true, 3);
            clientSocket.NoDelay = true;
            clientSocket.Blocking = true;

            // Setup the remote endpoint
            RemoteEndpoint remoteEndpoint = new RemoteEndpoint();

            lock (connectedClients)
            {
                if (connectedClients.Count + 1 > Config.maximumStreamClients)
                {
                    Logging.warn(string.Format("Maximum number of connected clients reached. Disconnecting client: {0}:{1}",
                        clientEndpoint.Address.ToString(), clientEndpoint.Port));
                    clientSocket.Disconnect(true);
                    clientSocket.Shutdown(SocketShutdown.Both);
                    return;
                }

                var existing_clients = connectedClients.Where(re => re.remoteIP.Address == clientEndpoint.Address);
                if (existing_clients.Count() > 0)
                {
                    Logging.warn(String.Format("Client {0}:{1} already connected as {2}.",
                        clientEndpoint.Address.ToString(), clientEndpoint.Port, existing_clients.First().ToString()));
                    // TODO: handle this situation (client already connected)
                }

                connectedClients.Add(remoteEndpoint);
            }

            Logging.info(String.Format("Client connection accepted: {0} | #{1}/{2}", clientEndpoint.ToString(), connectedClients.Count + 1, CoreConfig.maximumServerMasterNodes));

            remoteEndpoint.start(clientSocket);
        }

        // Removes an endpoint from the connected clients list
        public static bool removeEndpoint(RemoteEndpoint endpoint)
        {
            bool result = false;
            lock (connectedClients)
            {
                result = connectedClients.Remove(endpoint);
            }
            return result;
        }

        public static int getQueuedMessageCount()
        {
            int messageCount = 0;
            lock (connectedClients)
            {
                foreach (RemoteEndpoint client in connectedClients)
                {
                    messageCount += client.getQueuedMessageCount();
                }
            }
            return messageCount;
        }

        public static RemoteEndpoint getClient(int idx)
        {
            lock (connectedClients)
            {
                int i = 0;
                RemoteEndpoint lastClient = null;
                foreach (RemoteEndpoint client in connectedClients)
                {
                    if (client.isConnected())
                    {
                        lastClient = client;
                    }
                    if (i == idx && lastClient != null)
                    {
                        break;
                    }
                    i++;
                }
                return lastClient;
            }
        }

        // Forwards a network message to a specific presense address if it's in the client list
        public static bool forwardMessage(byte[] address, ProtocolMessageCode code, byte[] message)
        {
            if(address == null)
            {
                Logging.warn("Cannot forward message to null address.");
                return false;
            }

            Logging.info(String.Format(">>>> Preparing to forward to {0}", 
                Base58Check.Base58CheckEncoding.EncodePlain(address)));

            lock (connectedClients)
            {
                foreach (RemoteEndpoint endpoint in connectedClients)
                {
                    // Skip connections without presence information
                    if (endpoint.presence == null)
                        continue;

                    byte[] client_wallet = endpoint.presence.wallet;

                    if(client_wallet != null && address.SequenceEqual(client_wallet))
                    {
                        Logging.info(">>>> Forwarding message");
                        endpoint.sendData(code, message);

                    }

                }
            }

            // TODO: broadcast to network if no connect clients found?

            return false;
        }


        // Send data to all connected clients
        // Returns true if the data was sent to at least one client
        public static bool broadcastData(char[] types, ProtocolMessageCode code, byte[] data, RemoteEndpoint skipEndpoint = null)
        {
            bool result = false;
            lock (connectedClients)
            {
                foreach (RemoteEndpoint endpoint in connectedClients)
                {
                    if (skipEndpoint != null)
                    {
                        if (endpoint == skipEndpoint)
                            continue;
                    }

                    if (!endpoint.isConnected())
                    {
                        continue;
                    }

                    if (endpoint.helloReceived == false)
                    {
                        continue;
                    }

                    if (types != null)
                    {
                        if (endpoint.presenceAddress == null || !types.Contains(endpoint.presenceAddress.type))
                        {
                            continue;
                        }
                    }

                    endpoint.sendData(code, data);
                    result = true;
                }
            }
            return result;
        }

        // Sends event data to all subscribed clients
        public static bool broadcastEventData(ProtocolMessageCode code, byte[] data, byte[] address, RemoteEndpoint skipEndpoint = null)
        {
            bool result = false;
            lock (connectedClients)
            {
                foreach (RemoteEndpoint endpoint in connectedClients)
                {
                    if (skipEndpoint != null)
                    {
                        if (endpoint == skipEndpoint)
                            continue;
                    }

                    if (!endpoint.isConnected())
                    {
                        continue;
                    }

                    if (endpoint.helloReceived == false)
                    {
                        continue;
                    }

                    if (endpoint.presenceAddress == null || endpoint.presenceAddress.type != 'C')
                    {
                        continue;
                    }

                    // Finally, check if the endpoint is subscribed to this event and address
                    if (endpoint.isSubscribedToEvent((int)code, address))
                    {
                        endpoint.sendData(code, data);
                        result = true;
                    }
                }
            }
            return result;
        }

        // Sends data to a stream client
        public static void sendData(StreamClient client, ProtocolMessageCode code, byte[] data)
        {
            byte[] ba = CoreProtocolMessage.prepareProtocolMessage(code, data);
            try
            {
                client.clientSocket.Send(ba, SocketFlags.None);
            }
            catch (Exception e)
            {
                Logging.warn(String.Format("SRV: Socket send exception, skipping: {0}", e.ToString()));
            }
        }
        
    }
}
