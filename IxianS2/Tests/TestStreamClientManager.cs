using DLT;
using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace S2
{
    class TestStreamClientManager
    {
        private static List<TestStreamClient> streamClients = new List<TestStreamClient>();
        private static List<string> connectingClients = new List<string>(); // A list of clients that we're currently connecting

        private static Thread reconnectThread;
        private static bool autoReconnect = true;


        public static void start()
        {
            streamClients = new List<TestStreamClient>();

            if (Config.testS2Node.Length < 3)
            {
                Logging.error("Invalid S2 node for Test Client. Use -n or -s2node to specify node.");
                Node.running = false;
                S2.Program.noStart = true;
            }

            connectTo(Config.testS2Node);

            // Start the reconnect thread
            reconnectThread = new Thread(reconnectClients);
            autoReconnect = true;
            reconnectThread.Start();
        }

        public static void stop()
        {
            autoReconnect = false;
            isolate();

            // Force stopping of reconnect thread
            if (reconnectThread == null)
                return;
            reconnectThread.Abort();
            reconnectThread = null;
        }

        // Immediately disconnects all clients
        public static void isolate()
        {
            Logging.info("Isolating stream clients...");

            lock (streamClients)
            {
                // Disconnect each client
                foreach (TestStreamClient client in streamClients)
                {
                    client.stop();
                }

                // Empty the client list
                streamClients.Clear();
            }
        }

        public static void restartClients()
        {
            Logging.info("Stopping stream clients...");
            stop();
            Thread.Sleep(100);
            Logging.info("Starting stream clients...");
            start();
        }

        private static void reconnectClients()
        {

        }

        // Connects to a specified node, with the syntax host:port
        public static bool connectTo(string host)
        {
            Logging.info(String.Format("Connecting to S2 node: {0}", host));

            if (host == null || host.Length < 3)
            {
                Logging.error(String.Format("Invalid host address {0}", host));
                return false;
            }

            string[] server = host.Split(':');
            if (server.Count() < 2)
            {
                Logging.warn(string.Format("Cannot connect to invalid hostname: {0}", host));
                return false;
            }

            // Resolve the hostname first
            string resolved_server_name = NetworkUtils.resolveHostname(server[0]);

            // Skip hostnames we can't resolve
            if (resolved_server_name.Length < 1)
            {
                Logging.warn(string.Format("Cannot resolve IP for {0}, skipping connection.", server[0]));
                return false;
            }

            string resolved_host = string.Format("{0}:{1}", resolved_server_name, server[1]);

            // Verify against the publicly disclosed ip
            // Don't connect to self
            if (resolved_server_name.Equals(Config.publicServerIP, StringComparison.Ordinal))
            {
                if (server[1].Equals(string.Format("{0}", Config.serverPort), StringComparison.Ordinal))
                {
                    Logging.info(string.Format("Skipping connection to public self seed node {0}", host));
                    return false;
                }
            }

            // Get all self addresses and run through them
            List<string> self_addresses = CoreNetworkUtils.GetAllLocalIPAddresses();
            foreach (string self_address in self_addresses)
            {
                // Don't connect to self
                if (resolved_server_name.Equals(self_address, StringComparison.Ordinal))
                {
                    if (server[1].Equals(string.Format("{0}", Config.serverPort), StringComparison.Ordinal))
                    {
                        Logging.info(string.Format("Skipping connection to self seed node {0}", host));
                        return false;
                    }
                }
            }

            lock (connectingClients)
            {
                foreach (string client in connectingClients)
                {
                    if (resolved_host.Equals(client, StringComparison.Ordinal))
                    {
                        // We're already connecting to this client
                        return false;
                    }
                }

                // The the client to the connecting clients list
                connectingClients.Add(resolved_host);
            }

            // Check if node is already in the client list
            lock (streamClients)
            {
                foreach (TestStreamClient client in streamClients)
                {
                    if (client.getFullAddress(true).Equals(resolved_host, StringComparison.Ordinal))
                    {
                        // Address is already in the client list
                        return false;
                    }
                }
            }


            // Connect to the specified node
            TestStreamClient new_client = new TestStreamClient();
            // Recompose the connection address from the resolved IP and the original port
            bool result = new_client.connectToServer(resolved_server_name, Convert.ToInt32(server[1]));

            // Add this node to the client list if connection was successfull
            if (result == true)
            {
                // Add this node to the client list
                lock (streamClients)
                {
                    streamClients.Add(new_client);
                }

            }

            // Remove this node from the connecting clients list
            lock (connectingClients)
            {
                connectingClients.Remove(resolved_host);
            }

            return result;
        }
    }
}
