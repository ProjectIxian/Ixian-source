using DLT;
using DLT.Meta;
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

            if(Config.testS2Node.Length < 3)
            {
                Logging.error("Invalid S2 node for Test Client. Use -n or -s2node to specify node.");
                Node.running = false;
                S2.Program.noStart = true;
            }


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


    }
}
