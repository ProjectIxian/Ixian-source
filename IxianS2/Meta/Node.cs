using DLT.Network;
using S2.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT.Meta
{
    class Node
    {
        public static WalletStorage walletStorage;

        static public void start()
        {
            // Load or Generate the wallet
            walletStorage = new WalletStorage(Config.walletFile);

            // Show the IP selector menu
            showIPmenu();

            // Start the node stream server
            NetworkStreamServer.beginNetworkOperations();

            // Start the network client manager
            NetworkClientManager.startClients();

        }

        static public void update()
        {

        }

        static public void stop()
        {
            NetworkClientManager.stopClients();
            // Stop the network server
            NetworkStreamServer.stopNetworkOperations();
        }

        static public void reconnect()
        {
            // Reconnect server and clients
            NetworkStreamServer.restartNetworkOperations();
            NetworkClientManager.restartClients();
        }

        // Shows an IP selector menu
        static public void showIPmenu()
        {
            Console.WriteLine("This node needs to be reachable from the internet. Please select a valid IP address.");
            Console.WriteLine();

            List<string> ips = CoreNetworkUtils.GetAllLocalIPAddresses();

            uint counter = 0;
            foreach (string ip in ips)
            {
                Console.WriteLine("\t{0}) {1}", counter, ip);
                counter++;
            }
            Console.WriteLine("\tM) Manual Entry");
            Console.WriteLine();

            Console.Write("Choose option [default 0]: ");

            int option = 0;
            try
            {
                string result = Console.ReadLine();
                if (result.Equals("m", StringComparison.OrdinalIgnoreCase))
                {
                    option = -1;
                }
                else
                {
                    option = Convert.ToInt32(result);
                }
            }
            catch (Exception)
            {
                // Handle exceptions
                option = 0;
            }

            if (option == -1)
            {
                Console.Write("Type Manual IP: ");
                string chosenIP = Console.ReadLine();
                if (chosenIP != null && chosenIP.Length < 255)
                {
                    Config.publicServerIP = chosenIP;
                    Console.WriteLine("Using option M) {0} as the default external IP for this node.", chosenIP);
                }
            }
            else
            {
                if (option > ips.Count || option < 0)
                    option = 0;

                string chosenIP = ips[option];
                Config.publicServerIP = chosenIP;
                Console.WriteLine("Using option {0}) {1} as the default external IP for this node.", option, chosenIP);
            }
        }
    }
}
