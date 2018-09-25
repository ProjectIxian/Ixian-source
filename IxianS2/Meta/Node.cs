using DLT.Network;
using S2.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DLT.Meta
{
    class Node
    {
        public static WalletStorage walletStorage;

        public static UPnP upnp;


        static public void start()
        {
            // Load or Generate the wallet
            walletStorage = new WalletStorage(Config.walletFile);

            // Network configuration
            upnp = new UPnP();
            if (Config.externalIp != "" && IPAddress.TryParse(Config.externalIp, out _))
            {
                Config.publicServerIP = Config.externalIp;
            }
            else
            {
                Config.publicServerIP = "";
                List<IPAndMask> local_ips = CoreNetworkUtils.GetAllLocalIPAddressesAndMasks();
                foreach (IPAndMask local_ip in local_ips)
                {
                    if (IPv4Subnet.IsPublicIP(local_ip.Address))
                    {
                        Logging.info(String.Format("Public IP detected: {0}, mask {1}.", local_ip.Address.ToString(), local_ip.SubnetMask.ToString()));
                        Config.publicServerIP = local_ip.Address.ToString();
                    }
                }
                if (Config.publicServerIP == "")
                {
                    IPAddress primary_local = CoreNetworkUtils.GetPrimaryIPAddress();
                    if (primary_local == null)
                    {
                        Logging.warn("Unable to determine primary IP address.");
                        showIPmenu();
                    }
                    else
                    {
                        Logging.info(String.Format("None of the locally configured IP addresses are public. Attempting UPnP..."));
                        IPAddress public_ip = upnp.GetExternalIPAddress();
                        if (public_ip == null)
                        {
                            Logging.info("UPnP failed.");
                            showIPmenu();
                        }
                        else
                        {
                            Logging.info(String.Format("UPNP-determined public IP: {0}. Attempting to configure a port-forwarding rule.", public_ip.ToString()));
                            if (upnp.MapPublicPort(Config.serverPort, primary_local))
                            {
                                Config.publicServerIP = upnp.getMappedIP();
                                Logging.info(string.Format("Network configured. Public IP is: {0}", Config.publicServerIP));
                            }
                            else
                            {
                                Logging.info("UPnP configuration failed.");
                                // Show the IP selector menu
                                showIPmenu();
                            }
                        }
                    }
                }
            }


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
                showManualIPEntry();
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

        static public void showManualIPEntry()
        {
            Console.Write("Type Manual IP: ");
            string chosenIP = Console.ReadLine();

            // Validate the IP
            if (chosenIP.Length > 255 || validateIPv4(chosenIP) == false)
            {
                Console.WriteLine("Incorrect IP. Please try again.");
                showManualIPEntry();
                return;
            }

            Config.publicServerIP = chosenIP;
            Console.WriteLine("Using option M) {0} as the default external IP for this node.", chosenIP);
        }

        // Helper for validating IPv4 addresses
        static private bool validateIPv4(string ipString)
        {
            if (String.IsNullOrWhiteSpace(ipString))
            {
                return false;
            }

            string[] splitValues = ipString.Split('.');
            if (splitValues.Length != 4)
            {
                return false;
            }

            byte tempForParsing;
            return splitValues.All(r => byte.TryParse(r, out tempForParsing));
        }
    }
}
