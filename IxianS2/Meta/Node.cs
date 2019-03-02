using DLT.Network;
using IXICore.Utils;
using S2;
using S2.Network;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT.Meta
{
    class Node
    {
        // Public
        public static WalletStorage walletStorage;
        public static WalletState walletState;

        public static APIServer apiServer;

        public static UPnP upnp;

        public static StatsConsoleScreen statsConsoleScreen = null;


        public static IxiNumber balance = 0;      // Stores the last known balance for this node
        public static ulong blockHeight = 0;

        public static bool forceShutdown = false;

        // Private data
        static Block lastBlock = null;
        static int requiredConsensus = 0;

        private static Thread maintenanceThread;

        public static bool running = false;

        // Perform basic initialization of node
        static public void init()
        {


            running = true;

            // Load or Generate the wallet
            if (!initWallet())
            {
                running = false;
                S2.Program.noStart = true;
                return;
            }

            // Setup the stats console
            statsConsoleScreen = new StatsConsoleScreen();

            // Initialize the wallet state
            walletState = new WalletState();
        }

        static private void displayBackupText()
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("");
            Console.WriteLine("!! Always remember to keep a backup of your ixian.wal file and your password.");
            Console.WriteLine("!! In case of a lost file you will not be able to access your funds.");
            Console.WriteLine("!! Never give your ixian.wal and/or password to anyone.");
            Console.WriteLine("");
            Console.ResetColor();
        }

        // Requests the user to type a new password
        static private string requestNewPassword(string banner)
        {
            Console.WriteLine();
            Console.Write(banner);
            try
            {
                string pass = getPasswordInput();

                if (pass.Length < 10)
                {
                    Console.WriteLine("Password needs to be at least 10 characters. Try again.");
                    return "";
                }

                Console.Write("Type it again to confirm: ");

                string passconfirm = getPasswordInput();

                if (pass.Equals(passconfirm, StringComparison.Ordinal))
                {
                    return pass;
                }
                else
                {
                    Console.WriteLine("Passwords don't match, try again.");

                    // Passwords don't match
                    return "";
                }

            }
            catch (Exception)
            {
                // Handle exceptions
                return "";
            }
        }

        // Handles console password input
        static public string getPasswordInput()
        {
            StringBuilder sb = new StringBuilder();
            while (true)
            {
                if (forceShutdown)
                {
                    return "";
                }

                if (!Console.KeyAvailable)
                {
                    Thread.Yield();
                    continue;
                }

                ConsoleKeyInfo i = Console.ReadKey(true);
                if (i.Key == ConsoleKey.Enter)
                {
                    Console.WriteLine();
                    break;
                }
                else if (i.Key == ConsoleKey.Backspace)
                {
                    if (sb.Length > 0)
                    {
                        sb.Remove(sb.Length - 1, 1);
                        Console.Write("\b \b");
                    }
                }
                else if (i.KeyChar != '\u0000')
                {
                    sb.Append(i.KeyChar);
                    Console.Write("*");
                }
            }
            return sb.ToString();
        }

        static public bool initWallet()
        {
            walletStorage = new WalletStorage(Config.walletFile);

            Logging.flush();

            if (!walletStorage.walletExists())
            {
                displayBackupText();

                // Request a password
                // NOTE: This can only be done in testnet to enable automatic testing!
                string password = "";
                if (Config.dangerCommandlinePasswordCleartextUnsafe != "" && Config.isTestNet)
                {
                    Logging.warn("TestNet detected and wallet password has been specified on the command line!");
                    password = Config.dangerCommandlinePasswordCleartextUnsafe;
                    // Also note that the commandline password still has to be >= 10 characters
                }
                while (password.Length < 10)
                {
                    Logging.flush();
                    password = requestNewPassword("Enter a password for your new wallet: ");
                    if (forceShutdown)
                    {
                        return false;
                    }
                }
                walletStorage.generateWallet(password);
            }
            else
            {
                displayBackupText();

                bool success = false;
                while (!success)
                {

                    // NOTE: This is only permitted on the testnet for dev/testing purposes!
                    string password = "";
                    if (Config.dangerCommandlinePasswordCleartextUnsafe != "" && Config.isTestNet)
                    {
                        Logging.warn("Attempting to unlock the wallet with a password from commandline!");
                        password = Config.dangerCommandlinePasswordCleartextUnsafe;
                    }
                    if (password.Length < 10)
                    {
                        Logging.flush();
                        Console.Write("Enter wallet password: ");
                        password = getPasswordInput();
                    }
                    if (forceShutdown)
                    {
                        return false;
                    }
                    if (walletStorage.readWallet(password))
                    {
                        success = true;
                    }
                }
            }


            if (walletStorage.getPrimaryPublicKey() == null)
            {
                return false;
            }

            // Wait for any pending log messages to be written
            Logging.flush();

            Console.WriteLine();
            Console.WriteLine("Your IXIAN addresses are: ");
            Console.ForegroundColor = ConsoleColor.Green;
            foreach (var entry in walletStorage.getMyAddressesBase58())
            {
                Console.WriteLine(entry);
            }
            Console.ResetColor();
            Console.WriteLine();

            if (Config.onlyShowAddresses)
            {
                return false;
            }

            // Check if we should change the password of the wallet
            if (Config.changePass == true)
            {
                // Request a new password
                string new_password = "";
                while (new_password.Length < 10)
                {
                    new_password = requestNewPassword("Enter a new password for your wallet: ");
                    if (forceShutdown)
                    {
                        return false;
                    }
                }
                walletStorage.writeWallet(new_password);
            }

            Logging.info("Public Node Address: {0}", Base58Check.Base58CheckEncoding.EncodePlain(walletStorage.getPrimaryAddress()));


            return true;
        }

        static private void configureNetwork()
        {
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
                        Logging.warn(String.Format("None of the locally configured IP addresses are public. Attempting UPnP..."));
                        Task<IPAddress> public_ip = upnp.GetExternalIPAddress();
                        if (public_ip.Wait(1000))
                        {
                            if (public_ip.Result != null)
                            {
                                Logging.info(String.Format("UPNP-determined public IP: {0}. Attempting to configure a port-forwarding rule.", public_ip.Result.ToString()));
                                if (upnp.MapPublicPort(Config.serverPort, primary_local))
                                {
                                    Config.publicServerIP = public_ip.Result.ToString(); //upnp.getMappedIP();
                                    Logging.info(string.Format("Network configured. Public IP is: {0}", Config.publicServerIP));
                                }
                            }
                            else
                            {
                                Logging.warn("UPnP configuration failed.");
                                showIPmenu();
                            }
                        }

                    }
                }
            }
        }

        static public void start(bool verboseConsoleOutput)
        {
            // Network configuration
            configureNetwork();

            PresenceList.generatePresenceList(Config.publicServerIP, 'R');

            // Start the network queue
            NetworkQueue.start();

            ActivityStorage.prepareStorage();

            // Start the HTTP JSON API server
            apiServer = new APIServer();

            // Prepare stats screen
            Config.verboseConsoleOutput = verboseConsoleOutput;
            Logging.consoleOutput = verboseConsoleOutput;
            Logging.flush();
            if (Config.verboseConsoleOutput == false)
            {
                statsConsoleScreen.clearScreen();
            }

            // Check for test client mode
            if (Config.isTestClient)
            {
                TestClientNode.start();
                return;
            }

            // Start the node stream server
            NetworkServer.beginNetworkOperations();

            // Start the network client manager
            NetworkClientManager.start();

            // Start the keepalive thread
            PresenceList.startKeepAlive();

            // Start the maintenance thread
            maintenanceThread = new Thread(performMaintenance);
            maintenanceThread.Start();
        }

        static public bool update()
        {
            // Update the stream processor
            StreamProcessor.update();

            // Check for test client mode
            if (Config.isTestClient)
            {
                TestClientNode.update();
            }

            return running;
        }

        static public void stop()
        {
            // Stop the API server
            if (apiServer != null)
            {
                apiServer.stop();
                apiServer = null;
            }

            // Stop the keepalive thread
            PresenceList.stopKeepAlive();

            if (maintenanceThread != null)
            {
                maintenanceThread.Abort();
                maintenanceThread = null;
            }

            // Stop the network queue
            NetworkQueue.stop();

            // Check for test client mode
            if (Config.isTestClient)
            {
                TestClientNode.stop();
                return;
            }

            // Stop all network clients
            NetworkClientManager.stop();

            // Stop the network server
            NetworkServer.stopNetworkOperations();

            ActivityStorage.stopStorage();

            // Stop the console stats screen
            // Console screen has a thread running even if we are in verbose mode
            statsConsoleScreen.stop();
        }

        static public void reconnect()
        {

            // Reset the network receive queue
            NetworkQueue.reset();

            // Check for test client mode
            if (Config.isTestClient)
            {
                TestClientNode.reconnect();
                return;
            }

            // Reconnect server and clients
            NetworkServer.restartNetworkOperations();
            NetworkClientManager.restartClients();
        }

        // Shows an IP selector menu
        static public void showIPmenu()
        {
            return;
            Thread.Sleep(1000); // sleep a bit to allow logging to do it's thing
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
            if (chosenIP.Length > 255 || IxiUtils.validateIPv4(chosenIP) == false)
            {
                Console.WriteLine("Incorrect IP. Please try again.");
                showManualIPEntry();
                return;
            }

            Config.publicServerIP = chosenIP;
            Console.WriteLine("Using option M) {0} as the default external IP for this node.", chosenIP);
        }

        // Cleans the storage cache and logs
        public static bool cleanCacheAndLogs()
        {
            ActivityStorage.deleteCache();

            PeerStorage.deletePeersFile();

            Logging.clear();

            Logging.info("Cleaned cache and logs.");
            return true;
        }

        // Perform periodic cleanup tasks
        private static void performMaintenance()
        {
            while (running)
            {
                // Sleep a while to prevent cpu usage
                Thread.Sleep(1000);

                // Cleanup the presence list
                PresenceList.performCleanup();
            }
        }

        public static string getFullAddress()
        {
            return Config.publicServerIP + ":" + Config.serverPort;
        }

        public static int getLastBlockVersion()
        {
            if (lastBlock != null)
            {
                return lastBlock.version;
            }
            return 0;
        }

        public static char getNodeType()
        {
            return 'R';
        }

        public static bool isAcceptingConnections()
        {
            // TODO TODO TODO TODO implement this properly
            return true;
        }

        public static void setRequiredConsensus(int required_consensus)
        {
            requiredConsensus = required_consensus;
        }

        public static int getRequiredConsensus()
        {
            return requiredConsensus;
        }

        public static void setLastBlock(ulong block_num, byte[] checksum, byte[] ws_checksum, int version)
        {
            Block b = new Block();
            b.blockNum = block_num;
            b.blockChecksum = checksum;
            b.walletStateChecksum = ws_checksum;
            b.version = version;

            lastBlock = b;

            blockHeight = block_num;
        }

        public static Block getLastBlock()
        {
            return lastBlock;
        }

        public static bool isMasterNode()
        {
            return false;
        }
    }
}
