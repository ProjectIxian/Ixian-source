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

        static public bool initWallet()
        {
            walletStorage = new WalletStorage(Config.walletFile);

            Logging.flush();

            if (!walletStorage.walletExists())
            {
                ConsoleHelpers.displayBackupText();

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
                    password = ConsoleHelpers.requestNewPassword("Enter a password for your new wallet: ");
                    if (forceShutdown)
                    {
                        return false;
                    }
                }
                walletStorage.generateWallet(password);
            }
            else
            {
                ConsoleHelpers.displayBackupText();

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
                        password = ConsoleHelpers.getPasswordInput();
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
                    new_password = ConsoleHelpers.requestNewPassword("Enter a new password for your wallet: ");
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

        static public void start(bool verboseConsoleOutput)
        {
            // Network configuration
            NetworkUtils.configureNetwork();

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
            Program.noStart = true;
            forceShutdown = true;
            ConsoleHelpers.forceShutdown = true;

            // Stop the keepalive thread
            PresenceList.stopKeepAlive();

            // Stop the API server
            if (apiServer != null)
            {
                apiServer.stop();
                apiServer = null;
            }

            if (maintenanceThread != null)
            {
                maintenanceThread.Abort();
                maintenanceThread = null;
            }

            ActivityStorage.stopStorage();

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
