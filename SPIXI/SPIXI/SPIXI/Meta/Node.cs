﻿using DLT.Network;
using SPIXI;
using System;
using System.IO;
using System.Timers;
using Xamarin.Forms;

namespace DLT.Meta
{
    class Node
    {
        public static WalletState walletState;

        // Use the SPIXI-specific wallet storage code
        public static WalletStorage walletStorage;

        // Store the in-memory friendlist here
        public static SPIXI.FriendList friendList;

        // Used for all local data storage
        public static SPIXI.Storage.LocalStorage localStorage;

        // Used to force reloading of some homescreen elements
        public static bool changedSettings = false;

        // Node timer
        private static System.Timers.Timer mainLoopTimer;

        // Private data
        static Block lastBlock = null;
        static int requiredConsensus = 0;


        public static IxiNumber balance = 0;      // Stores the last known balance for this node
        public static ulong blockHeight = 0;

        public static string primaryS2Address = "";

        static public void start()
        {
            // Initialize the crypto manager
            CryptoManager.initLib();

            // Prepare the wallet
            string path = Environment.GetFolderPath(Environment.SpecialFolder.Personal);
            walletStorage = new WalletStorage(Path.Combine(path, Config.walletFile));

            // Initialize the wallet state
            walletState = new WalletState();

            // Prepare the local storage
            localStorage = new SPIXI.Storage.LocalStorage();

            // Read the account file
            localStorage.readAccountFile();

            // Start the network queue
            NetworkQueue.start();

            // Prepare the stream processor
            StreamProcessor.initialize();

            // Start the keepalive thread
            PresenceList.startKeepAlive();

            // Setup a timer to handle routine updates
            mainLoopTimer = new System.Timers.Timer(2500);
            mainLoopTimer.Elapsed += new ElapsedEventHandler(onUpdate);
            mainLoopTimer.Start();
        }


        // Checks for existing wallet file. Can also be used to handle wallet/account upgrading in the future.
        // Returns true if found, otherwise false.
        static public bool checkForExistingWallet()
        {
            if (File.Exists(walletStorage.getFileName()) == false)
            {
                Logging.log(LogSeverity.error, "Cannot read wallet file.");
                return false;
            }

            return true;
        }

        static public bool loadWallet()
        {
            if (Application.Current.Properties.ContainsKey("walletpass") == false)
                return false;

            // TODO: decrypt the password
            string password = Application.Current.Properties["walletpass"].ToString();


            return walletStorage.readWallet(password);
        }

        static public bool generateWallet(string pass)
        {
            return walletStorage.generateWallet(pass);
        }
        

        static public void connectToNetwork()
        {
            // Start the network client manager
            NetworkClientManager.start();
            // TODOSPIXI
            // Start the s2 client manager
            //StreamClientManager.start();
        }

        // Handle timer routines
        static public void onUpdate(object source, ElapsedEventArgs e)
        {
            // Update the friendlist
            FriendList.Update();

            // Cleanup the presence list
            // TODO: optimize this by using a different thread perhaps
            PresenceList.performCleanup();


            if (Node.walletStorage.getPrimaryAddress() == null)
                return;

            if (PresenceList.curNodePresence == null)
            {
                PresenceList.generatePresenceList("spixi:000", 'C'); // TODO TODO TODO TODO spixi:000 is used only for tech preview and will later be replaced with something more secure
            }

            // Request wallet balance
            using (MemoryStream mw = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(mw))
                {
                    writer.Write(Node.walletStorage.getPrimaryAddress().Length);
                    writer.Write(Node.walletStorage.getPrimaryAddress());
                    NetworkClientManager.broadcastData(new char[] { 'M' }, ProtocolMessageCode.getBalance, mw.ToArray(), null);
                }
            }

        }

        static public void stop()
        {
            // Stop the keepalive thread
            PresenceList.stopKeepAlive();

            // Stop the loop timer
            mainLoopTimer.Stop();

            // Stop the network queue
            NetworkQueue.stop();

            NetworkClientManager.stop();
            StreamClientManager.stop();

            // Stop the stream processor
            StreamProcessor.uninitialize();
        }

        public static string getFullAddress()
        {
            return Config.publicServerIP + ":" + Config.serverPort;
        }


        public static ulong getLastBlockHeight()
        {
            if (lastBlock != null)
            {
                return lastBlock.blockNum;
            }
            return 0;
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
            return 'C';
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
    }
}