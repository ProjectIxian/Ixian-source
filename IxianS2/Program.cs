using DLT;
using DLT.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using IXICore;
using System.Threading;

namespace S2
{
    class Program
    {
        private static System.Timers.Timer mainLoopTimer;
        private static APIServer apiServer;

        public static bool noStart = false;

        static void Main(string[] args)
        {
            // Clear the console first
            Console.Clear();

            // Start logging
            Logging.start();

            onStart(args);

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                apiServer.forceShutdown = true;
            };

            if (apiServer != null)
            {
                while (apiServer.forceShutdown == false)
                {
                    Thread.Sleep(1000);
                }
            }
            onStop();

        }

        static void onStart(string[] args)
        {
            bool verboseConsoleOutputSetting = Config.verboseConsoleOutput;
            Config.verboseConsoleOutput = true;

            Console.WriteLine(string.Format("IXIAN S2 {0}", Config.version));

            // Read configuration from command line
            Config.readFromCommandLine(args);

            if (noStart)
            {
                Thread.Sleep(1000);
                return;
            }

            // Set the logging options
            Logging.setOptions(Config.maxLogSize, Config.maxLogCount);

            Logging.info(string.Format("Starting IXIAN S2 {0}", Config.version));

            // Log the parameters to notice any changes
            Logging.info(String.Format("Mainnet: {0}", !Config.isTestNet));
            Logging.info(String.Format("Server Port: {0}", Config.serverPort));
            Logging.info(String.Format("API Port: {0}", Config.apiPort));
            Logging.info(String.Format("Wallet File: {0}", Config.walletFile));

            // Initialize the crypto manager
            CryptoManager.initLib();

            // Start the actual DLT node
            Node.start(verboseConsoleOutputSetting);

            if (noStart)
            {
                Thread.Sleep(1000);
                return;
            }

            // Start the HTTP JSON API server
            apiServer = new APIServer();

            // Setup a timer to handle routine updates
            mainLoopTimer = new System.Timers.Timer(1000);
            mainLoopTimer.Elapsed += new ElapsedEventHandler(onUpdate);
            mainLoopTimer.Start();

            if (Config.verboseConsoleOutput)
                Console.WriteLine("-----------\nPress Ctrl-C or use the /shutdown API to stop the S2 process at any time.\n");
        }

        static void onUpdate(object source, ElapsedEventArgs e)
        {
            if (Console.KeyAvailable)
            {
                ConsoleKeyInfo key = Console.ReadKey();

                if (key.Key == ConsoleKey.W)
                {
                    string ws_checksum = Crypto.hashToString(Node.walletState.calculateWalletStateChecksum());
                    Logging.info(String.Format("WalletState checksum: ({0} wallets, {1} snapshots) : {2}",
                        Node.walletState.numWallets, Node.walletState.hasSnapshot, ws_checksum));
                }
                else if (key.Key == ConsoleKey.V)
                {
                    Config.verboseConsoleOutput = !Config.verboseConsoleOutput;
                    Logging.consoleOutput = Config.verboseConsoleOutput;
                    Console.CursorVisible = Config.verboseConsoleOutput;
                    if (Config.verboseConsoleOutput == false)
                        Node.statsConsoleScreen.clearScreen();
                }
                else if (key.Key == ConsoleKey.Escape)
                {
                    Node.stop();
                    Environment.Exit(-1);
                }

            }
            if (Node.update() == false)
            {
                apiServer.forceShutdown = true;
            }
        }

        static void onStop()
        {
            if (mainLoopTimer != null)
            {
                mainLoopTimer.Stop();
            }

            // Stop the API server
            if (apiServer != null)
            {
                apiServer.stop();
            }

            if (noStart == false)
            {
                // Stop the DLT
                Node.stop();
            }

            // Stop logging
            while (Logging.getRemainingStatementsCount() > 0)
            {
                Thread.Sleep(100);
            }
            Logging.stop();

            if (noStart == false)
            {
                Console.WriteLine("");
                Console.WriteLine("Ixian S2 Node stopped.");
            }
        }
    }
}
