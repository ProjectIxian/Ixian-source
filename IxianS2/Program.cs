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
using System.Security.Permissions;
using System.Runtime.InteropServices;
using System.Diagnostics;

namespace S2
{
    class Program
    {
        // STD_INPUT_HANDLE (DWORD): -10 is the standard input device.
        const int STD_INPUT_HANDLE = -10;

        [DllImport("kernel32.dll", SetLastError = true)]
        static extern IntPtr GetStdHandle(int nStdHandle);

        [DllImport("kernel32.dll")]
        static extern bool GetConsoleMode(IntPtr hConsoleHandle, out uint lpMode);

        [DllImport("kernel32.dll")]
        static extern bool SetConsoleMode(IntPtr hConsoleHandle, uint dwMode);

        [DllImport("Kernel32")]
        static extern bool SetConsoleCtrlHandler(HandlerRoutine Handler, bool Add);

        delegate bool HandlerRoutine(CtrlTypes CtrlType);

        enum CtrlTypes
        {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT,
            CTRL_CLOSE_EVENT,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT
        }


        private static System.Timers.Timer mainLoopTimer;

        public static bool noStart = false;

        [SecurityPermission(SecurityAction.Demand, Flags = SecurityPermissionFlag.ControlAppDomain)]
        static void installUnhandledExceptionHandler()
        {
            System.AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Logging.error(String.Format("Exception was triggered and not handled. Please send this log to the Ixian developers!"));
            Logging.error(e.ExceptionObject.ToString());
        }

        // Handle Windows OS-specific calls
        static void prepareWindowsConsole()
        {
            // Ignore if we're on Mono
            if (IXICore.Platform.onMono())
                return;

            installUnhandledExceptionHandler();

            IntPtr consoleHandle = GetStdHandle(STD_INPUT_HANDLE);

            // get current console mode
            uint consoleMode;
            if (!GetConsoleMode(consoleHandle, out consoleMode))
            {
                // ERROR: Unable to get console mode.
                return;
            }

            // Clear the quick edit bit in the mode flags
            consoleMode &= ~(uint)0x0040; // quick edit

            // set the new mode
            if (!SetConsoleMode(consoleHandle, consoleMode))
            {
                // ERROR: Unable to set console mode
            }

            // Hook a handler for force close
            SetConsoleCtrlHandler(new HandlerRoutine(HandleConsoleClose), true);
        }

        static void Main(string[] args)
        {
            // Clear the console first
            Console.Clear();

            prepareWindowsConsole();

            // Start logging
            Logging.start();

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                Config.verboseConsoleOutput = true;
                Logging.consoleOutput = Config.verboseConsoleOutput;
                e.Cancel = true;
                Node.forceShutdown = true;
            };

            onStart(args);

            if (Node.apiServer != null)
            {
                while (Node.forceShutdown == false)
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

            // Initialize the node
            Node.init();

            // Start the actual S2 node
            Node.start(verboseConsoleOutputSetting);

            if (noStart)
            {
                Thread.Sleep(1000);
                return;
            }

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
                    Config.verboseConsoleOutput = true;
                    Logging.consoleOutput = Config.verboseConsoleOutput;
                    Node.forceShutdown = true;
                }

            }
            if (Node.update() == false)
            {
                Node.forceShutdown = true;
            }
        }

        static void onStop()
        {
            if (mainLoopTimer != null)
            {
                mainLoopTimer.Stop();
            }

            if (noStart == false)
            {
                // Stop the DLT
                Node.stop();
            }

            // Stop logging
            Logging.flush();
            Logging.stop();

            if (noStart == false)
            {
                Console.WriteLine("");
                Console.WriteLine("Ixian S2 Node stopped.");
            }
        }

        static bool HandleConsoleClose(CtrlTypes type)
        {
            switch (type)
            {
                case CtrlTypes.CTRL_C_EVENT:
                case CtrlTypes.CTRL_BREAK_EVENT:
                case CtrlTypes.CTRL_CLOSE_EVENT:
                case CtrlTypes.CTRL_LOGOFF_EVENT:
                case CtrlTypes.CTRL_SHUTDOWN_EVENT:
                    Config.verboseConsoleOutput = true;
                    Logging.consoleOutput = Config.verboseConsoleOutput;
                    Console.WriteLine();
                    Console.WriteLine("Application is being closed!");
                    Logging.info("Shutting down...");
                    Logging.flush();
                    noStart = true;
                    Node.forceShutdown = true;
                    // Wait (max 5 seconds) for everything to die
                    DateTime waitStart = DateTime.Now;
                    while (true)
                    {
                        if (Process.GetCurrentProcess().Threads.Count > 1)
                        {
                            Thread.Sleep(50);
                        }
                        else
                        {
                            Console.WriteLine(String.Format("Graceful shutdown achieved in {0} seconds.", (DateTime.Now - waitStart).TotalSeconds));
                            break;
                        }
                        if ((DateTime.Now - waitStart).TotalSeconds > 30)
                        {
                            Console.WriteLine("Unable to gracefully shutdown. Aborting. Threads that are still alive: ");
                            foreach (Thread t in Process.GetCurrentProcess().Threads)
                            {
                                Console.WriteLine(String.Format("Thread {0}: {1}.", t.ManagedThreadId, t.Name));
                            }
                            break;
                        }
                    }
                    return true;
            }
            return true;
        }
    }
}
