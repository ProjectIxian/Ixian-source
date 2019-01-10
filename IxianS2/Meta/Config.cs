using Fclp;
using IXICore;
using System;

namespace DLT
{
    namespace Meta
    {

        public class Config
        {
            // Providing pre-defined values
            // Can be read from a file later, or read from the command line
            public static int serverPort = 10235;
            public static int testnetServerPort = 11235;
            public static int apiPort = 8001;
            public static int testnetApiPort = 8101;
            public static string publicServerIP = "127.0.0.1";

            public static string walletFile = "ixian.wal";

            public static bool verboseConsoleOutput = false; // Flag for verbose console output

            public static int maxLogSize = 50;
            public static int maxLogCount = 10;

            // Store the device id in a cache for reuse in later instances
            public static string device_id = Guid.NewGuid().ToString();
            public static string externalIp = "";

            // Read-only values
            public static readonly string version = "s2-0.1.1"; // S2 Node version
            public static bool isTestNet = true; // Testnet designator

            public static readonly int maximumStreamClients = 100; // Maximum number of stream clients this server can accept

            // Quotas
            public static readonly long lastPaidTimeQuota = 10 * 60; // Allow 10 minutes after payment before checking quotas
            public static readonly int infoMessageQuota = 10;  // Allow 10 info messages per 1 data message
            public static readonly int dataMessageQuota = 3; // Allow up to 3 data messages before receiving a transaction signature


            public static bool isTestClient = false;
            public static string testS2Node = "";

            // Development/testing options
            public static bool generateWalletOnly = false;
            public static string dangerCommandlinePasswordCleartextUnsafe = "";


            public static int forceTimeOffset = int.MaxValue;

            // internal
            public static bool changePass = false;

            private Config()
            {

            }

            public static void readFromCommandLine(string[] args)
            {
                Logging.log(LogSeverity.info, "Reading config...");
                // first pass
                var cmd_parser = new FluentCommandLineParser();

                // testnet
                cmd_parser.Setup<bool>('t', "testnet").Callback(value => Config.isTestNet = true).Required();

                cmd_parser.Parse(args);

                if (isTestNet)
                {
                    serverPort = testnetServerPort;
                    apiPort = testnetApiPort;
                    PeerStorage.peersFilename = "testnet-peers.dat";
                }

                string seedNode = "";


                // second pass
                cmd_parser = new FluentCommandLineParser();

                // Read the parameters from the command line and overwrite values if necessary
                string new_value = "";
                cmd_parser.Setup<string>('v').Callback(value => new_value = value).Required();

                cmd_parser.Setup<int>('p', "port").Callback(value => serverPort = value).Required();
                cmd_parser.Setup<int>('a', "apiport").Callback(value => apiPort = value).Required();

                cmd_parser.Setup<string>('w', "wallet").Callback(value => walletFile = value).Required();

                cmd_parser.SetupHelp("h", "help").Callback(text => Console.WriteLine("IXIAN S2 Help"));

                cmd_parser.Setup<bool>('c', "test").Callback(value => Config.isTestClient = true).Required();
                cmd_parser.Setup<string>('n', "s2node").Callback(value => Config.testS2Node = value).Required();

                cmd_parser.Setup<int>("forceTimeOffset").Callback(value => forceTimeOffset = value).SetDefault(int.MaxValue);

                cmd_parser.Setup<bool>("generateWallet").Callback(value => generateWalletOnly = value).SetDefault(false);

                cmd_parser.Setup<string>("walletPassword").Callback(value => dangerCommandlinePasswordCleartextUnsafe = value).SetDefault("");

                cmd_parser.Parse(args);

                // Log the parameters to notice any changes
                Logging.log(LogSeverity.info, String.Format("S2 Server Port: {0}", serverPort));
                Logging.log(LogSeverity.info, String.Format("S2 API Port: {0}", apiPort));
                Logging.log(LogSeverity.info, String.Format("Wallet File: {0}", walletFile));

            }

        }

    }
}