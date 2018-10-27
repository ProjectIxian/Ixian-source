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

            // Store the device id in a cache for reuse in later instances
            public static string device_id = Guid.NewGuid().ToString();
            public static string externalIp = "";

            // Read-only values
            public static readonly string version = "s2-0.1.0"; // S2 Node version
            public static bool isTestNet = true; // Testnet designator


            public static readonly int maximumStreamClients = 100; // Maximum number of stream clients this server can accept

            public static bool isTestClient = false;
            public static string testS2Node = "";


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


                cmd_parser.Parse(args);

                // Log the parameters to notice any changes
                Logging.log(LogSeverity.info, String.Format("S2 Server Port: {0}", serverPort));
                Logging.log(LogSeverity.info, String.Format("S2 API Port: {0}", apiPort));
                Logging.log(LogSeverity.info, String.Format("Wallet File: {0}", walletFile));

            }

        }

    }
}