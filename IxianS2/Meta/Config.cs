using Fclp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace DLT
{
    namespace Meta
    {

        public class Config
        {
            // Providing pre-defined values
            // Can be read from a file later, or read from the command line
            public static int serverPort = 10235;
            public static int apiPort = 8001;
            public static string publicServerIP = "127.0.0.1";

            public static string walletFile = "wallet.dat";

            // Store the device id in a cache for reuse in later instances
            public static string device_id = Guid.NewGuid().ToString();
            public static string externalIp = "";

            // Read-only values
            public static readonly string version = "s2-0.1.0"; // S2 Node version
            public static readonly bool isTestNet = true; // Testnet designator

            public static readonly int maxLogFileSize = 1024; // 50MB

            public static readonly int nodeVersion = 4;
            public static readonly int networkClientReconnectInterval = 10 * 1000; // Time in milliseconds

            public static readonly int keepAliveSecondsInterval = 45; // Standard expire is 300 (5 minutes)

            public static readonly int maximumNodeReconnectCount = 5; // Number of retries before proceeding to a different dlt node
            public static readonly int simultaneousConnectedNodes = 4; // Desired number of simulatenously connected dlt nodes
            public static readonly int maximumStreamServerClients = 5000; // Maximum number of stream clients this s2 node can accept

            public static readonly int defaultRsaKeySize = 4096;


            private static Config singletonInstance;
            private Config()
            {

            }

            public static Config singleton
            {
                get
                {
                    if (singletonInstance == null)
                    {
                        singletonInstance = new Config();
                    }
                    return singletonInstance;
                }
            }

            public static void readFromCommandLine(string[] args)
            {
                Logging.log(LogSeverity.info, "Reading config...");
                var cmd_parser = new FluentCommandLineParser();

                // Read the parameters from the command line and overwrite values if necessary
                string new_value = "";
                cmd_parser.Setup<string>('v').Callback(value => new_value = value).Required();

                cmd_parser.Setup<int>('p', "port").Callback(value => serverPort = value).Required();
                cmd_parser.Setup<int>('a', "apiport").Callback(value => apiPort = value).Required();

                // Convert the genesis block funds to ulong, as only long is accepted with FCLP
                cmd_parser.Setup<string>('w', "wallet").Callback(value => walletFile = value).Required();

                cmd_parser.SetupHelp("h", "help").Callback(text => Console.WriteLine("IXIAN S2 Help"));


                cmd_parser.Parse(args);

                // Log the parameters to notice any changes
                Logging.log(LogSeverity.info, String.Format("S2 Server Port: {0}", serverPort));
                Logging.log(LogSeverity.info, String.Format("S2 API Port: {0}", apiPort));
                Logging.log(LogSeverity.info, String.Format("Wallet File: {0}", walletFile));

            }

        }

    }
}