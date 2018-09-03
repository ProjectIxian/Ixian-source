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

            // Note: device id is stored in  a cache for reuse in later instances
            public static string device_id = Guid.NewGuid().ToString();

            // Read-only values
            public static readonly string s2Version = "0.1.0";
            public static readonly int nodeVersion = 4;
            public static readonly int networkClientReconnectInterval = 10 * 1000; // Time in milliseconds

            public static readonly int keepAliveSecondsInterval = 45; // Standard expire is 300 (5 minutes)

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