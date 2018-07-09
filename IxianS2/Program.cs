using DLT;
using DLT.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace S2
{
    class Program
    {
        private static Timer mainLoopTimer;

        static void Main(string[] args)
        {
            Logging.info(string.Format("IXIAN S2 Node {0} started", Config.s2Version));

            onStart(args);

            // For testing purposes, wait for the Escape key to be pressed before stopping execution
            // In production this will be changed, as the dlt will run in the background
            while (Console.ReadKey().Key != ConsoleKey.Escape)
            {

            }

            onStop();

            Logging.info(string.Format("IXIAN S2 Node {0} stopped", Config.s2Version));
            Console.ReadKey();
            Console.WriteLine("\nClosing network connections, please wait...\n");
        }

        static void onStart(string[] args)
        {
            // Read configuration from command line
            Config.readFromCommandLine(args);

            // Initialize the crypto manager
            CryptoManager.initLib();

            // Start the HTTP JSON API server
            //apiServer = new APIServer();

            // Start the actual DLT node
            Node.start();

            // Setup a timer to handle routine updates
            mainLoopTimer = new Timer(1000);
            mainLoopTimer.Elapsed += new ElapsedEventHandler(onUpdate);
            mainLoopTimer.Start();


            Console.WriteLine("-----------\nPress Escape to stop the IXIAN S2 Node at any time.\n");

        }

        static void onUpdate(object source, ElapsedEventArgs e)
        {
            Node.update();

            //Console.Write(".");
        }

        static void onStop()
        {
            mainLoopTimer.Stop();

            // Stop the API server
            //apiServer.stop();

            // Stop the DLT
            Node.stop();

            Console.WriteLine("\n");
        }
    }
}
