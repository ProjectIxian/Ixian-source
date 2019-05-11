using DLT.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT.Meta
{
    public class StatsConsoleScreen
    {
        private DateTime startTime;

        private Thread thread = null;
        private bool running = false;

        private int consoleWidth = 50;

        public StatsConsoleScreen()
        {
            Console.Clear();

            Console.CursorVisible = Config.verboseConsoleOutput;

            // Start thread
            running = true;
            thread = new Thread(new ThreadStart(threadLoop));
            thread.Start();

            startTime = DateTime.Now;
        }

        // Shutdown console thread
        public void stop()
        {
            running = false;
        }

        private void threadLoop()
        {
            while (running)
            {
                if (Config.verboseConsoleOutput == false)
                    drawScreen();

                Thread.Sleep(2000);
                Thread.Yield();
            }
        }

        public void clearScreen()
        {
            //Console.BackgroundColor = ConsoleColor.DarkGreen;
            Console.Clear();
            drawScreen();
        }

        public void drawScreen()
        {
            Console.SetCursorPosition(0, 0);



            writeLine("  _______   _______          _   _    _____ ___  ");
            writeLine(" |_   _\\ \\ / /_   _|   /\\   | \\ | |  / ____|__ \\ ");
            writeLine("   | |  \\ V /  | |    /  \\  |  \\| | | (___    ) |");
            writeLine("   | |   > <   | |   / /\\ \\ | . ` |  \\___ \\  / / ");
            writeLine("  _| |_ / . \\ _| |_ / ____ \\| |\\  |  ____) |/ /_ ");
            writeLine(" |_____/_/ \\_\\_____/_/    \\_\\_| \\_| |_____/|____|");
            writeLine(" {0} ", ("" + Config.version).PadLeft(48));
            writeLine(" http://localhost:{0}/                       ", Config.apiPort);
            writeLine("──────────────────────────────────────────────────");

            writeLine(" Thank you for running an Ixian S2 node.\n For help please visit www.ixian.io");
            writeLine("──────────────────────────────────────────────────\n");


            /*
                        if (Node.serverStarted == false)
                        {
                            return;
                        }*/

            // Node status
            string dltStatus = "active       ";

            int connectionsIn = 0;

            string connectionsInStr = "-";  // Default to no inbound connections accepted
            if (NetworkServer.isRunning())
            {
                // If the server is running, show the number of inbound connections
                connectionsIn = NetworkServer.getConnectedClients().Count();
                if (!NetworkServer.isConnectable())
                {
                    connectionsInStr = "Not connectable";
                }
                else
                {
                    connectionsInStr = String.Format("{0}", connectionsIn);
                }
            }

            int connectionsOut = NetworkClientManager.getConnectedClients().Count();
            if (connectionsIn + connectionsOut < 1)
                dltStatus = "connecting   ";


            writeLine("\tStatus:\t\t{0}\n", dltStatus);
            writeLine("\tConnections (I/O):\t{0}", connectionsInStr + "/" + connectionsOut);
            writeLine("\tPresences:\t\t{0}", PresenceList.getTotalPresences());

            writeLine("\n──────────────────────────────────────────────────");

            TimeSpan elapsed = DateTime.Now - startTime;

            writeLine(" Running for {0} days {1}h {2}m {3}s", elapsed.Days, elapsed.Hours, elapsed.Minutes, elapsed.Seconds);
            writeLine("");
            writeLine(" Press V to toggle stats. Ctrl-C to exit.");

        }

        private void writeLine(string str, params object[] arguments)
        {
            Console.WriteLine(string.Format(str, arguments).PadRight(consoleWidth));
        }
    }
}
