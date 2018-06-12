using S2.Network;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT.Meta
{
    class Node
    {
        public static WalletStorage walletStorage;

        static public void start()
        {
            // Load or Generate the wallet
            walletStorage = new WalletStorage(Config.walletFile);

            // Start the node stream server
            NetworkStreamServer.beginNetworkOperations();

            // Start the network client manager
            NetworkClientManager.startClients();

        }

        static public void update()
        {

        }

        static public void stop()
        {
            NetworkClientManager.stopClients();
            // Stop the network server
            NetworkStreamServer.stopNetworkOperations();
        }

        static public void reconnect()
        {
            // Reconnect server and clients
            NetworkStreamServer.restartNetworkOperations();
            NetworkClientManager.restartClients();
        }
    }
}
