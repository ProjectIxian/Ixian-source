using DLT;
using DLT.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S2
{
    class TestClientNode
    {
        public static List<TestFriend> friends = new List<TestFriend>();

        static public void start()
        {
            Logging.info("Starting as an S2 Test Client...");

            // Start the stream client manager
            TestStreamClientManager.start();

        }

        static public void update()
        {

        }

        static public void stop()
        {
            // Stop all stream clients
            TestStreamClientManager.stop();
        }

        static public void reconnect()
        {

            TestStreamClientManager.restartClients();
        }

        // Adds a friend based on a wallet address
        // Returns false if the wallet address could not be found in the Presence List
        static public bool addFriend(byte[] wallet)
        {
            Presence presence = PresenceList.containsWalletAddress(wallet);
            if (presence == null)
                return false;

            TestFriend friend = new TestFriend();
            friend.walletAddress = presence.wallet;
            friend.publicKey = presence.pubkey;

            friends.Add(friend);

            // For testing purposes, we also initiate a key exchange by sending a message
            // In a normal client, we'd wait for an accept friend request-type message first
            sendTestMessage(friend);

            return false;
        }

        // Sends a test message to a specified friend
        static public bool sendTestMessage(TestFriend friend)
        {
            // Check the relay ip
            string relayip = friend.getRelayIP();

            // Check if we're connected to the relay node
            if (TestStreamClientManager.isConnectedTo(relayip) == false)
            {
                // In a normal client, this should be done in a different thread and wait for the
                // connection to be established
                TestStreamClientManager.connectTo(relayip);
            }




            return true;
        }


        // Handles extended protocol messages
        static public void handleExtendProtocol(byte[] data)
        {
            using (MemoryStream m = new MemoryStream(data))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    int code = reader.ReadInt32();

                }
            }
        }





    }
}
