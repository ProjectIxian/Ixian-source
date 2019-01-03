using DLT;
using DLT.Meta;
using DLT.Network;
using IXICore;
using S2.Network;
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
            // Request wallet balance
            using (MemoryStream mw = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(mw))
                {
                    writer.Write(Node.walletStorage.getPrimaryAddress().Length);
                    writer.Write(Node.walletStorage.getPrimaryAddress());
                    NetworkClientManager.broadcastData(new char[]{ 'M', 'R' }, ProtocolMessageCode.getBalance, mw.ToArray());
                }
            }
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
            return sendTestMessage(friend);
        }

        // Sends a test message to a specified friend
        static public bool sendTestMessage(TestFriend friend)
        {
            // Search for the relay ip
            string relayip = friend.searchForRelay();

            if (relayip == null)
            {
                Logging.error("No relay ip found.");
                return false;
            }

            Logging.info(String.Format("Relay: {0}", relayip));

            // Check if we're connected to the relay node
            TestStreamClient stream_client = TestStreamClientManager.isConnectedTo(relayip);
            if (stream_client == null)
            {
                // In a normal client, this should be done in a different thread and wait for the
                // connection to be established
                stream_client = TestStreamClientManager.connectTo(relayip);

                if (stream_client == null)
                {
                    Logging.error(string.Format("Error sending message. Could not connect to stream node: {0}", relayip));
                }
            }

            // Generate encryption keys
            byte[] keys_data = friend.generateKeys();

            // Generate the transaction
            Transaction transaction = new Transaction((int)Transaction.Type.Normal);
            transaction.amount = CoreConfig.relayPriceInitial;
            transaction.toList.Add(friend.relayWallet, transaction.amount);
            transaction.fee = CoreConfig.transactionPrice;
            transaction.fromList.Add(new byte[1] { 0 }, transaction.amount + transaction.fee);
            transaction.blockHeight = Node.blockHeight;
            transaction.pubKey = Node.walletStorage.getPrimaryPublicKey(); // TODO: check if it's in the walletstate already
            transaction.checksum = Transaction.calculateChecksum(transaction);

            // Prepare the stream message
            StreamMessage message = new StreamMessage();
            message.recipient = friend.walletAddress;
            message.sender = Node.walletStorage.getPrimaryAddress();
            message.transaction = transaction.getBytes();


            // Encrypt the message
            byte[] text_message = Encoding.UTF8.GetBytes("Hello Ixian World!");
            message.encryptMessage(text_message, friend.aesPassword, friend.chachaKey);

            // Encrypt the transaction signature
            byte[] tx_signature = transaction.getSignature(transaction.checksum);
            message.encryptSignature(tx_signature, friend.aesPassword, friend.chachaKey);

            stream_client.sendData(ProtocolMessageCode.s2data, message.getBytes());

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
