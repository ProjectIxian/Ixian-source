using DLT;
using DLT.Meta;
using DLT.Network;
using IXICore;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;


// Dummy Network server for IXICore
namespace DLT.Network
{
    public class NetworkServer
    {
        // Returns all the connected clients
        public static string[] getConnectedClients(bool useIncomingPort = false)
        {
            List<String> result = new List<String>();
            return result.ToArray();
        }
    }
}


namespace S2.Network
{
    class StreamTransaction
    {
        public string messageID;
        public Transaction transaction;
    }


    class StreamProcessor
    {
        static List<StreamMessage> messages = new List<StreamMessage>(); // List that stores stream messages
        static List<StreamTransaction> transactions = new List<StreamTransaction>(); // List that stores stream transactions

        static Dictionary<byte[], int> dataRelays = new Dictionary<byte[], int>();

        // Called when receiving S2 data from clients
        public static void receiveData(byte[] bytes, RemoteEndpoint endpoint)
        {
            Logging.info(string.Format("Receiving S2 data from {0}", 
                Base58Check.Base58CheckEncoding.EncodePlain(endpoint.presence.wallet)));

            StreamMessage message = new StreamMessage(bytes);

            // Relay certain messages without transaction
            // TODO: always true for development purposes ONLY!
          //  if(message.type == StreamMessageCode.requestAdd || message.type == StreamMessageCode.acceptAdd)
            {
                NetworkStreamServer.forwardMessage(message.recipient, DLT.Network.ProtocolMessageCode.s2data, bytes);
                return;
            }

            // Extract the transaction
            Transaction transaction = new Transaction(message.transaction);

            // Validate transaction sender
            if(transaction.from.SequenceEqual(message.sender) == false)
            {
                Logging.error("Relayed message transaction mismatch");

                return;
            }

            // Validate transaction amount and fee
            if(transaction.amount < CoreConfig.relayPriceInitial || transaction.fee < CoreConfig.transactionPrice)
            {
                Logging.error("Relayed message transaction amount too low");
                return;
            }

            // Validate transaction receiver
            if (transaction.toList.Keys.First().SequenceEqual(Node.walletStorage.address) == false)
            {
                Logging.error("Relayed message transaction receiver is not this S2 node");
                return;
            }

            // Update the recipient dictionary
            if (dataRelays.ContainsKey(message.recipient))
            {
                dataRelays[message.recipient]++;
                if(dataRelays[message.recipient] > 3)
                {
                    Logging.error("Exceeded amount of unpaid relay messages.");
                    //return;
                }
            }
            else
            {
                dataRelays.Add(message.recipient, 1);
            }


            // Store the transaction
            StreamTransaction streamTransaction = new StreamTransaction();
            streamTransaction.messageID = message.getID();
            streamTransaction.transaction = transaction;
            lock (transactions)
            {
                transactions.Add(streamTransaction);
            }

            // For testing purposes, allow the S2 node to receive relay data itself
            if (message.recipient.SequenceEqual(Node.walletStorage.getWalletAddress()))
            {               
                string test = Encoding.UTF8.GetString(message.data);
                Logging.info(test);

                return;
            }

            Logging.info("NET: Forwarding S2 data");
            NetworkStreamServer.forwardMessage(message.recipient, DLT.Network.ProtocolMessageCode.s2data, bytes);           
        }

        // Called when receiving a transaction signature from a client
        public static void receivedTransactionSignature(byte[] bytes, RemoteEndpoint endpoint)
        {
            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    // Read the message ID
                    string messageID = reader.ReadString();
                    int sig_length = reader.ReadInt32();
                    if(sig_length <= 0)
                    {
                        Logging.warn("Incorrect signature length received.");
                        return;
                    }

                    // Read the signature
                    byte[] signature = reader.ReadBytes(sig_length);

                    lock (transactions)
                    {
                        // Find the transaction with a matching message id
                        StreamTransaction tx = transactions.Find(x => x.messageID.Equals(messageID, StringComparison.Ordinal));
                        if(tx == null)
                        {
                            Logging.warn("No transaction found to match signature messageID.");
                            return;
                        }
                     
                        // Compose a new transaction and apply the received signature
                        Transaction transaction = new Transaction(tx.transaction);
                        transaction.signature = signature;

                        // Verify the signed transaction
                        if (transaction.verifySignature(transaction.pubKey))
                        {
                            // Broadcast the transaction
                            ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.newTransaction, transaction.getBytes(), endpoint, false);
                        }
                        return;
                                                 
                    }
                }
            }
        }


        // Called periodically to clear the black list
        public static void update()
        {

        }


    }
}
