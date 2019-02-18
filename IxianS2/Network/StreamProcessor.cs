using DLT;
using DLT.Meta;
using DLT.Network;
using IXICore;
using System;
using System.Collections.Generic;
using System.IO;

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


        // Called when receiving S2 data from clients
        public static void receiveData(byte[] bytes, RemoteEndpoint endpoint)
        {
            string endpoint_wallet_string = Base58Check.Base58CheckEncoding.EncodePlain(endpoint.presence.wallet);
            Logging.info(string.Format("Receiving S2 data from {0}", endpoint_wallet_string));

            StreamMessage message = new StreamMessage(bytes);

            // Don't allow clients to send error stream messages, as it's reserved for S2 nodes only
            if(message.type == StreamMessageCode.error)
            {
                Logging.warn(string.Format("Discarding error message type from {0}", endpoint_wallet_string));
                return;
            }

            // TODO: commented for development purposes ONLY!
            /*if (QuotaManager.exceededQuota(endpoint.presence.wallet))
            {
                Logging.error(string.Format("Exceeded quota of info relay messages for {0}", endpoint_wallet_string));
                sendError(endpoint.presence.wallet);
                return;
            }*/

            bool data_message = false;
            if (message.type == StreamMessageCode.data)
                data_message = true;

            QuotaManager.addActivity(endpoint.presence.wallet, data_message);

            // Relay certain messages without transaction
            NetworkServer.forwardMessage(message.recipient, ProtocolMessageCode.s2data, bytes);

            // TODO: commented for development purposes ONLY!
            /*
                        // Extract the transaction
                        Transaction transaction = new Transaction(message.transaction);

                        // Validate transaction sender
                        if(transaction.from.SequenceEqual(message.sender) == false)
                        {
                            Logging.error(string.Format("Relayed message transaction mismatch for {0}", endpoint_wallet_string));
                            sendError(message.sender);
                            return;
                        }

                        // Validate transaction amount and fee
                        if(transaction.amount < CoreConfig.relayPriceInitial || transaction.fee < CoreConfig.transactionPrice)
                        {
                            Logging.error(string.Format("Relayed message transaction amount too low for {0}", endpoint_wallet_string));
                            sendError(message.sender);
                            return;
                        }

                        // Validate transaction receiver
                        if (transaction.toList.Keys.First().SequenceEqual(Node.walletStorage.address) == false)
                        {
                            Logging.error("Relayed message transaction receiver is not this S2 node");
                            sendError(message.sender);
                            return;
                        }

                        // Update the recipient dictionary
                        if (dataRelays.ContainsKey(message.recipient))
                        {
                            dataRelays[message.recipient]++;
                            if(dataRelays[message.recipient] > Config.relayDataMessageQuota)
                            {
                                Logging.error(string.Format("Exceeded amount of unpaid data relay messages for {0}", endpoint_wallet_string));
                                sendError(message.sender);
                                return;
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
                        */
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
                        if (transaction.verifySignature(transaction.pubKey, null))
                        {
                            // Broadcast the transaction
                            CoreProtocolMessage.broadcastProtocolMessage(new char[] { 'M', 'H' }, ProtocolMessageCode.newTransaction, transaction.getBytes(), endpoint);
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

        // Sends an error stream message to a recipient
        // TODO: add additional data for error details
        public static void sendError(byte[] recipient)
        {
            StreamMessage message = new StreamMessage();
            message.type = StreamMessageCode.error;
            message.recipient = recipient;
            message.transaction = new byte[1];
            message.sigdata = new byte[1];
            message.data = new byte[1];

            NetworkServer.forwardMessage(recipient, DLT.Network.ProtocolMessageCode.s2data, message.getBytes());
        }
    }
}
