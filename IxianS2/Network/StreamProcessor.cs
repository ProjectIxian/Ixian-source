using DLT;
using DLT.Meta;
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
    class StreamKeyPair
    {
        public string messageID;
        public string publicKey;
        public string privateKey;

        public StreamKeyPair(string msg, string pub, string priv)
        {
            messageID = msg;
            publicKey = pub;
            privateKey = priv;
        }
    }


    class StreamProcessor
    {
        static List<StreamKeyPair> keypairs = new List<StreamKeyPair>();

        public static void receiveData(byte[] bytes, RemoteEndpoint endpoint)
        {
            Console.WriteLine("NET: Receiving S2 data!");
            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    // Send the message, the onetime public key and the transaction id
                    byte[] from = endpoint.presence.wallet;

                    string message_id = reader.ReadString();

                    int recipientLen = reader.ReadInt32();
                    byte[] recipient = reader.ReadBytes(recipientLen);
                    string transaction_id = reader.ReadString();

                    int encrypted_bytes_count = reader.ReadInt32();
                    byte[] encrypted_message = reader.ReadBytes(encrypted_bytes_count);
                    //string message = Encoding.UTF8.GetString(encrypted_message);

                    StreamKeyPair msgKeys = null;
                    lock(keypairs)
                    {
                        foreach(StreamKeyPair keypair in keypairs)
                        {
                            if(keypair.messageID.Equals(message_id, StringComparison.Ordinal))
                            {
                                msgKeys = keypair;
                                break;
                            }
                        }
                    }

                    // If no keypair is found, abort the procedure
                    if(msgKeys == null)
                    {
                        Logging.warn(string.Format("Missing keypair for message {0} from {1}", message_id, Base58Check.Base58CheckEncoding.EncodePlain(from)));
                        return;
                    }


                    string private_key = msgKeys.privateKey;
                    Console.WriteLine("Encrypted message recp {0} | tx {1} | priv {2}", Base58Check.Base58CheckEncoding.EncodePlain(recipient), transaction_id, private_key);

                    // Forward message to the receiver
                    using (MemoryStream mw = new MemoryStream())
                    {
                        using (BinaryWriter writer = new BinaryWriter(mw))
                        {
                            writer.Write(from);
                            writer.Write(transaction_id);
                            writer.Write(private_key);
                            writer.Write(encrypted_bytes_count);
                            writer.Write(encrypted_message);

                            NetworkStreamServer.forwardMessage(recipient, DLT.Network.ProtocolMessageCode.s2data, mw.ToArray());
                        }
                    }
                }
            }

     

        }

        // Called when receiving a prepare message
        public static void prepareSend(byte[] bytes, RemoteEndpoint client)
        {
            // Deprecated for now
        }

        // Called when a client wants to send a new message. First it has to receive an encryption key
        public static void generateKeys(byte[] bytes, RemoteEndpoint client)
        {
            // TODO: implement this according to the recent changed in the Crypto lib
     /*       string messageID = Encoding.UTF8.GetString(bytes);
            Console.WriteLine("Generating keys for messageid: {0}", messageID);

            List<string> gen_keys = CryptoManager.lib.generateEncryptionKeys();
            if(gen_keys.Count < 2)
            {
                Logging.warn("Could not generate keypair!");
                return;
            }

            string genPublic = gen_keys[0];
            string genPrivate = gen_keys[1];

            StreamKeyPair keypair = new StreamKeyPair(messageID, genPublic, genPrivate);

            lock (keypairs)
            {
                keypairs.Add(keypair);
            }

            using (MemoryStream mw = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(mw))
                {
                    writer.Write(messageID);
                    writer.Write(genPublic);
                    NetworkStreamServer.sendData(client, DLT.Network.ProtocolMessageCode.s2keys, mw.ToArray());
                }
            }
            */

        }


    }
}
