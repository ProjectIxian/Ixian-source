using DLT;
using DLT.Meta;
using DLT.Network;
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
        static List<StreamMessage> messages = new List<StreamMessage>(); // List that stores stream messages


        public static void receiveData(byte[] bytes, RemoteEndpoint endpoint)
        {
            Logging.info("Receiving S2 data");

            StreamMessage message = new StreamMessage(bytes);

            if(message.recipient.SequenceEqual(Node.walletStorage.getWalletAddress()))
            {
                // This is the recipient
                string test = Encoding.UTF8.GetString(message.data);
                Logging.info(test);

                return;
            }



       /*     Console.WriteLine("NET: Receiving S2 data!");

                            NetworkStreamServer.forwardMessage(recipient, DLT.Network.ProtocolMessageCode.s2data, mw.ToArray());

            */
     

        }


        // Sends a stream message
        public static void sendMessage(StreamMessage message, string hostname = null)
        {
      /*      // Request a new keypair from the S2 Node
            if (hostname == null)
                ProtocolMessage.broadcastProtocolMessage(ProtocolMessageCode.s2generateKeys, Encoding.UTF8.GetBytes(msg.getID()));
            else
            {
                NetworkClientManager.sendData(ProtocolMessageCode.s2generateKeys, Encoding.UTF8.GetBytes(msg.getID()), hostname);
            }*/
        }

    }
}
