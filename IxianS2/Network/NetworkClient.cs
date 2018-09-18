using DLT;
using DLT.Meta;
using DLT.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace S2.Network
{
    class NetworkClient
    {
        private TcpClient tcpClient = null;
        public bool running;
        public string address = "127.0.0.1:10000";

        private string tcpHostname = "";
        private int tcpPort = 0;

        public NetworkClient()
        {
            prepareClient();
        }

        public void prepareClient()
        {
            tcpClient = new TcpClient();

            // Don't allow another socket to bind to this port.
            tcpClient.Client.ExclusiveAddressUse = true;

            // The socket will linger for 3 seconds after 
            // Socket.Close is called.
            tcpClient.Client.LingerState = new LingerOption(true, 3);

            // Disable the Nagle Algorithm for this tcp socket.
            tcpClient.Client.NoDelay = true;

            //tcpClient.Client.ReceiveBufferSize = 1024 * 64;
            // tcpClient.Client.ReceiveTimeout = 1000;

           // tcpClient.Client.SendBufferSize = 1024 * 64;
            // tcpClient.Client.SendTimeout = 1000;

            //tcpClient.Client.Ttl = 42;
            //tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, 1);

        }

        public bool connectToServer(string hostname, int port)
        {
            tcpHostname = hostname;
            tcpPort = port;
            address = string.Format("{0}:{1}", hostname, port);

            // Prepare the TCP client
            prepareClient();

            try
            {
                tcpClient.Connect(hostname, port);
            }
            catch (SocketException se)
            {
                SocketError errorCode = (SocketError)se.ErrorCode;

                switch (errorCode)
                {
                    case SocketError.IsConnected:
                        break;

                    case SocketError.AddressAlreadyInUse:
                        break;

                    default:
                        {
                            //    Logging.warn(string.Format("Socket exception for {0}:{1} has failed. Reason {2}", hostname, port, se.ToString()));
                            Logging.warn(string.Format("Socket exception for {0}", address));
                        }
                        break;
                }
                // Todo: make this more elegant
                try
                {
                    tcpClient.Client.Close();
                }
                catch (Exception e)
                {
                    Logging.warn(string.Format("Socket exception when closing. Reason {0}", e.ToString()));
                }
                running = false;
                return false;
            }
            catch (Exception)
            {
                Logging.warn(string.Format("Network client connection to {0}:{1} has failed.", hostname, port));
                running = false;
                return false;
            }

            Logging.info(string.Format("Network client connected to {0}:{1}", hostname, port));

            running = true;
            Thread thread = new Thread(new ThreadStart(onUpdate));
            thread.Start();

            return true;
        }

        // Reconnect with the previous settings
        public void reconnect()
        {
            if (tcpHostname.Length < 1)
            {
                Logging.warn("Network client reconnect failed due to invalid hostname.");
                return;
            }

            // Check if socket already disconnected
            if (tcpClient == null)
            {
                // TODO: handle this scenario
            }
            else if (tcpClient.Client == null)
            {
                // TODO: handle this scenario
            }
            else if (tcpClient.Client.Connected)
            {
                tcpClient.Client.Shutdown(SocketShutdown.Both);
                tcpClient.Close();
            }

            Console.WriteLine("--> Reconnecting to {0}", address);
            connectToServer(tcpHostname, tcpPort);
        }


        // Sends data over the network
        public void sendData(ProtocolMessageCode code, byte[] data)
        {
            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data);
            try
            {
                tcpClient.Client.Send(ba, SocketFlags.None);
            }
            catch (Exception)
            {
                Console.WriteLine("CLN: Socket exception, attempting to reconnect");
                reconnect();
            }
        }


        private void onUpdate()
        {
            // Send a hello message containing the public ip and port of this node
            using (MemoryStream m = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(m))
                {
                    string publicHostname = string.Format("{0}:{1}", NetworkStreamServer.publicIPAddress, Config.serverPort);
                    // Send the public IP address and port
                    writer.Write(publicHostname);

                    // Send the public node address
                    string address = Node.walletStorage.address;
                    writer.Write(address);

                    // Send the testnet designator
                    writer.Write(Config.isTestNet);

                    // Send the node type
                    char node_type = 'R'; // This is a Relay node
                    writer.Write(node_type);

                    // Send the version
                    writer.Write(Config.version);

                    // Send the node device id
                    writer.Write(Config.device_id);

                    // Send the S2 public key
                    writer.Write(Node.walletStorage.encPublicKey);

                    // Send the wallet public key
                    writer.Write(Node.walletStorage.publicKey);

                    sendData(ProtocolMessageCode.hello, m.ToArray());

                }
            }

            while (running)
            {
                try
                {
                    // Let the protocol handler receive and handle messages
                    ProtocolMessage.readProtocolMessage(tcpClient.Client, null);
                }
                catch (Exception)
                {
                    //Console.WriteLine("Client EX: {0}", e.ToString());
                    disconnect();
                    Thread.Yield();
                    return;
                }

                // Sleep a while to prevent cpu cycle waste
                Thread.Sleep(10);
            }

            disconnect();
            Thread.Yield();
        }

        public void disconnect()
        {
            // Check if socket already disconnected
            if (tcpClient == null)
            {
                return;
            }
            if (tcpClient.Client == null)
            {
                return;
            }

            // Stop reading protocol messages
            running = false;

            if (tcpClient.Client.Connected)
            {
                tcpClient.Client.Shutdown(SocketShutdown.Both);
                // tcpClient.Client.Disconnect(true);
                tcpClient.Close();
            }
        }

        // Get the ip/hostname and port
        public string getFullAddress()
        {
            return string.Format("{0}:{1}", tcpHostname, tcpPort);
        }

        public bool isConnected()
        {
            try
            {
                if (tcpClient == null)
                {
                    return false;
                }

                if (tcpClient.Client == null)
                {
                    return false;
                }

                return tcpClient.Connected && running;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public void sendPing()
        {
            if (!isConnected())
            {
                Logging.info(string.Format("Attempted to ping disconnected client {0}", address));
                reconnect();
                return;
            }

            byte[] tmp = new byte[1];
            byte[] ba = ProtocolMessage.prepareProtocolMessage(ProtocolMessageCode.ping, tmp);
            try
            {
                tcpClient.Client.Send(ba, SocketFlags.None);
                if (tcpClient.Client.Connected == false)
                {
                    Console.WriteLine("Failed sendping to client: {0}. Reconnecting.", address);
                    reconnect();

                }
            }
            catch (Exception)
            {
                Console.WriteLine("Failed ping to client: {0}. Reconnecting.", address);
                reconnect();
            }
        }

        // Broadcasts a keepalive network message for this node PL address
        public bool sendKeepAlive()
        {
            try
            {
                using (MemoryStream m = new MemoryStream())
                {
                    using (BinaryWriter writer = new BinaryWriter(m))
                    {

                        string publicHostname = string.Format("{0}:{1}", NetworkStreamServer.publicIPAddress, Config.serverPort);
                        string wallet = Node.walletStorage.address;
                        writer.Write(wallet);
                        writer.Write(Config.device_id);
                        writer.Write(publicHostname);

                        // Add the unix timestamp
                        string timestamp = Clock.getTimestamp(DateTime.Now);
                        writer.Write(timestamp);

                        // Add a verifiable signature
                        string private_key = Node.walletStorage.privateKey;
                        string signature = CryptoManager.lib.getSignature(timestamp, private_key);
                        writer.Write(signature);

                    }

                    sendData(ProtocolMessageCode.keepAlivePresence, m.ToArray());
                }
            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }
    }

}
