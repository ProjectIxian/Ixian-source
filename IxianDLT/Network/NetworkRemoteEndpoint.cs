﻿using DLT.Meta;
using DLT.Network;
using DLTNode.Network;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DLT
{
    public class RemoteEndpoint
    {
        public string fullAddress = "127.0.0.1:10000";
        public string address = "";
        public int incomingPort = Config.serverPort;


        protected long lastDataReceivedTime = 0;
        protected long lastPing = 0;

        public IPEndPoint remoteIP;
        public Socket clientSocket;
        public RemoteEndpointState state;
        public bool inIO;
        // Maintain two threads for handling data receiving and sending
        protected Thread recvThread = null;
        protected Thread sendThread = null;
        protected Thread parseThread = null;

        public Presence presence = null;
        public PresenceAddress presenceAddress = null;

        protected bool running = false;

        // Maintain a queue of messages to send
        private List<QueueMessage> sendQueueMessages = new List<QueueMessage>();

        // Maintain a queue of raw received data
        private List<QueueMessageRaw> recvRawQueueMessages = new List<QueueMessageRaw>();

        private byte[] socketReadBuffer = null;

        protected void prepareSocket(Socket socket)
        {
            // The socket will linger for 3 seconds after 
            // Socket.Close is called.
            socket.LingerState = new LingerOption(true, 3);

            // Disable the Nagle Algorithm for this tcp socket.
            socket.NoDelay = true;

            //tcpClient.Client.ReceiveTimeout = 5000;
            socket.ReceiveBufferSize = 1024 * 64;
            socket.SendBufferSize = 1024 * 64;
            //tcpClient.Client.SendTimeout = 5000;

            socket.Blocking = true;
        }

        public void start(Socket socket = null)
        {
            if(running)
            {
                return;
            }

            if (socket != null)
            {
                clientSocket = socket;
            }
            if (clientSocket == null)
            {
                Logging.error("Could not start NetworkRemoteEndpoint, socket is null");
                return;
            }

            prepareSocket(clientSocket);

            remoteIP = (IPEndPoint)clientSocket.RemoteEndPoint;
            address = remoteIP.Address.ToString();
            fullAddress = address + ":" + remoteIP.Port;
            presence = null;
            presenceAddress = null;

            lastPing = 0;
            lastDataReceivedTime = Clock.getTimestamp(DateTime.Now);

            state = RemoteEndpointState.Established;

            running = true;

            // Abort all related threads
            if (recvThread != null)
            {
                recvThread.Abort();
            }
            if (sendThread != null)
            {
                sendThread.Abort();
            }
            if (parseThread != null)
            {
                parseThread.Abort();
            }

            // Start receive thread
            recvThread = new Thread(new ThreadStart(recvLoop));
            recvThread.Start();

            // Start send thread
            sendThread = new Thread(new ThreadStart(sendLoop));
            sendThread.Start();

            // Start parse thread
            parseThread = new Thread(new ThreadStart(parseLoop));
            parseThread.Start();
        }

        // Aborts all related endpoint threads and data
        public void stop()
        {
            Thread.Sleep(50); // clear any last messages

            state = RemoteEndpointState.Closed;
            running = false;

            Thread.Sleep(50); // wait for threads to stop

            lock (sendQueueMessages)
            {
                sendQueueMessages.Clear();
            }

            lock (recvRawQueueMessages)
            {
                recvRawQueueMessages.Clear();
            }

            // Abort all related threads
            if (recvThread != null)
            {
                recvThread.Abort();
            }
            if (sendThread != null)
            {
                sendThread.Abort();
            }
            if (parseThread != null)
            {
                parseThread.Abort();
            }

            disconnect();
        }

        // Receive thread
        protected virtual void recvLoop()
        {
            socketReadBuffer = new byte[8192];
            while (running)
            {
                // Let the protocol handler receive and handle messages
                try
                {
                    byte[] data = readSocketData();
                    if (data != null)
                    {
                        parseDataInternal(data, this);
                    }
                }
                catch (Exception e)
                {
                    Logging.warn(string.Format("recvRE: Disconnected client {0} with exception {1}", getFullAddress(), e.ToString()));
                    state = RemoteEndpointState.Closed;
                }

                // Sleep a while to throttle the client
                // Check if there are too many messages
                // TODO TODO TODO should we include txqueue here as well?
                if (NetworkQueue.getQueuedMessageCount() > Config.maxNetworkQueue)
                {
                    Thread.Sleep(50);
                }else
                {
                    Thread.Sleep(1);
                }

                // Check if the client disconnected
                if (state == RemoteEndpointState.Closed)
                {
                    running = false;
                }
            }

            disconnect();

            // Remove this endpoint from the network server
            NetworkServer.removeEndpoint(this);
        }

        public virtual void disconnect()
        {
            // Close the client socket
            if (clientSocket != null)
            {
                try
                {
                    clientSocket.Shutdown(SocketShutdown.Both);
                    clientSocket.Close();
                    clientSocket = null;
                }
                catch (Exception e)
                {
                    Logging.warn(string.Format("recvRE: Could not shutdown client socket: {0}", e.ToString()));
                }
            }
        }


        // Send thread
        protected void sendLoop()
        {
            // Prepare an special message object to use while sending, without locking up the queue messages
            QueueMessage active_message = new QueueMessage();

            while (running)
            {
                long curTime = Clock.getTimestamp(DateTime.Now);
                if (curTime - lastDataReceivedTime > Config.pingInterval)
                {
                    if (lastPing == 0)
                    {
                        lastPing = curTime;
                        byte[] pingBytes = new byte[1];
                        sendDataInternal(ProtocolMessageCode.ping, pingBytes, Crypto.sha256(pingBytes));
                        Thread.Sleep(1);
                        continue;
                    }else if(curTime - lastPing > Config.pingTimeout)
                    {
                        // haven't received any data for 30 seconds, stop running
                        Logging.error(String.Format("Node {0} hasn't received any data from remote endpoint for over 30 seconds, disconnecting.", getFullAddress()));
                        running = false;
                        break;
                    }
                }

                bool message_found = false;
                lock (sendQueueMessages)
                {
                    if (sendQueueMessages.Count > 0)
                    {
                        // Pick the oldest message
                        QueueMessage candidate = sendQueueMessages[0];
                        active_message.code = candidate.code;
                        active_message.data = candidate.data;
                        active_message.checksum = candidate.checksum;
                        active_message.skipEndpoint = candidate.skipEndpoint;
                        // Remove it from the queue
                        sendQueueMessages.Remove(candidate);
                        message_found = true;
                    }
                }

                if (message_found)
                {
                    // Active message set, attempt to send it
                    sendDataInternal(active_message.code, active_message.data, active_message.checksum);
                    Thread.Sleep(1);
                }
                else
                {
                    // Sleep for 10ms to prevent cpu waste
                    Thread.Sleep(10);
                }
            }
        }

        // Parse thread
        protected void parseLoop()
        {
            // Prepare an special message object to use while sending, without locking up the queue messages
            QueueMessageRaw active_message = new QueueMessageRaw();

            while (running)
            {
                try
                {
                    bool message_found = false;
                    lock (recvRawQueueMessages)
                    {
                        if (recvRawQueueMessages.Count > 0)
                        {
                            // Pick the oldest message
                            QueueMessageRaw candidate = recvRawQueueMessages[0];
                            active_message.data = candidate.data;
                            active_message.endpoint = candidate.endpoint;
                            // Remove it from the queue
                            recvRawQueueMessages.Remove(candidate);
                            message_found = true;
                        }
                    }

                    if (message_found)
                    {
                        // Active message set, add it to Network Queue
                        ProtocolMessage.readProtocolMessage(active_message.data, this);
                    }
                    else
                    {
                        Thread.Sleep(10);
                    }

                }
                catch (Exception e)
                {
                    Logging.error(String.Format("Exception occured for client {0} in parseLoopRE: {1} ", getFullAddress(), e));
                }
                // Sleep a bit to prevent cpu waste
                Thread.Yield();
            }

        }

        protected void parseDataInternal(byte[] data, RemoteEndpoint endpoint)
        {
            QueueMessageRaw message = new QueueMessageRaw();
            message.data = data;
            message.endpoint = endpoint;

            lock (recvRawQueueMessages)
            {
                recvRawQueueMessages.Add(message);
            }
        }


        // Internal function that sends data through the socket
        protected void sendDataInternal(ProtocolMessageCode code, byte[] data, byte[] checksum)
        {
            byte[] ba = ProtocolMessage.prepareProtocolMessage(code, data, checksum);
            NetDump.Instance.appendSent(ba);
            try
            {
                for (int sentBytes = 0; sentBytes < ba.Length;)
                {
                    int bytesToSendCount = ba.Length - sentBytes;
                    if (bytesToSendCount > 8000)
                    {
                        bytesToSendCount = 8000;
                    }
                    int curSentBytes = clientSocket.Send(ba, sentBytes, ba.Length - sentBytes, SocketFlags.None);
                    if (curSentBytes < bytesToSendCount)
                    {
                        Thread.Sleep(10);
                    }
                    else
                    {
                        // Sleep a bit to allow other threads to do their thing
                        Thread.Yield();
                    }
                    sentBytes += curSentBytes;
                    // TODO TODO TODO timeout
                }
                if (clientSocket.Connected == false)
                {
                    Logging.warn(String.Format("sendRE: Failed senddata to remote endpoint {0}, Closing.", getFullAddress()));
                    state = RemoteEndpointState.Closed;
                }
            }
            catch (Exception e)
            {
                Logging.warn(String.Format("sendRE: Socket exception for {0}, closing. {1}", getFullAddress(), e));
                state = RemoteEndpointState.Closed;

            }
        }


        // Sends data over the network
        public void sendData(ProtocolMessageCode code, byte[] data)
        {
            if (data == null)
            {
                Logging.warn(string.Format("Invalid protocol message data for {0}", code));
                return;
            }

            QueueMessage message = new QueueMessage();
            message.code = code;
            message.data = data;
            message.checksum = Crypto.sha256(data);
            message.skipEndpoint = null;

            lock (sendQueueMessages)
            {
                if (sendQueueMessages.Exists(x => x.code == message.code && message.checksum.SequenceEqual(x.checksum)))
                {
                    Logging.warn(string.Format("Attempting to add a duplicate message (code: {0}) to the network queue", code));
                }
                else
                {
                    if (sendQueueMessages.Count > 6)
                    {
                        // Prioritize certain messages if the queue is large
                        if (message.code != ProtocolMessageCode.newTransaction)
                        {
                            sendQueueMessages.Insert(3, message);
                        }
                        else
                        {
                            // Check if there are too many messages
                            if (sendQueueMessages.Count > Config.maxSendQueue)
                            {
                                sendQueueMessages.RemoveAt(1);
                            }

                            sendQueueMessages.Add(message);
                        }
                    }
                    else
                    {
                        sendQueueMessages.Add(message);
                    }
                }
            }

        }

        public int getQueuedMessageCount()
        {
            lock (sendQueueMessages)
            {
                return sendQueueMessages.Count;
            }
        }

        public bool isConnected()
        {
            try
            {
                if (clientSocket == null)
                {
                    return false;
                }

                return clientSocket.Connected && running;
            }
            catch (Exception)
            {
                return false;
            }
        }

        // Get the ip/hostname and port
        public string getFullAddress(bool useIncomingPorts = false)
        {
            if(useIncomingPorts)
            {
                return address + ":" + incomingPort;
            }
            return fullAddress;
        }

        private int getDataLengthFromMessageHeader(List<byte> header)
        {
            int data_length = -1;
            // we should have the full header, save the data length
            using (MemoryStream m = new MemoryStream(header.ToArray()))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    reader.ReadByte(); // skip start byte
                    int code = reader.ReadInt32(); // skip message code
                    data_length = reader.ReadInt32(); // finally read data length
                    byte[] data_checksum = reader.ReadBytes(32); // skip checksum sha256, 32 bytes
                    byte checksum = reader.ReadByte(); // header checksum byte
                    byte endByte = reader.ReadByte(); // end byte

                    if (endByte != 'I')
                    {
                        Logging.warn("Header end byte was not 'I'");
                        return -1;
                    }

                    if (ProtocolMessage.getHeaderChecksum(header.Take(41).ToArray()) != checksum)
                    {
                        Logging.warn(String.Format("Header checksum mismatch"));
                        return -1;
                    }

                    if (data_length <= 0)
                    {
                        Logging.warn(String.Format("Data length was {0}, code {1}", data_length, code));
                        return -1;
                    }

                    if (data_length > Config.maxMessageSize)
                    {
                        Logging.warn(String.Format("Received data length was bigger than max allowed message size - {0}, code {1}.", data_length, code));
                        return -1;
                    }
                }
            }
            return data_length;
        }

        // Reads data from a socket and returns a byte array
        public byte[] readSocketData()
        {
            Socket socket = clientSocket;

            byte[] data = null;

            // Check for socket availability
            if (socket.Connected == false)
            {
                throw new Exception("Socket already disconnected at other end");
            }

            if (socket.Available < 1)
            {
                // Sleep a while to prevent cpu cycle waste
                Thread.Sleep(10);
                return data;
            }

            // Read multi-packet messages
            // TODO: optimize this as it's not very efficient
            List<byte> big_buffer = new List<byte>();

            bool message_found = false;

            try
            {
                int data_length = 0;
                int header_length = 43; // start byte + int32 (4 bytes) + int32 (4 bytes) + checksum (32 bytes) + header checksum (1 byte) + end byte
                int bytesToRead = 1;
                while (message_found == false && socket.Connected)
                {
                    //int pos = bytesToRead > NetworkProtocol.recvByteHist.Length ? NetworkProtocol.recvByteHist.Length - 1 : bytesToRead;
                    /*lock (NetworkProtocol.recvByteHist)
                    {
                        NetworkProtocol.recvByteHist[pos]++;
                    }*/
                    int byteCounter = socket.Receive(socketReadBuffer, bytesToRead, SocketFlags.None);
                    NetDump.Instance.appendReceived(socketReadBuffer, byteCounter);
                    if (byteCounter > 0)
                    {
                        lastDataReceivedTime = Clock.getTimestamp(DateTime.Now);
                        lastPing = 0;
                        if (big_buffer.Count > 0)
                        {
                            big_buffer.AddRange(socketReadBuffer.Take(byteCounter));
                            if (big_buffer.Count == header_length)
                            {
                                data_length = getDataLengthFromMessageHeader(big_buffer);
                                if (data_length <= 0)
                                {
                                    data_length = 0;
                                    big_buffer.Clear();
                                    bytesToRead = 1;
                                }
                            }
                            else if (big_buffer.Count == data_length + header_length)
                            {
                                // we have everything that we need, save the last byte and break
                                message_found = true;
                            }
                            if (data_length > 0)
                            {
                                bytesToRead = data_length + header_length - big_buffer.Count;
                                if (bytesToRead > 8000)
                                {
                                    bytesToRead = 8000;
                                }
                            }
                        }
                        else
                        {
                            if (socketReadBuffer[0] == 'X') // X is the message start byte
                            {
                                big_buffer.Add(socketReadBuffer[0]);
                                bytesToRead = header_length - 1; // header length - start byte
                            }
                        }
                        Thread.Yield();
                    }
                    else
                    {
                        // sleep a litte while waiting for bytes
                        Thread.Sleep(10);
                        // TODO TODO TODO, should reset the big_buffer if a timeout occurs
                    }
                }
            }
            catch (Exception e)
            {
                Logging.error(String.Format("NET: endpoint {0} disconnected {1}", getFullAddress(), e));
                throw;
            }
            if (message_found)
            {
                data = big_buffer.ToArray();
            }
            return data;
        }

    }
}