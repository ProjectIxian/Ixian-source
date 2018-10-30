using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S2.Network
{
    class StreamMessage
    {
        public byte[] sender;           // Sender wallet
        public byte[] recipient;        // Recipient wallet 

        public string transactionID;    // Transaction ID used to validate payment for this message
        public byte[] data;             // Actual message data, encrypted

        private string id;              // Message unique id

        public StreamMessage()
        {
            id = Guid.NewGuid().ToString();
        }

        public StreamMessage(byte[] bytes)
        {
            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    id = reader.ReadString();

                    int sender_length = reader.ReadInt32();
                    sender = reader.ReadBytes(sender_length);

                    int recipient_length = reader.ReadInt32();
                    recipient = reader.ReadBytes(recipient_length);

                    int data_length = reader.ReadInt32();
                    data = reader.ReadBytes(data_length);
                }
            }
        }

        public byte[] getBytes()
        {
            using (MemoryStream m = new MemoryStream())
            {
                using (BinaryWriter writer = new BinaryWriter(m))
                {
                    writer.Write(id);

                    // Write the sender
                    int sender_length = sender.Length;
                    writer.Write(sender_length);

                    if (sender_length > 0)
                        writer.Write(sender);
                    else
                        writer.Write(0);


                    // Write the recipient
                    int recipient_length = recipient.Length;
                    writer.Write(recipient_length);

                    if (recipient_length > 0)
                        writer.Write(recipient);
                    else
                        writer.Write(0);


                    // Write the data
                    int data_length = data.Length;
                    writer.Write(data_length);

                    if (data_length > 0)
                        writer.Write(data);
                    else
                        writer.Write(0);

                }
                return m.ToArray();
            }
        }

        public string getID()
        {
            return id;
        }

    }
}
