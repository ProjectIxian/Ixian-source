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
        List<TestFriend> friends = new List<TestFriend>();

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
