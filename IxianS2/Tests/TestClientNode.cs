using DLT.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S2
{
    class TestClientNode
    {


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

    }
}
