using DLT;
using DLT.Meta;
using IXICore;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;

namespace S2
{
    class APIServer : GenericAPIServer
    {
        public APIServer()
        {
            // Start the API server
            start(String.Format("http://localhost:{0}/", Config.apiPort));
        }

        protected override void onUpdate(HttpListenerContext context)
        {
            try
            {
                Console.Write("*");

                if (context.Request.Url.Segments.Length < 2)
                {
                    sendError(context, "{\"message\":\"no parameters supplied\"}");
                    return;
                }

                string methodName = context.Request.Url.Segments[1].Replace("/", "");

                if (methodName == null)
                {
                    sendError(context, "{\"message\":\"invalid parameters\"}");
                    return;
                }

                try
                {
                    if (parseRequest(context, methodName) == false)
                    {
                        sendError(context, "{\"message\":\"error\"}");
                    }
                }
                catch (Exception e)
                {
                    sendError(context, "{\"message\":\"error\"}");
                    Logging.error(string.Format("Exception occured in API server while processing '{0}'. {1}", methodName, e.ToString()));

                }
            }
            catch (Exception)
            {
                continueRunning = false;
            }
        }

        private bool parseRequest(HttpListenerContext context, string methodName)
        {
            HttpListenerRequest request = context.Request;
            // Set the content type to plain to prevent xml parsing errors in various browsers
            context.Response.ContentType = "application/json";

            if (methodName.Equals("shutdown", StringComparison.OrdinalIgnoreCase))
            {
                string responseString = JsonConvert.SerializeObject("Node shutdown");
                sendResponse(context.Response, responseString);

                forceShutdown = true;

                return true;
            }

            if (methodName.Equals("servers", StringComparison.OrdinalIgnoreCase))
            {
                String[] res = NetworkClientManager.getConnectedClients();
                string responseString = JsonConvert.SerializeObject(res);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("status", StringComparison.OrdinalIgnoreCase))
            {
                Dictionary<string, Object> networkArray = new Dictionary<string, Object>();

                networkArray.Add("S2 Node Version", Config.version);
                string netType = "mainnet";
                if (Config.isTestNet)
                {
                    netType = "testnet";
                }
                networkArray.Add("Network type", netType);
                networkArray.Add("My time", Clock.getTimestamp());
                networkArray.Add("Network time difference", Core.networkTimeDifference);
                networkArray.Add("My External IP", Config.publicServerIP);

/*                networkArray.Add("Queues", "Rcv: " + NetworkQueue.getQueuedMessageCount() + ", RcvTx: " + NetworkQueue.getTxQueuedMessageCount()
                    + ", SendClients: " + NetworkServer.getQueuedMessageCount() + ", SendServers: " + NetworkClientManager.getQueuedMessageCount()
                    + ", Storage: " + Storage.getQueuedQueryCount() + ", Logging: " + Logging.getRemainingStatementsCount());
                networkArray.Add("Node Deprecation Block Limit", Config.compileTimeBlockNumber + Config.deprecationBlockOffset);
                
*/
                networkArray.Add("Wallets", Node.walletState.numWallets);
                networkArray.Add("Presences", PresenceList.getTotalPresences());
                networkArray.Add("Supply", Node.walletState.calculateTotalSupply().ToString());

                networkArray.Add("WS Checksum", Crypto.hashToString(Node.walletState.calculateWalletStateChecksum()));
                networkArray.Add("WS Delta Checksum", Crypto.hashToString(Node.walletState.calculateWalletStateChecksum(0, true)));

//                networkArray.Add("Network Clients", NetworkServer.getConnectedClients());
                networkArray.Add("Network Servers", NetworkClientManager.getConnectedClients());

                string responseString = JsonConvert.SerializeObject(networkArray);
                sendResponse(context.Response, responseString);
                return true;
            }

            if (methodName.Equals("pl", StringComparison.OrdinalIgnoreCase))
            {
                string responseString = "None";

                // Show a list of presences
                lock (PresenceList.presences)
                {
                    var json = PresenceList.presences;
                    responseString = JsonConvert.SerializeObject(json, Formatting.None, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore, Formatting = Formatting.Indented });
                }

                sendResponse(context.Response, responseString);

                return true;
            }

            if (methodName.Equals("testadd", StringComparison.OrdinalIgnoreCase))
            {
                byte[] wallet = Base58Check.Base58CheckEncoding.DecodePlain(request.QueryString["wallet"]);

                string responseString = JsonConvert.SerializeObject("Friend added successfully");

                if(TestClientNode.addFriend(wallet) == false)
                {
                    responseString = JsonConvert.SerializeObject("Could not find wallet id or add friend");
                }

                sendResponse(context.Response, responseString);

                return true;
            }



            return false;
        }
    }
}
