using DLT;
using DLT.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S2
{
    class TestFriend
    {
        public byte[] walletAddress;
        public byte[] publicKey;

        public byte[] cachaKey = null;
        public string aesPassword = null;

        // Handles receiving and decryption of keys
        public bool receiveKeys(byte[] data)
        {
            try
            {
                // Decrypt data first
                byte[] decrypted = CryptoManager.lib.decryptWithRSA(data, Node.walletStorage.privateKey);

                using (MemoryStream m = new MemoryStream(data))
                {
                    using (BinaryReader reader = new BinaryReader(m))
                    {
                        // Read the chacha key
                        int length = reader.ReadInt32();
                        byte[] chacha = reader.ReadBytes(length);
                        
                        // Assign the cacha key
                        cachaKey = chacha.ToArray();

                        // Read and assign the aes password
                        aesPassword = reader.ReadString();

                        // Everything succeeded
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Logging.error(String.Format("Exception during receive keys: {0}", e.Message));
            }

            return false;
        }

        // Retrieve the friend's connected S2 node address. Returns null if not found
        public string getRelayIP()
        {
            string ip = null;
            Presence presence = PresenceList.containsWalletAddress(walletAddress);
            if (presence == null)
                return ip;

            lock (PresenceList.presences)
            {
                // Go through each presence address searching for C nodes
                foreach (PresenceAddress addr in presence.addresses)
                {
                    // Only check Client nodes
                    if (addr.type == 'C')
                    {
                        // We have a potential candidate here, store it
                        string candidate_ip = addr.address;

                        // Go through each presence again. This should be more optimized.
                        foreach (Presence s2presence in PresenceList.presences)
                        {
                            // Go through each single address
                            foreach (PresenceAddress s2addr in s2presence.addresses)
                            {
                                // Only check Relay nodes that have the candidate ip
                                if (s2addr.type == 'R' && s2addr.address.Equals(candidate_ip, StringComparison.Ordinal))
                                {
                                    // We found the friend's connected s2 node
                                    ip = s2addr.address;
                                    break;
                                }
                            }
                        }
                    }

                    // If we find a valid node ip, don't continue searching
                    if (ip != null)
                        break;
                }
            }

            // Finally, return the ip address of the node
            return ip;
        }

    }
}
