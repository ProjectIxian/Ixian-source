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

    }
}
