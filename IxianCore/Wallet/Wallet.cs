using System.IO;

namespace DLT
{
    public class Wallet
    {
        public string id; // 36 B (18 B)
        public IxiNumber balance; // 16 B
        public string data; // 0 B

        // TOTAL: 52 B (34 B)

        public Wallet()
        {
            id = "";
            balance = new IxiNumber();
            data = "";
        }

        public Wallet(string w_id, IxiNumber w_balance)
        {
            id = w_id;
            balance = w_balance;
            data = "";
        }
        
        public Wallet(Wallet wallet)
        {
            id = wallet.id;
            balance = wallet.balance;
            data = wallet.data;
        }

        public Wallet(byte[] bytes)
        {
            using (MemoryStream m = new MemoryStream(bytes))
            {
                using (BinaryReader reader = new BinaryReader(m))
                {
                    id = reader.ReadString();
                    string balance_str = reader.ReadString();
                    balance = new IxiNumber(balance_str);
                    data = reader.ReadString();
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
                    writer.Write(balance.ToString());
                    writer.Write(data);
                }
                return m.ToArray();
            }
        }

        public string calculateChecksum()
        {
            string baseData = id + balance.ToString() + data;
            return Crypto.sha256(baseData);
        }



    }
}