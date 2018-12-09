using DLT.Meta;
using IXICore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S2.Network
{
    class Quota
    {
        // Stats that are reset on every successfull payment
        public long infoMessages = 0;
        public long dataMessages = 0;

        // Activity stats
        public long lastActivityTime = 0;
        public long lastPaidTime = 0;
    }

    public class QuotaManager
    {
        static Dictionary<byte[], Quota> quotas = new Dictionary<byte[], Quota>();

        // Clears all quotas
        public static void clearQuotas()
        {
            lock (quotas)
            {
                quotas.Clear();
            }
        }

        // Check if the provided wallet address has exceeded it's quota
        public static bool exceededQuota(byte[] wallet)
        {
            // Extract the quota if found
            if (quotas.ContainsKey(wallet))
            {
                Quota quota = quotas[wallet];

                if (Core.getCurrentTimestamp() - quota.lastPaidTime > Config.lastPaidTimeQuota)
                {
                    if (quota.infoMessages > Config.infoMessageQuota)
                    {
                        return false;
                    }

                    if (quota.dataMessages > Config.dataMessageQuota)
                    {

                        return false;
                    }
                }           
            }
            // If we reach this point, there is no quota for the specified wallet
            return false;
        }

        // Adds a quota activity for a specified wallet
        public static bool addActivity(byte[] wallet, bool info = true)
        {
            Quota quota = null;
            long current_timestamp = Core.getCurrentTimestamp();

            lock (quotas)
            {
                try
                {
                    // Extract the quota if found
                    if (quotas.ContainsKey(wallet))
                    {
                        quota = quotas[wallet];
                    }

                    // Generate a new quota if not found
                    if (quota == null)
                        quota = new Quota();

                    if (info == true)
                    {
                        quota.infoMessages++;
                    }
                    else
                    {
                        quota.dataMessages++;
                    }

                    quota.lastActivityTime = current_timestamp;
                }
                catch(Exception e)
                {
                    Logging.warn("Quota Exception: {0}", e.Message);
                    return false;
                }
            }

            return true;
        }

        // Adds a valid payment, resetting the quota for the specified wallet
        public static bool addValidPayment(byte[] wallet)
        {
            Quota quota = null;
            // Extract the quota if found
            if (quotas.ContainsKey(wallet))
            {
                quota = quotas[wallet];
                if (quota == null)
                    return false;

                long current_timestamp = Core.getCurrentTimestamp();

                // Reset quotas for this wallet
                quota.lastActivityTime = current_timestamp;
                quota.lastPaidTime = current_timestamp;
                quota.infoMessages = 0;
                quota.dataMessages = 0;
            }

            // If we reach this point, there is no quota for the specified wallet
            return false;
        }


    }
}
