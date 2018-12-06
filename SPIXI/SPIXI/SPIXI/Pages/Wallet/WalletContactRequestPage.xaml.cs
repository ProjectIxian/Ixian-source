﻿using DLT;
using DLT.Meta;
using DLT.Network;
using IXICore;
using SPIXI.Interfaces;
using SPIXI.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace SPIXI
{
	[XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class WalletContactRequestPage : SpixiContentPage
	{
        private Friend friend = null;
        private string amount = null;
        private string date = null;

        public WalletContactRequestPage (Friend fr, string am, string dt)
		{
			InitializeComponent ();
            NavigationPage.SetHasNavigationBar(this, false);

            friend = fr;
            amount = am;
            date = dt;

            // Load the platform specific home page url
            var source = new UrlWebViewSource();
            source.Url = string.Format("{0}html/wallet_contact_request.html", DependencyService.Get<IBaseUrl>().Get());
            webView.Source = source;
        }

        private void onNavigated(object sender, WebNavigatedEventArgs e)
        {
            // Deprecated due to WPF, use onLoad
        }

        private void onLoad()
        {
            webView.Eval(string.Format("setData('{0}','{1}','{2}','{3}','{4}')", Base58Check.Base58CheckEncoding.EncodePlain(friend.walletAddress), friend.nickname, amount, CoreConfig.transactionPrice.ToString(), date));
        }

        private void onNavigating(object sender, WebNavigatingEventArgs e)
        {
            string current_url = e.Url;

            if (current_url.Equals("ixian:onload", StringComparison.Ordinal))
            {
                onLoad();
            }
            else if (current_url.Equals("ixian:back", StringComparison.Ordinal))
            {
                Navigation.PopAsync(Config.defaultXamarinAnimations);
            }
            else if (current_url.Equals("ixian:send", StringComparison.Ordinal))
            {
                onSend();
            }
            else
            {
                // Otherwise it's just normal navigation
                e.Cancel = false;
                return;
            }
            e.Cancel = true;

        }

        private void onSend()
        {
            // Create an ixian transaction and send it to the dlt network
            byte[] to = friend.walletAddress;

            IxiNumber amounti = new IxiNumber(amount);
            IxiNumber fee = CoreConfig.transactionPrice;
            byte[] from = Node.walletStorage.getWalletAddress();
            byte[] pubKey = Node.walletStorage.publicKey;

            Transaction transaction = new Transaction((int)Transaction.Type.Normal, amount, fee, to, from, null, pubKey, Node.getLastBlockHeight());

            NetworkClientManager.broadcastData(ProtocolMessageCode.newTransaction, transaction.getBytes());

            // Add the unconfirmed transaction the the cache
            TransactionCache.addUnconfirmedTransaction(transaction);
            FriendList.addMessageWithType(FriendMessageType.sentFunds, friend.walletAddress, transaction.id);
            Navigation.PopAsync(Config.defaultXamarinAnimations);

        }


    }
}