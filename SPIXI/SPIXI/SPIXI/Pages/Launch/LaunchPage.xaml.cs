﻿using System;
using SPIXI.Interfaces;

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;
using DLT.Meta;

namespace SPIXI
{
	[XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class LaunchPage : SpixiContentPage
	{
		public LaunchPage ()
		{
			InitializeComponent ();
            NavigationPage.SetHasNavigationBar(this, false);

            // Load the platform specific home page url
            var source = new UrlWebViewSource();
            source.Url = string.Format("{0}html/intro.html", DependencyService.Get<IBaseUrl>().Get());
            Logging.info(source.Url);
            webView.Source = source;

        }

        private void onNavigated(object sender, WebNavigatedEventArgs e)
        {

        }

        private void onNavigating(object sender, WebNavigatingEventArgs e)
        {
            string current_url = e.Url;
            
            if (current_url.Equals("ixian:create", StringComparison.Ordinal))
            {
                Navigation.PushAsync(new LaunchCreatePage(), Config.defaultXamarinAnimations);
            }
            else if (current_url.Equals("ixian:restore", StringComparison.Ordinal))
            {
                Navigation.PushAsync(new LaunchRestorePage(), Config.defaultXamarinAnimations);
            }
            else
            {
                // Otherwise it's just normal navigation
                e.Cancel = false;
                return;
            }
            e.Cancel = true;

        }

        public void onCreateAccount(object sender, EventArgs e)
        {
            Navigation.PushAsync(new LaunchCreatePage(), Config.defaultXamarinAnimations);
        }

        public void onRestoreAccount(object sender, EventArgs e)
        {
            Navigation.PushAsync(new LaunchRestorePage(), Config.defaultXamarinAnimations);
        }

    }
}