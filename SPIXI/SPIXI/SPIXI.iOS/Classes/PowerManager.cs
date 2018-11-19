﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Foundation;
using UIKit;
using SPIXI.Interfaces;
using Xamarin.Forms.Platform.iOS;
using Xamarin.Forms;

[assembly: Dependency(typeof(PowerManager_iOS))]


public class PowerManager_iOS : IPowerManager
{
    public bool AquireLock()
    {
        UIApplication.SharedApplication.IdleTimerDisabled = true;
        return true;
    }

    public bool ReleaseLock()
    {
        UIApplication.SharedApplication.IdleTimerDisabled = false;
        return true;
    }
}