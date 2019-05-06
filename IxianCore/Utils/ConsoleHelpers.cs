﻿using DLT.Meta;
using System;
using System.Diagnostics;
using System.Security.Permissions;
using System.Text;
using System.Threading;

namespace IXICore.Utils
{
    class ConsoleHelpers
    {
        static public bool forceShutdown = false;

        // STD_INPUT_HANDLE (DWORD): -10 is the standard input device.
        const int STD_INPUT_HANDLE = -10;


        [SecurityPermission(SecurityAction.Demand, Flags = SecurityPermissionFlag.ControlAppDomain)]
        static void installUnhandledExceptionHandler()
        {
            System.AppDomain.CurrentDomain.UnhandledException += currentDomain_UnhandledException;
        }

        private static void currentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Logging.error(String.Format("Exception was triggered and not handled. Please send this log to the Ixian developers!"));
            Logging.error(e.ExceptionObject.ToString());
        }
        
        // Handle Windows OS-specific calls
        static public void prepareWindowsConsole()
        {
            // Ignore if we're on Mono
            if (IXICore.Platform.onMono())
                return;

            installUnhandledExceptionHandler();

            IntPtr consoleHandle = NativeMethods.GetStdHandle(STD_INPUT_HANDLE);

            // get current console mode
            uint consoleMode;
            if (!NativeMethods.GetConsoleMode(consoleHandle, out consoleMode))
            {
                // ERROR: Unable to get console mode.
                return;
            }

            // Clear the quick edit bit in the mode flags
            consoleMode &= ~(uint)0x0040; // quick edit

            // set the new mode
            if (!NativeMethods.SetConsoleMode(consoleHandle, consoleMode))
            {
                // ERROR: Unable to set console mode
            }

            // Hook a handler for force close
            NativeMethods.SetConsoleCtrlHandler(new NativeMethods.HandlerRoutine(IXICore.Utils.ConsoleHelpers.HandleConsoleClose), true);

        }

        static public void displayBackupText()
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("");
            Console.WriteLine("!! Always remember to keep a backup of your ixian.wal file and your password.");
            Console.WriteLine("!! In case of a lost file you will not be able to access your funds.");
            Console.WriteLine("!! Never give your ixian.wal and/or password to anyone.");
            Console.WriteLine("");
            Console.ResetColor();
        }

        // Requests the user to type a new password
        static public string requestNewPassword(string banner)
        {
            Console.WriteLine();
            Console.Write(banner);
            try
            {
                string pass = getPasswordInput();

                if (pass.Length < 10)
                {
                    Console.WriteLine("Password needs to be at least 10 characters. Try again.");
                    return "";
                }

                Console.Write("Type it again to confirm: ");

                string passconfirm = getPasswordInput();

                if (pass.Equals(passconfirm, StringComparison.Ordinal))
                {
                    return pass;
                }
                else
                {
                    Console.WriteLine("Passwords don't match, try again.");

                    // Passwords don't match
                    return "";
                }

            }
            catch (Exception)
            {
                // Handle exceptions
                return "";
            }
        }

        // Handles console password input
        static public string getPasswordInput()
        {
            StringBuilder sb = new StringBuilder();
            while (true)
            {
                if (forceShutdown)
                {
                    return "";
                }

                if (!Console.KeyAvailable)
                {
                    Thread.Yield();
                    continue;
                }

                ConsoleKeyInfo i = Console.ReadKey(true);
                if (i.Key == ConsoleKey.Enter)
                {
                    Console.WriteLine();
                    break;
                }
                else if (i.Key == ConsoleKey.Backspace)
                {
                    if (sb.Length > 0)
                    {
                        sb.Remove(sb.Length - 1, 1);
                        Console.Write("\b \b");
                    }
                }
                else if (i.KeyChar != '\u0000')
                {
                    sb.Append(i.KeyChar);
                    Console.Write("*");
                }
            }
            return sb.ToString();
        }

        static public bool HandleConsoleClose(NativeMethods.CtrlTypes type)
        {
            switch (type)
            {
                case NativeMethods.CtrlTypes.CTRL_C_EVENT:
                case NativeMethods.CtrlTypes.CTRL_BREAK_EVENT:
                case NativeMethods.CtrlTypes.CTRL_CLOSE_EVENT:
                case NativeMethods.CtrlTypes.CTRL_LOGOFF_EVENT:
                case NativeMethods.CtrlTypes.CTRL_SHUTDOWN_EVENT:
                    Config.verboseConsoleOutput = true;
                    Logging.consoleOutput = Config.verboseConsoleOutput;
                    Console.WriteLine();
                    Console.WriteLine("Application is being closed!");
                    Logging.info("Shutting down...");
                    Logging.flush();

                    Node.stop();
                    
                    // Wait (max 5 seconds) for everything to die
                    DateTime waitStart = DateTime.Now;
                    while (true)
                    {
                        if (Process.GetCurrentProcess().Threads.Count > 1)
                        {
                            Thread.Sleep(50);
                        }
                        else
                        {
                            Console.WriteLine(String.Format("Graceful shutdown achieved in {0} seconds.", (DateTime.Now - waitStart).TotalSeconds));
                            break;
                        }
                        if ((DateTime.Now - waitStart).TotalSeconds > 30)
                        {
                            Console.WriteLine("Unable to gracefully shutdown. Aborting. Threads that are still alive: ");
                            foreach (Thread t in Process.GetCurrentProcess().Threads)
                            {
                                Console.WriteLine(String.Format("Thread {0}: {1}.", t.ManagedThreadId, t.Name));
                            }
                            break;
                        }
                    }
                    return true;
            }
            return true;
        }
    }
}
