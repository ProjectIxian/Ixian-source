Param(
    [int]$NumInstances = 5,
    [int]$MinerOnlyInstances = 0,
    [switch]$ClearState,
    [switch]$StartNetwork,
    [switch]$CollectResults,
    [string]$IPInterface = "",
    [switch]$EnableMining, # Ignored if $MinerOnlyInstances > 0
    [switch]$DisplayWindows
)

$ClientBinary = "Debug"
$TestRoot = "TestClients"
$ResultsDir = "RunResults"
$DLTStartPort = 10000
$APIStartPort = 11000
$WalletPassword = "IXIANIXIANIXIANIXIAN"

$IgnoreList = @(
    ".+\.log",
    ".+\.wal",
    ".+\.wal\.bak",
    "peers.dat"
)

###### Libraries ######

. .\Testnet-Functions.ps1
. .\Display-Functions.ps1


###### Really low level keyboard hack (Windows only, for now) #####

function Check-Key {
    Param(
        [System.Windows.Forms.Keys]$Key
    )
    $Signature = @'
        [DllImport("user32.dll", CharSet=CharSet.Auto, ExactSpelling=true)] 
        public static extern short GetAsyncKeyState(int virtualKeyCode); 
'@

    $API = Add-Type -MemberDefinition $Signature -Name 'Keypress' -Namespace Keytest -PassThru

    if($API::GetAsyncKeyState($Key) -eq -32767) {
        return $true
    } else {
        return $false
    }
}

###### functions ######

function Check-FileIgnoreList {
    Param(
        [string]$File
    )
    foreach($ignorePattern in $IgnoreList) {
        $regex = New-Object System.Text.RegularExpressions.Regex($ignorePattern)
        if($regex.IsMatch($File)) {
            return $true
        }
    }
    return $false
}

function Synchronize-Folder {
    Param(
        [string]$SourceFolder,
        [string]$TargetFolder
    )
    if([System.IO.Directory]::Exists($SourceFolder)) {
        if([System.IO.Directory]::Exists($TargetFolder)) {
            $cDir = New-Object System.IO.DirectoryInfo($SourceFolder)
            foreach($file in $cDir.GetFiles()) {
                if(Check-FileIgnoreList -File $file.Name) {
                    continue
                }
                $tgtFile = "$($TargetFolder)\$($file.Name)"
                if([System.IO.File]::Exists($tgtFile)) {
                    $tfInfo = New-Object System.IO.FileInfo($tgtFile)
                    if($file.LastWriteTime -gt $tfInfo.LastWriteTime) {
                        [System.IO.File]::Copy($file.FullName, $tfInfo.FullName, $true)
                    }
                } else {
                    [System.IO.File]::Copy($file.FullName, $tgtFile)
                }
            }
            foreach($child_dir in $cDir.GetDirectories()) {
                $tgtDir = "$($TargetFolder)\$($child_dir.Name)"
                if([System.IO.Directory]::Exists($tgtDir) -eq $false) {
                    $dir = [System.IO.Directory]::CreateDirectory($tgtDir)
                }
                Synchronize-Folder -SourceFolder $child_dir.FullName -TargetFolder $tgtDir
            }
        } else {
            return "Target folder $($TargetFolder) does not exist!"
        }
    } else {
        return "Source folder $($SourceFolder) does not exist!"
    }
}

function Execute-Binary {
    Param(
        [string]$CWD,
        [string]$Binary,
        [string]$Parameters,
        [switch]$CaptureOutput
    )
    $old_wd = [System.IO.Directory]::GetCurrentDirectory()
    [System.IO.Directory]::SetCurrentDirectory($CWD)
    if([System.IO.File]::Exists($Binary)) {
        Write-Host -ForegroundColor Green "Preparing to start $($Binary)..."
        $pi = New-Object System.Diagnostics.ProcessStartInfo
        $pi.FileName = $Binary
        $pi.UseShellExecute = $false
        $pi.Arguments = $Parameters
        $pi.CreateNoWindow = $true
        $p = New-Object System.Diagnostics.Process
        $p.StartInfo = $pi
        [void]$p.Start()
        Write-Host -ForegroundColor Green "Waiting for $($Binary) to close..."
        try {
            Wait-Process -InputObject $p -Timeout 5
        } catch { }
        $output = ""
        if($p.HasExited -eq $false) {
            Write-Host -ForegroundColor Red "Process didn't exit. Terminating!"
            Stop-Process -Id $p.Id -Force
        } else {
            Write-Host "Process exit code is: $($p.ExitCode)"
            if([System.IO.File]::Exists("./ixian.log")) {
                $output = [System.IO.File]::ReadAllText("./ixian.log")
            } else {
                $output = "No log file generated."
            }
        }
        [System.IO.Directory]::SetCurrentDirectory($old_wd)
        return $output
    } else {
        [System.IO.Directory]::SetCurrentDirectory($old_wd)
        throw "Binary '$($CWD)\$($Binary)' does not exist!"
    }
}

function Start-DLTClient {
    Param(
        [string]$Client,
        [string]$StartupArgs
    )
    $clientBinary = "IxianDLT.exe"
    $cwd = [System.IO.Directory]::GetCurrentDirectory()
    [System.IO.Directory]::SetCurrentDirectory($Client)
    $result = $null
    try {
        $pi = New-Object System.Diagnostics.ProcessStartInfo
        $pi.FileName = $clientBinary
        $pi.UseShellExecute = $false
        $pi.Arguments = $StartupArgs
        if($DisplayWindows.IsPresent) {
            $pi.CreateNoWindow = $false
        } else {
            $pi.CreateNoWindow = $true
        }
        $p = New-Object System.Diagnostics.Process
        $p.StartInfo = $pi
        Write-Host -ForegroundColor Gray "Start-DLTClient: Commandline: $($clientBinary) $($StartupArgs)"
        [void]$p.Start()
        $result = $p
    } catch {
        Write-Host -ForegroundColor Magenta "Error while starting client '$($Client)': $($_.Message)"
    }
    [System.IO.Directory]::SetCurrentDirectory($cwd)
    return $result    
}

function Shutdown-TestNet {
    Param(
        [System.Collections.ArrayList]$Clients
    )
    Write-Host -ForegroundColor Cyan "Terminating TestNet..."
    # TODO: Attempt shutdown over API and wait for a while
    foreach($dltClient in $Clients) {
        Write-Host -ForegroundColor Yellow "-> [$($dltClient.idx)]: $($dltClient.Client)"
        if($dltClient.Process -ne $null) {
            $dltClient.Process.Kill()
        }
    }
    Write-Host -ForegroundColor Green "-> TestNet terminated!"
}

function Enter-MainDLTLoop {
    Param(
        [System.Collections.ArrayList]$Clients
    )
    try {
        while($true) {
            # status display
            Display-ClientStatus -Clients $Clients
            
            Start-Sleep -Seconds 2
            if(Check-Key -Key E) {
                Write-Host -ForegroundColor Magenta "Exit key pressed!"
                break
            }
        }
    } catch {
        Write-Host -ForegroundColor Yellow "Exception caught in main loop: $($_)"
    }
    Write-Host -ForegroundColor Cyan "Exiting..."
}

###### Main ######
# DEBUG
#[System.IO.Directory]::SetCurrentDirectory("C:\ZAGAR\Dev\Ixian_source\TestingScripts")
#[System.IO.Directory]::SetCurrentDirectory("D:\Dev_zagar\Ixian-source\TestingScripts")
$wd = pwd
# /DEBUG
Write-Host "Working directory: $($wd)"
[System.IO.Directory]::SetCurrentDirectory($wd)

$srcDir = "..\IxianDLT\bin\$($ClientBinary)"
$dstPaths = New-Object System.Collections.ArrayList

for($i = 0; $i -lt $numInstances; $i++) {
    $dpath = ".\$($TestRoot)\Client_$($i)"
    [void]$dstPaths.Add($dpath);
}

Write-Host -ForegroundColor Cyan "Checking for old TestClient Installations and updating..."
foreach($targetClient in $dstPaths) {
    if([System.IO.Directory]::Exists($targetClient)) {
        Write-Host "-> Updating $($targetClient)"
    } else {
        Write-Host "-> Creating $($targetClient)"
        $dir = [System.IO.Directory]::CreateDirectory($targetClient)
        Write-Host "-> Created: $($dir.FullName)"
    }
    Synchronize-Folder -SourceFolder $srcDir -TargetFolder $targetClient
}

if($ClearState.IsPresent) {
    Write-Host -ForegroundColor Yellow "Clearing network state!"
    foreach($targetClient in $dstPaths) {
        $tgtDir = New-Object System.IO.DirectoryInfo($targetClient)
        Write-Host "-> Clearing $($targetClient)"
        foreach($file in $tgtDir.GetFiles()) {
            if(Check-FileIgnoreList -File $file.Name) {
                $file.Delete()
            }
        }
    }
    if([System.IO.File]::Exists("state.xml")) {
        [System.IO.File]::Delete("state.xml")
    }
} else {
    $ClientAddresses = @{}
    if([System.IO.File]::Exists("state.xml")) {
        $ClientAddresses = Import-Clixml "state.xml"
    }
    Write-Host -ForegroundColor Cyan "Checking and generating wallets for all clients..."
    $any_failure = $false
    foreach($targetClient in $dstPaths) {
        if($ClientAddresses.ContainsKey($targetClient)) {
            Write-Host -ForegroundColor Cyan -NoNewline "Client '$($targetClient)' already has a wallet: "
            Write-Host -ForegroundColor Green "$($ClientAddresses[$targetClient])"
        } else {
            Write-Host -ForegroundColor Cyan "Generating address for client $($targetClient)..."
            $targetWD = $targetClient
            $targetExe = "IxianDLT.exe"
            $params = "-t --generateWallet --walletPassword $($WalletPassword)"
            $output = Execute-Binary -CWD $targetWD -Binary $targetExe -Parameters $params -CaptureOutput
            # Looking for log line like: 01-10 11:15:17.4001|info|(1): Public Node Address: 1STq7YC3y71uiN1QHbp8jFfVg3A1rfBe8qYytbgr2CNEKeXUD
            $pub_addr_r = New-Object System.Text.RegularExpressions.Regex(
                "^[0-9\-\ \:\.]+\|info\|\([0-9]+\)\: Public Node Address\: ([a-zA-Z0-9]+)",
                [System.Text.RegularExpressions.RegexOptions]::Multiline)
            $match = $pub_addr_r.Match($output)
            if($match.Success) {
                $pub_addr = $match.Groups[1].Value
                Write-Host -ForegroundColor Green "-> Done! Address: $($pub_addr)"
                $ClientAddresses.Add($targetClient, $pub_addr)
                $ClientAddresses | Export-Clixml -Path ".\state.xml" -Force
            } else {
                Write-Host -ForegroundColor Magenta "-> Error! Address was not generated, please check the log file!"
                $any_failure = $true
            }
        }
    }
    if($any_failure) {
        Write-Host -ForegroundColor Magenta "Something failed. Aborting."
    } else {
        # Start DLT network
        if($IPInterface -eq "") {
            Write-Host -ForegroundColor Cyan "Attempting to automatically discover local IP address..."
            $IPInterface = Determine-LocalIP
            if($IPInterface -eq "") {
                Write-Host -ForegroundColor Magenta "Unable to determine a valid private IP interface, please speficy -IPInterface on the commandline!"
                exit
            }
        }
        Write-Host -ForegroundColor Cyan "IP Interface over which the nodes will communicate: $($IPInterface)"
        if($StartNetwork.IsPresent) {
            if($dstPaths.Count -lt 2) {
                Write-Host -ForegroundColor Magenta "At least two instances are required to start the testnet!"
            } else {
                $gen2Addr = $ClientAddresses[$dstPaths[1]]
                if($gen2Addr -eq $null -or $gen2Addr -eq "") {
                    Write-Host -ForegroundColor Magenta "Error while reading client2 wallet address!"
                } else {
                    $DLTProcesses = New-Object System.Collections.ArrayList
                    $idx = 0
                    $wasError = $false
                    foreach($client in $dstPaths) {
                        $dltPort = $DLTStartPort + $idx
                        $apiPort = $APIStartPort + $idx
                        $clientAddr = $ClientAddresses[$client]
                        $startParams = ""
                        if($idx -eq 0) {
                            # Genesis node
                            $startParams = "-t -s -c -p $($dltPort) -a $($apiPort) -i $($IPInterface) --genesis 1000000 --genesis2 $($gen2Addr) --walletPassword $($WalletPassword) --disableWebStart"
                        } else {
                            $startParams = "-t -s -c -p $($dltPort) -a $($apiPort) -i $($IPInterface) -n $($IPInterface):$($DLTStartPort) --walletPassword $($WalletPassword) --disableWebStart"
                        }
                        if(-not $EnableMining.IsPresent) {
                           $startParams += " --disableMiner" 
                        }
                        Write-Host -ForegroundColor Cyan "Starting Client $($idx) - Address: $($clientAddr)..."
                        $dltClientProcess = Start-DLTClient -Client $client -StartupArgs $startParams
                        if($dltClientProcess -eq $null) {
                            Write-Host -ForegroundColor Magenta "-> There was an error starting client $($idx)!"
                            $wasError = $true
                            break
                        } else {
                            Write-Host -ForegroundColor Green "-> Client started!"
                        }
                        $dltClient = [PSCustomObject]@{
                            idx     = $idx
                            Client  = $client
                            Process = $dltClientProcess
                            DLTPort = $dltPort
                            APIPort = $apiPort
                            Address = $clientAddr
                        }
                        [void]$DLTProcesses.Add($dltClient)
                        Write-Host -ForegroundColor Yellow "-> Process ID: $($dltClientProcess.ID)"
                        $idx = $idx + 1
                        if($idx -eq 1) {
                            Write-Host -ForegroundColor Cyan "Waiting for genesis node to complete a few blocks..."
                            while($true) {
                                $ns = Get-DLTNodeStatus -Clients $DLTProcesses -NodeIdx 0
                                if($ns -eq $null) {
                                    Write-Host -ForegroundColor Magenta "Error while reading node status..."
                                    $wasError = $true
                                    break
                                }
                                if($ns.'Block Height' -lt 2) {
                                    Write-Host -ForegroundColor Green "-> Block Height: $($ns.'Block Height')"
                                } else {
                                    Write-Host -ForegroundColor Green "-> Block Height reached, proceeding."
                                    break                                    
                                }
                                Start-Sleep -Seconds 2
                            }
                        }
                        if($wasError -eq $false) {
                            if($idx -eq 2) {
                                # Leave two nodes running until they reach block #11
                                Write-Host -ForegroundColor Cyan "Waiting for the genesis nodes to reach block #11..."
                                while($true) {
                                    $ns = Get-DLTNodeStatus -Clients $DLTProcesses -NodeIdx 0
                                    if($ns -eq $null) {
                                        Write-Host -ForegroundColor Magenta "Error while reading node status..."
                                        $wasError = $true
                                        break
                                    }
                                    if($ns.'Block Height' -le 10) {
                                        Write-Host -ForegroundColor Green "-> Block Height: $($ns.'Block Height')"
                                    } else {
                                        Write-Host -ForegroundColor Green "-> Block Height reached, proceeding."
                                        break
                                    }
                                    Start-Sleep -Seconds 2
                                }
                            }
                        }
                        # early exit on error
                        if($wasError) { break }
                    }

                    $pendingTx = New-Object System.Collections.ArrayList
                    if($wasError -eq $false) {
                        # Put in transactions to give other seed nodes initial funds
                        Write-Host -ForegroundColor Cyan "Creating transactions to give other nodes required minimum funds..."
                        foreach($n in $DLTProcesses) {
                            if($n.idx -lt 2) { continue }
                            $tx = Send-Transaction -Clients $DLTProcesses -FromClient 0 -ToClient $n.idx -Amount 50000
                            if($tx -eq $null) {
                                Write-Host -ForegroundColor Magenta "Error sending initial funds to client $($n.idx). Aborting."
                                $wasError = $true
                                break
                            }
                            [void]$pendingTx.Add($tx)
                        }
                    }
                    if($wasError -eq $false) {
                        # wait for all pending transactions to clear
                        $pendTxCleared = WaitConfirm-PendingTX -Clients $DLTProcesses -TXList $pendingTx -Blocks 5
                        if($pendTxCleared -eq $false) {
                            Write-Host -ForegroundColor Magenta "Pending transctions did not clear within 5 blocks, aborting."
                            $wasError = $true
                        }
                    }

                    if($wasError) {
                        Write-Host -ForegroundColor Magenta "At least one error occured while starting the testnet. Aborting!"
                        Shutdown-TestNet -Clients $DLTProcesses
                        exit
                    }
                    Write-Host -ForegroundColor Cyan "Entering main loop..."
                    Enter-MainDLTLoop -Clients $DLTProcesses
                    Write-Host -ForegroundColor Cyan "Main loop finished. Terminating..."
                    Shutdown-TestNet -Clients $DLTProcesses
                }
            }
        } else {
            Write-Host -ForegroundColor Cyan "Option 'StartNetwork' was not specified, so the DLT network will not be started."
        }
        if($CollectResults.IsPresent) {
            Write-Host -ForegroundColor Cyan "Collecting DLT run results..."
            # Collect run results (Logs, wallets, peer lists, block databases)
        }
    }
}