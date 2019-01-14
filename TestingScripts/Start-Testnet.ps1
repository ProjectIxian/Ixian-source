Param(
    [int]$numInstances = 5,
    [switch]$ClearState,
    [switch]$StartNetwork,
    [switch]$CollectResults,
    [string]$IPInterface = ""
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

function Determine-LocalIP {
    $ipAddresses = Get-NetIPAddress -AddressFamily IPv4 -Type Unicast -AddressState Preferred | Sort-Object ifIndex
    foreach($ip in $ipAddresses) {
        $ip_object = New-Object System.Net.IPAddress ($ip)
        $octets = $ip_object.GetAddressBytes()
        if(($octets[0] -eq 10) -or 
            ($octets[0] -eq 172 -and $octets[1] -ge 16 -and $octets[1] -le 31) -or
            ($octets[0] -eq 192 -and $octets[1] -eq 168)) {

            # Is private address, we return the first one
            return $ip
        }
    }
    # No private IP address
    return ""
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
        $pi.CreateNoWindow = $true
        $p = New-Object System.Diagnostics.Process
        $p.StartInfo = $pi
        [void]$p.Start()
        $result = $p
    } catch {
        Write-Host -ForegroundColor Magenta "Error while starting client '$($Client)': $($_.Message)"
    }
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

###### Main ######
# DEBUG
[System.IO.Directory]::SetCurrentDirectory("C:\ZAGAR\Dev\Ixian_source\TestingScripts")
$wd = [System.IO.Directory]::GetCurrentDirectory()
Write-Host "Working directory: $($wd)"

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
                $ClientAddresses | Export-Clixml -Path "state.xml" -Force
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
            if($dstPaths.Count < 2) {
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
                        $startParams = ""
                        if($idx -eq 0) {
                            # Genesis node
                            $startParams = "-t -s -p $($dltPort) -a $($apiPort) -i $($IPInterface) --genesis 1000000 --genesis2 $($gen2Addr) --walletPassword $($WalletPassword)"
                        } else {
                            $startParams = "-t -s -p $($dltPort) -a $($apiPort) -i $($IPInterface) -n $($IPInterface):$($DLTStartPort) --walletPassword $($WalletPassword)"
                        }
                        Write-Host -ForegroundColor Cyan "Starting Client $($idx)..."
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
                        }
                        [void]$DLTProcesses.Add($dltClient)
                        Write-Host
                        $idx = $idx + 1
                    }
                    if($wasError) {
                        Write-Host -ForegroundColor Magenta "At least one error occured while starting the testnet. Aborting!"
                        Shutdown-TestNet -Clients $DLTProcesses
                        exit
                    }
                    Write-Host -ForegroundColor Cyan "Entering main loop..."
                    #Enter-MainDLTLoop
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