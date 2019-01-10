Param(
    [int]$numInstances = 5,
    [switch]$ClearState,
    [switch]$StartNetwork,
    [switch]$CollectResults
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
    ".+\.wal\.bak"
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
        # proceed running the simulation
    }
}