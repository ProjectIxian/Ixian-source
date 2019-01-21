Param(
    [int]$APIStartPort = 11000
)

###### Libraries ######
. .\Testnet-Functions.ps1
. .\Ixian-Tests.ps1

###### Functions ######

function Add-Test {
    Param(
        [System.Collections.ArrayList]$Tests,
        [string]$TestName
    )
    $test_funcs = Get-Command -Noun $TestName
    if( ($test_funcs | Where-Object { $_.Verb -eq "Init" }).Count -lt 1) {
        Write-Host -ForegroundColor Red "-> Test $($TestName) is missing the Init function Init-$($TestName)"
        return
    }
    if( ($test_funcs | Where-Object { $_.Verb -eq "Check" }).Count -lt 1) {
        Write-Host -ForegroundColor Red "-> Test $($TestName) is missing the Init function Check-$($TestName)"
        return
    }
    [void]$Tests.Add($TestName)
    Write-Host -ForegroundColor Cyan "-> $($TestName) added"
}

function WaitFor-NextBlock {
    $bh = Get-CurrentBlockHeight
    while($true) {
        $nbh = Get-CurrentBlockHeight
        if($nbh -gt $bh) {
            return
        }
        Start-Sleep -Seconds 2
    }
}


###### Main ######

Write-Host -ForegroundColor White "Preparing tests..."

$Tests = New-Object System.Collections.ArrayList
Add-Test -Tests $Tests -TestName "WalletStateIsMaintained"


Write-Host -ForegroundColor Green "Done."

Write-Host -ForegroundColor White "Executing..."

$succeeded_tests = 0
$failed_tests = 0
$failed_test_names = New-Object System.Collections.ArrayList

foreach($t in $Tests) {
    Write-Host -ForegroundColor Yellow -NoNewline "- $($t) : "
    $test_data = &"Init-$($t)" -APIPort $($APIStartPort)
    while($true) {
        $test_r = &"Check-$($t)" -APIPort $($APIStartPort) $test_data
        if($test_r -eq "WAIT") {
            WaitFor-NextBlock
        } elseif($test_r -eq "OK") {
            Write-Host -ForegroundColor Green "OK"
            $succeeded_tests++
            break
        } else {
            Write-Host -ForegroundColor Red "FAIL: $($test_r)"
            $failed_tests++
            [void]$failed_test_names.Add($t)
        }
    }
}

Write-Host -ForegroundColor White -NoNewline "Results: Succeeded: "
Write-Host -ForegroundColor Green -NoNewline $succeeded_tests
Write-Host -ForegroundColor White -NoNewline " / Failed: "
Write-Host -ForegroundColor Red -NoNewline $failed_tests
Write-Host -ForegroundColor White "."

if($failed_tests -gt 0) {
    Write-Host -ForegroundColor White "Failing tests:"
    foreach($tn in $failed_test_names) {
        Write-Host -ForegroundColor Red "-> $($tn)"
    }
}