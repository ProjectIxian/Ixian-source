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

function Add-ParallelTest {
    Param(
        [System.Collections.ArrayList]$PTests,
        [int]$Batch,
        [string]$TestName
    )
    if($Batch -lt 0) {
        $Batch = 0
    }
    if($Batch -ge $PTests.Count) {
        $Batch = $PTests.Count
        $na = New-Object System.Collections.ArrayList
        [void]$PTests.Add($na)
    }
    Add-Test -Tests $PTests[$Batch] -TestName $TestName
}

function WaitFor-NextBlock {
    $bh = Get-CurrentBlockHeight -APIPort $APIStartPort
    while($true) {
        $nbh = Get-CurrentBlockHeight -APIPort $APIStartPort
        if($nbh -gt $bh) {
            return
        }
        Start-Sleep -Seconds 2
    }
}


###### Main ######

Write-Host -ForegroundColor White "Preparing tests..."

$Tests = New-Object System.Collections.ArrayList

Add-ParallelTest -PTests $Tests -Batch 0 -TestName "WalletStateIsMaintained"
Add-ParallelTest -PTests $Tests -Batch 0 -TestName "BasicTX"


Write-Host -ForegroundColor Green "Done."

Write-Host -ForegroundColor White "Executing..."

$succeeded_tests = 0
$failed_tests = 0
$failed_test_names = New-Object System.Collections.ArrayList

for($batch = 0; $batch -lt $Tests.Count; $batch++) {
    Write-Host -ForegroundColor Cyan -NoNewline "Initializing batch "
    Write-Host -ForegroundColor Green "$($batch)..."

    $current_tests = @{}

    foreach($t in $Tests[$batch]) {
        Write-Host -ForegroundColor Yellow -NoNewline "- $($t) : "
        $test_data = &"Init-$($t)" -APIPort $APIStartPort
        if($test_data -eq $null) {
            Write-Host -ForegroundColor Red "Unable to initialize test $($t)"
            $failed_tests++
            [void]$failed_test_names.Add($t)
            continue
        }
        [void]$current_tests.Add($t, $test_data)
    }

    Write-Host -ForegroundColor Cyan -NoNewline "Checking batch "
    Write-Host -ForegroundColor Green "$($batch)..."

    while($current_tests.Count -gt 0) {
        foreach($t in $current_tests.Keys) {
            $td = $current_tests[$t]
            $test_result = &"Check-$($t)" -APIPort $APIStartPort $test_data
            if($test_result -eq "WAIT") {
                # do nothing this iteration
                continue
            } elseif($test_result -eq "OK") {
                Write-Host -ForegroundColor Green "-> OK: $($t)"
            } else {
                Write-Host -ForegroundColor Red "FAIL: $($test_r)"
                $failed_tests++
                [void]$failed_test_names.Add($t)
            }
        }
        if($current_tests.Count -gt 0) {
            WaitFor-NextBlock
        }
    }
    Write-Host -ForegroundColor Green "Batch complete."
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