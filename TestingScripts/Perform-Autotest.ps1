Param(
    [int]$APIStartPort = 11000
)

###### Libraries ######
. .\Testnet-Functions.ps1
. .\Ixian-Tests.ps1


# Global stuff
$TestBatches = New-Object System.Collections.ArrayList
$InitError = $false

$FailedTestNames = New-Object System.Collections.ArrayList
$SucceededTestNames = New-Object System.Collections.ArrayList


## Test PSObject

# [PSCustomObject]@{
#     Name = [String]
#     Steps = [Number]
# }
# Functions which must exist:
# `Init-Name`  , step initialization
# `StepX-Name` , where X [1..Steps] (inclusive). If Steps = 0, no Step functions need exist
# `Check-Name` , step final check for success/failure

## Return from each test step:
# [PSCustomObject]@{
#     Name = [String]
#     BH = [Number] , Block Height when the step was performed
#     Steps = [Number] , number of steps this test must go through
#     CurrentStep = [Number] , step this test is currently on
#     WaitBlockChange = [Boolean] , if $true, the next step won't be scheduled until the block number changes
#     ... , own data
# }

# All tests in a single batch are run in parallel, batches are completely one by one sequentially
# Batches are numbered from 0 forward

###### Functions ######

function Function-Exists {
    Param(
        [string]$Name
    )
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'stop'
    try {
        Get-Command -Name $Name
        return $true
    } catch {
        return $false
    } finally {
        $ErrorActionPreference = $old
    }
}

function Extend-TestBatches {
    Param(
        [int]$Batch
    )
    while($TestBatches.Count -le $Batch) {
        $batchArray = New-Object System.Collections.ArrayList
        [void]$TestBatches.Add($batchArray)
    }
}

function Add-Test {
    Param(
        [string]$TestName,
        [int]$Batch,
        [int]$NumExtraSteps
    )
    Extend-TestBatches -Batch $Batch
    # Check if all required functions exist
    if((Function-Exists -Name "Init-$($TestName)") -eq $false) {
        Write-Host -ForegroundColor Red "Initialization function missing: 'Init-$($TestName)'"
        $InitError = $true
        return
    }
    if((Function-Exists -Name "Check-$($TestName)") -eq $false) {
        Write-Host -ForegroundColor Red "Check function missing: 'Check-$($TestName)'"
        $InitError = $true
        return
    }
    for($step = 0; $step -lt $NumExtraSteps; $step++) {
        if((Function-Exists -Name "Step$($step)-$($TestName)") -eq $false) {
            Write-Host -ForegroundColor Red "Step function missing: 'Step$($step)-$($TestName)'"
            $InitError = $false
            return
        }
    }

    # Create the appropriate test descriptor
    $td = [PSCustomObject]@{
        Name = $TestName
        Steps = $NumExtraSteps
    }
    [void]$TestBatches[$Batch].Add($td)
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

function Process-TestBatch {
    Param(
        [System.Collections.ArrayList]$Batch
    )
    Write-Host -ForegroundColor Cyan "-> Initializing..."
    $RemainingTests = New-Object System.Collections.ArrayList
    foreach($td in $Batch) {
        $test_return = & "Init-$($td.Name)"
        if($test_return -eq $null) {
            Write-Host -ForegroundColor Yellow "-> $($td.Name)"
            [void]$FailedTestNames.Add($td.Name)
        }
        $test_return | Add-Member -Name 'Name' -Type NoteProperty -Value $td.Name
        $test_return | Add-Member -Name 'BH' -Type NoteProperty -Value (Get-CurrentBlockHeight -APIPort $APIStartPort)
        $test_return | Add-Member -Name 'Steps' -Type NoteProperty -Value $td.Steps
        $test_return | Add-Member -Name 'CurrentStep' -Type NoteProperty -Value 0
        if(([bool]($test_return.PSObject.Properties.name -match "WaitBlockChange")) -eq $false) {
            $test_return | Add-Member -Name 'WaitBlockChange' -Type NoteProperty -Value $true
        }
        [void]$RemainingTests.Add($test_return)
        Write-Host -ForegroundColor Green "-> $($td.Name)"
    }
    Write-Host -ForegroundColor Cyan "-> Performing intermediate steps..."
    $DoneSteps = New-Object System.Collections.ArrayList
    while($true) {
        if($RemainingTests.Count -eq 0) {
            # we are done with it all
            break
        }
        $tests_were_run = $false
        for($i = 0; $i -lt $RemainingTests.Count; $i++) {
            $tr1 = $RemainingTests[$i]
            if($tr1.CurrentStep -lt $tr1.Steps) {
                # Test has more steps to run
                if($tr1.WaitBlockChange -eq $false -or $tr1.BH -lt (Get-CurrentBlockHeight -APIPort $APIStartPort)) {
                    # it can be run right away
                    $tests_were_run = $true
                    $tr2 = & "Step$($tr1.CurrentStep)-$($tr1.Name)" -Data $tr1
                    # null return means a failure
                    if($tr2 -eq $null) {
                        Write-Host -ForegroundColor Yellow " -X $($tr1.Name)"
                        [void]$FailedTestNames.Add($tr1.Name)
                    } else {
                        Write-Host -ForegroundColor Green " -> $($tr1.Name), Step $($tr1.CurrentStep)"
                        # check if there are more steps
                        $tr2.CurrentStep++
                        $tr2.BH = Get-CurrentBlockHeight -APIPort $APIStartPort
                    }
                }
            } else {
                # Test has no more intermediate steps
                $RemainingTests.RemoveAt($i)
                $i--
                [void]$DoneSteps.Add($tr1)
            }
        }
        # if no tests were run, we probably need to wait for the next block
        if($tests_were_run -eq $false) {
            if($RemainingTests.Count -gt 0) {
                Write-Host -ForegroundColor Gray " -> Waiting for next block..."
                WaitFor-NextBlock
            }
        }
    }

    Write-Host -ForegroundColor Cyan "-> Performing final check..."
    foreach($tr1 in $DoneSteps) {
        $tr2 = & "Check-$($tr1.Name)" -Data $tr1
        if($tr2 -eq $null) {
            Write-Host -ForegroundColor Yellow " -X $($tr1.Name)"
            [void]$FailedTestNames.Add($tr1.Name)
        } else {
            # test is successful...
            Write-Host -ForegroundColor Green " -> $($tr1.Name)"
            [void]$SucceededTestNames.Add($tr1.Name)
        }
    }
}


###### Main ######

Write-Host -ForegroundColor White "Preparing tests..."

Add-Test -TestName "DummyTestSimple" -Batch 0 -NumExtraSteps 0
Add-Test -TestName "DummyTestExtraStepsNoWait" -Batch 0 -NumExtraSteps 2
Add-Test -TestName "DummyTestExtraStepsWait" -Batch 0 -NumExtraSteps 1
Add-Test -TestName "DummyTestFailing" -Batch 0 -NumExtraSteps 0

if($InitError) {
    Write-Host -ForegroundColor Magenta "Initialization error occured. Aborting."
    exit(-1)
}

$i = 0
foreach($Batch in $TestBatches) {
    Write-Host -ForegroundColor Cyan "Processing batch $($i)"
    Process-TestBatch -Batch $Batch
}

Write-Host -ForegroundColor White -NoNewline "Results: Succeeded: "
Write-Host -ForegroundColor Green -NoNewline $SucceededTestNames.Count
Write-Host -ForegroundColor White -NoNewline " / Failed: "
Write-Host -ForegroundColor Red -NoNewline $FailedTestNames.Count
Write-Host -ForegroundColor White "."

if($FailedTestNames.Count -gt 0) {
    Write-Host -ForegroundColor White "Failing tests:"
    foreach($tn in $FailedTestNames) {
        Write-Host -ForegroundColor Red "-> $($tn)"
    }
}