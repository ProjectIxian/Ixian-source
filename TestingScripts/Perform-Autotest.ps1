Param(
    [int]$APIStartPort = 11000
)

###### Libraries ######
. .\Testnet-Functions.ps1
. .\Ixian-MS-Tests.ps1


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

function Fill-ResultMembers {
    Param(
        [PSCustomObject]$Result,
        [string]$Name,
        [long]$BH,
        [int]$Steps,
        [int]$CurrentStep
    )

    if(([bool]($Result.PSObject.Properties.name -match "Name")) -eq $false) {
        $Result | Add-Member -Name 'Name' -Type NoteProperty -Value $Name
    } else {
        $Result.Name = $Name
    }
    if(([bool]($Result.PSObject.Properties.name -match "BH")) -eq $false) {
        $Result | Add-Member -Name 'BH' -Type NoteProperty -Value $BH
    } else {
        $Result.BH = $BH
    }
    if(([bool]($Result.PSObject.Properties.name -match "Steps")) -eq $false) {
        $Result | Add-Member -Name 'Steps' -Type NoteProperty -Value $Steps
    } else {
        $Result.Steps = $Steps
    }
    if(([bool]($Result.PSObject.Properties.name -match "CurrentStep")) -eq $false) {
        $Result | Add-Member -Name 'CurrentStep' -Type NoteProperty -Value $CurrentStep
    } else {
        $Result.CurrentStep = $CurrentStep
    }
    if(([bool]($Result.PSObject.Properties.name -match "WaitBlockChange")) -eq $false) {
        $Result | Add-Member -Name 'WaitBlockChange' -Type NoteProperty -Value $true
    }
    # We do not override WaitBlockChange, if it is already present
    return $Result
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
        $test_return = Fill-ResultMembers -Result $test_return -Name $td.Name -BH (Get-CurrentBlockHeight -APIPort $APIStartPort) -Steps $td.Steps -CurrentStep 0
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
                # Test has more intermediate steps to run
                if($tr1.WaitBlockChange -eq $false -or $tr1.BH -lt (Get-CurrentBlockHeight -APIPort $APIStartPort)) {
                    # it can be run right away
                    $tests_were_run = $true
                    $tr2 = & "Step$($tr1.CurrentStep)-$($tr1.Name)" -Data $tr1
                    $delayed = $false
                    # a "DELAY" result means we repeat this step
                    if(([bool]($tr2.PSObject.Properties.name -match "DELAY")) -eq $true) {
                        $delayed = $true
                        # if $tr1 already has a delay, we decrement it
                        if(([bool]($tr1.PSObject.Properties.name -match "DELAY")) -eq $true) {
                            $tr1.DELAY--
                            if($tr1.DELAY -le 0) {
                                #time is up, so we fail the test
                                $tr2 = $null
                            }
                        } else {
                            # this is the first time this step was delayed
                            $tr1 | Add-Member -Name 'DELAY' -Type NoteProperty -Value $tr2.DELAY
                        }
                    }
                    # null return means a failure
                    if($tr2 -eq $null) {
                        Write-Host -ForegroundColor Yellow " -X $($tr1.Name)"
                        # remove failed test from the queue
                        $RemainingTests.RemoveAt($i)
                        $i--
                        [void]$FailedTestNames.Add($tr1.Name)
                    } elseif($delayed -eq $true) {
                        # basically do nothing, just go another circle - because a test was run, we'll wait for the next block automatically
                        Write-Host -ForegroundColor White " -> $($tr1.Name) DELAYED($($tr1.DELAY))"
                    } else {
                        Write-Host -ForegroundColor Green " -> $($tr1.Name), Step $($tr1.CurrentStep)"
                        $tr2 = Fill-ResultMembers -Result $tr2 -Name $tr1.Name -BH (Get-CurrentBlockHeight -APIPort $APIStartPort) -Steps $tr1.Steps -CurrentStep $tr1.CurrentStep
                        $tr2.CurrentStep++
                        $RemainingTests[$i] = $tr2
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
    # Wait block?

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

#Add-Test -TestName "MSGenerateSecondaryWallets" -Batch 0 -NumExtraSteps 1
Add-Test -TestName "MSPreparePrimaryWallets" -Batch 0 -NumExtraSteps 1
Add-Test -TestName "MSSetOwner" -Batch 1 -NumExtraSteps 0
Add-Test -TestName "MSAddKey" -Batch 2 -NumExtraSteps 1
Add-Test -TestName "MSSendTxSimple" -Batch 3 -NumExtraSteps 1
Add-Test -TestName "MSSendTxMulti" -Batch 4 -NumExtraSteps 1
Add-Test -TestName "MSDelKey" -Batch 5 -NumExtraSteps 1
Add-Test -TestName "MSAddKey" -Batch 6 -NumExtraSteps 1
Add-Test -TestName "MSChangeReqSigs" -Batch 7 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner1" -Batch 8 -NumExtraSteps 0
Add-Test -TestName "MSAddKey" -Batch 9 -NumExtraSteps 1
Add-Test -TestName "MSSetOwner" -Batch 10 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 11 -NumExtraSteps 1
Add-Test -TestName "MSSendTxSimple" -Batch 12 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner1" -Batch 13 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 14 -NumExtraSteps 1
Add-Test -TestName "MSSetOwner" -Batch 15 -NumExtraSteps 0
Add-Test -TestName "MSSendTxMulti" -Batch 16 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner1" -Batch 17 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 18 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner2" -Batch 19 -NumExtraSteps 0
Add-Test -TestName "MSChangeReqSigs" -Batch 20 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner1" -Batch 21 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 22 -NumExtraSteps 1
Add-Test -TestName "MSSendTxSimple" -Batch 23 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner2" -Batch 24 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 25 -NumExtraSteps 1
Add-Test -TestName "MSSetOwner" -Batch 26 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 27 -NumExtraSteps 1
Add-Test -TestName "MSSendTxMulti" -Batch 28 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner2" -Batch 29 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 30 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner1" -Batch 31 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 32 -NumExtraSteps 1
Add-Test -TestName "MSDelKey" -Batch 33 -NumExtraSteps 1
Add-Test -TestName "MSSetOwner" -Batch 34 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 35 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner2" -Batch 36 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 37 -NumExtraSteps 1

# test from here must totally fail
Add-Test -TestName "MSSendTxMulti" -Batch 38 -NumExtraSteps 1
Add-Test -TestName "MSDelKey" -Batch 39 -NumExtraSteps 1
Add-Test -TestName "MSAddKey" -Batch 40 -NumExtraSteps 1
Add-Test -TestName "MSChangeReqSigs" -Batch 41 -NumExtraSteps 1

# test from here - first two in set must pass, second two must fail
Add-Test -TestName "MSSetOwner" -Batch 42 -NumExtraSteps 0
Add-Test -TestName "MSSendTxMulti" -Batch 43 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner2" -Batch 44 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 45 -NumExtraSteps 1

Add-Test -TestName "MSSetOwner" -Batch 46 -NumExtraSteps 0
Add-Test -TestName "MSChangeReqSigs" -Batch 47 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner2" -Batch 48 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 49 -NumExtraSteps 1

Add-Test -TestName "MSSetOwner" -Batch 50 -NumExtraSteps 0
Add-Test -TestName "MSDelKey" -Batch 51 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner2" -Batch 52 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 53 -NumExtraSteps 1

Add-Test -TestName "MSSetSigner1" -Batch 54 -NumExtraSteps 0
Add-Test -TestName "MSAddKey" -Batch 55 -NumExtraSteps 1
Add-Test -TestName "MSSetSigner2" -Batch 56 -NumExtraSteps 0
Add-Test -TestName "MSAddSignature" -Batch 57 -NumExtraSteps 1




if($InitError) {
    Write-Host -ForegroundColor Magenta "Initialization error occured. Aborting."
    exit(-1)
}

$i = 0
foreach($Batch in $TestBatches) {
    Write-Host -ForegroundColor Cyan "Processing batch $($i)"
    Process-TestBatch -Batch $Batch
    $i++
    Start-Sleep -Seconds 5
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