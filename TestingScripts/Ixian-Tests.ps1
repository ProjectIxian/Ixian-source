# Add-Test -TestName "DummyTestSimple" -Batch 0 -NumExtraSteps 0
# Add-Test -TestName "DummyTestExtraStepsNoWait" -Batch 0 -NumExtraSteps 2
# Add-Test -TestName "DummyTestExtraStepsWait" -Batch -NumExtraSteps 1
# Add-Test -TestName "DummyTestFailing" -Batch 0 -NumExtraSteps 0


function Init-DummyTestSimple {
    return [PSCustomObject]@{}
}

function Check-DummyTestSimple {
    Param(
        [PSCustomObject]$Data
    )
    return $Data
}

function Init-DummyTestExtraStepsNoWait {
    return [PSCustomObject]@{
        WaitBlockChange = $false
    }
}

function Step0-DummyTestExtraStepsNoWait {
    Param(
        [PSCustomObject]$Data
    )
    return $Data
}

function Step1-DummyTestExtraStepsNoWait {
    Param(
        [PSCustomObject]$Data
    )
    return $Data
}

function Check-DummyTestExtraStepsNoWait {
    Param(
        [PSCustomObject]$Data
    )
    return $Data
}

function Init-DummyTestExtraStepsWait {
    return [PSCustomObject]@{}
}

function Step0-DummyTestExtraStepsWait {
    Param(
        [PSCustomObject]$Data
    )
    return $Data
}

function Check-DummyTestExtraStepsWait {
    Param(
        [PSCustomObject]$Data
    )
    return $Data
}

function Init-DummyTestFailing {
    return [PSCustomObject]@{}
}

function Check-DummyTestFailing {
    Param(
        [PSCustomObject]$Data
    )
    return $null
}