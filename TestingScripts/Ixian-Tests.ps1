function Init-WalletStateIsMaintained {
    Param(
        [int]$APIPort
    )
    $wallets = Invoke-DLTApi -APIPort $APIPort -Command "walletlist"
    $resulting_wallets = @{}
    foreach($w in $wallets) {
        [void]$resulting_wallets.Add($w.id, $2.balance)
    }
    return [PSCustomObject]@{
        BH = Get-CurrentBlockHeight
        wallets = $resulting_wallets
    }
}

function Check-WalletStateIsMaintained {
    Param(
        [int]$APIPort,
        $test_data
    )
    $wallets = Invoke-DLTApi -APIPort $APIPort -Command "walletlist"
    $oldbh = $test_data.BH
    $newbh = Get-CurrentBlockHeight
    if($newbh -le $oldbh) {
        return "WAIT"
    }
    foreach($id in $test_data.wallets.Keys) {
        $next_w = $wallets | Where-Object { $_.id -eq $id }
        if($next_w -eq $null) {
            return $false, "Wallet $($id) did not persist over block generation."
        }
        if($next_w.balance -lt $test_data[$id]) {
            return "Wallet $($id) unexpectedly lost balance in the past block. Before = $($test_data[$id]), Now = $($next_w.balance)"
        }
    }
    return "OK"
}