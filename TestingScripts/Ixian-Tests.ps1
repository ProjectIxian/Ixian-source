function Init-WalletStateIsMaintained {
    Param(
        [int]$APIPort
    )
    $wallets = Invoke-DLTApi -APIPort $APIPort -Command "walletlist"
    if($wallets -eq $null) {
        Write-Host -ForegroundColor Red "Unable to read the wallet list from a DLT node - is the testnet running?"
        return $null
    }
    $resulting_wallets = @{}
    foreach($w in $wallets) {
        [void]$resulting_wallets.Add($w.id, $2.balance)
    }
    return [PSCustomObject]@{
        BH = Get-CurrentBlockHeight -APIPort $APIPort
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
    $newbh = Get-CurrentBlockHeight -APIPort $APIPort
    if($newbh -le $oldbh) {
        return "WAIT"
    }
    foreach($id in $test_data.wallets.Keys) {
        $next_w = $wallets | Where-Object { $_.id -eq $id }
        if($next_w -eq $null) {
            return "Wallet $($id) did not persist over block generation."
        }
        if($next_w.balance -lt $test_data[$id]) {
            return "Wallet $($id) unexpectedly lost balance in the past block. Before = $($test_data[$id]), Now = $($next_w.balance)"
        }
    }
    return "OK"
}

function Init-BasicTX {
    Param(
        [int]$APIPort
    )
    $random_src_node = Choose-RandomNode -APIPort $APIPort
    if($random_src_node -eq $null) {
        Write-Host -ForegroundColor Red "Unable to select a random source DLT node - is the testnet running?"
        return $null
    }
    $random_dst_node = Choose-RandomNode -APIPort $APIPort
    if($random_dst_node -eq $null) {
        Write-Host -ForegroundColor Red "Unable to select a random destination DLT node - is the testnet running?"
        return $null
    }
    $orig_wallet_list = Invoke-DLTApi -APIPort $random_src_node -Command "mywallet"
    $orig_wallet = ""
    $amnt = 0
    $src_balance_old = 0
    # returns dictionary with wallet addr : balance
    foreach($p in $orig_wallet_list.PSObject.Properties) {
        $addr = $p.Name
        if($orig_wallet_list.$addr -gt 0) {
            $orig_wallet = $addr
            $src_balance_old = ($orig_wallet_list.$addr)
            $amnt = ($orig_wallet_list.$addr) / 2
        }
    }
    if($orig_wallet -eq "") {
        Write-Host -ForegroundColor Red "Originating node doesn't have any funds at all!"
        return $null
    }
    $new_wallet = Invoke-DLTApi -APIPort $random_dst_node -Command "generatenewaddress"
    if($new_wallet -eq $null) {
        Write-Host -ForegroundColor Red "Unable to generate a new wallet address. Check the log for node with API $($random_dst_node)"
        return $null
    }
    $txid = Send-Transaction -FromNode $random_src_node -FromAddresses @($orig_wallet) -FromAmounts @($amnt) -ToAddresses @($new_wallet) -ToAmounts @($amnt)
    #$txid = Send-Transaction -FromNode $random_src_node -ToAddresses @($new_wallet) -ToAmounts @($amnt)
    if($txid -eq $null) {
        Write-Host -ForegroundColor Red "Error generating the transaction! Check the log for node with API $($random_src_node)"
        return $null
    }
    return [PSCustomObject]@{
        BH = Get-CurrentBlockHeight -APIPort $APIPort
        SrcWallet = $orig_wallet
        SrcWalletBalance = $src_balance_old
        DstWallet = $new_wallet
        Amount = $amnt
        TXID = $txid
    }
}

function Check-BasicTX {
    Param(
        [int]$APIPort,
        $test_data
    )
    $oldbh = $test_data.BH
    $newbh = Get-CurrentBlockHeight -APIPort $APIPort
    if($newbh -le $oldbh) {
        return "WAIT"
    }
    $src_balance = Get-WalletBalance -APIPort $APIPort -address $test_data.SrcWallet

    if($src_balance -ge $test_data.SrcWalletBalance) {
        return "Source wallet was not deducted! Details: $($test_data)"
    }
    # dest wallet should have exactly the required amount
    $dst_balance = Get-WalletBalance -APIPort $APIPort -address $test_data.DstWallet
    if($dst_balance -ne $test_data.Amount) {
        return "Incorrect amount was deposited! Details: $($test_data)"
    }
    return "OK"
}