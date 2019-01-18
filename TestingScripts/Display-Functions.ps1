function Format-Human {
    Param(
        [int]$num
    )
    $suffix = ''
    if($num -ge 1000*1000*1000*1000) {
        $num = ($num*1.0) / (1000*1000*1000*1000)
        $suffix = ' T'
    } elseif($num -ge 1000*1000*1000) {
        $num = ($num*1.0) / (1000*1000*1000)
        $suffix = ' G'
    } elseif($num -ge 1000*1000) {
        $num = ($num*1.0) / (1000*1000)
        $suffix = ' M'
    } elseif($num -ge 1000) {
        $num = ($num*1.0) / (1000)
        $suffix = ' K'
    }

    return "$($num.ToString('0.00'))$($suffix)"
}


function Render-Header {
    Param(
        [int]$NumNodes
    )
    Write-Host -ForegroundColor White "------------------------------------------------------------------------"
    $now = Get-Date
    Write-Host -ForegroundColor White "Last refresh: $($now.ToString())"
    Write-Host -ForegroundColor Yellow "Testnet has $($NumNodes) nodes."
}

function Render-Indexes {
    Param(
        [System.Collections.ArrayList]$Clients
    )
    Write-Host -ForegroundColor White -NoNewline "NODES:".PadRight(10)
    foreach($c in $Clients) {
        if($c.idx -ge 13) { break }
        Write-Host -ForegroundColor Green -NoNewline "$($c.idx.ToString().PadLeft(10))"
    }
    Write-Host ""
}

function Render-Status {
    Param(
        [System.Collections.ArrayList]$dlt_statuses,
        [System.Collections.ArrayList]$bp_statuses
    )
    Write-Host -ForegroundColor White -NoNewline "STATUS:".PadRight(10)
    $num_elem = $dlt_statuses.Count
    if($num_elem -gt $bp_statuses.Count) {
        $num_elem = $bp_statuses.Count
    }
    for($i = 0; $i -lt $num_elem; $i++) {
        $dlt_s = $dlt_statuses[$i]
        $bp_s = $bp_statuses[$i]
        $dlt_c = [ConsoleColor]::Green
        $bp_c = [ConsoleColor]::Green
        if($dlt_s -eq "SYN") {
            $dlt_c = [ConsoleColor]::Yellow
        }
        if($bp_s -eq "STOP") {
            $bp_c = [ConsoleColor]::Red
        }
        $len = 10 - $dlt_s.Length - $bp_s.Length - 1
        Write-Host -NoNewline "$(New-Object String (' ', $len))"
        Write-Host -NoNewline -ForegroundColor $dlt_c $dlt_s
        Write-Host -NoNewline -ForegroundColor White "/"
        Write-Host -NoNewline -ForegroundColor $bp_c $bp_s
    }
    Write-Host ""
}

function Render-BlockHeight {
    Param(
        [System.Collections.ArrayList]$bhs
    )
    Write-Host -NoNewline -ForegroundColor White "B.HEIGHT:".PadRight(10)
    foreach($bh in $bhs) {
        Write-Host -NoNewline -ForegroundColor Cyan $bh
    }
    Write-Host ""
}

function Render-Consensus {
    Param(
        [System.Collections.ArrayList]$consensus
    )
    Write-Host -NoNewline -ForegroundColor White "CONSENSUS:"
    foreach($c in $consensus) {
        Write-Host -NoNewline -ForegroundColor Yellow $c
    }
    Write-Host ""
}

function Render-Connections {
    Param(
        [System.Collections.ArrayList]$in_conn,
        [System.Collections.ArrayList]$out_conn
    )
    Write-Host -ForegroundColor White -NoNewline "N(In/Out):"
    $num_elem = $in_conn.Count
    if($num_elem -gt $out_conn.Count) {
        $num_elem = $out_conn.Count
    }
    for($i = 0; $i -lt $num_elem; $i++) {
        $in_s = $in_conn[$i]
        $out_s = $out_conn[$i]
        $len = 10 - $in_s.Length - $out_s.Length - 1
        Write-Host -NoNewline "$(New-Object String (' ', $len))"
        Write-Host -NoNewline -ForegroundColor Cyan $in_s
        Write-Host -NoNewline -ForegroundColor White "/"
        Write-Host -NoNewline -ForegroundColor Cyan $out_s
    }
    Write-Host ""
}

function Render-TXs {
    Param(
        [System.Collections.ArrayList]$applied,
        [System.Collections.ArrayList]$unapplied
    )
    Write-Host -ForegroundColor White -NoNewline "TX(A/U):  "
    $num_elem = $applied.Count
    if($num_elem -gt $unapplied.Count) {
        $num_elem = $unapplied.Count
    }
    for($i = 0; $i -lt $num_elem; $i++) {
        $a_s = $applied[$i]
        $u_s = $unapplied[$i]
        $len = 10 - $a_s.Length - $u_s.Length - 1
        Write-Host -NoNewline "$(New-Object String (' ', $len))"
        Write-Host -NoNewline -ForegroundColor Cyan $a_s
        Write-Host -NoNewline -ForegroundColor White "/"
        Write-Host -NoNewline -ForegroundColor Cyan $u_s
    }
    Write-Host ""
}

function Render-MinerHashrate {
    Param(
        [System.Collections.ArrayList]$hashrates
    )
    Write-Host -NoNewline -ForegroundColor White "HASH:".PadRight(10)
    foreach($hr in $hashrates) {
        if($hr -eq 0) {
            Write-Host -NoNewline -ForegroundColor Red "N/A".PadLeft(10)
        } else {
            $hr_t = Format-Human $hr
            Write-Host -NoNewline -ForegroundColor Green $hr_t.PadLeft(10)
        }
    }
    Write-Host ""
}

function Render-MinerSolved {
    Param(
        [System.Collections.ArrayList]$solved_counts
    )
    Write-Host -NoNewline -ForegroundColor White "SOLVED:".PadRight(10)
    foreach($s in $solved_counts) {
        Write-Host -NoNewline -ForegroundColor Green $s.PadLeft(10)
    }
    Write-Host ""
}

function Render-GlobalStats {
    Param(
        [int]$solved,
        [int]$unsolved,
        [int]$wallets,
        [uint64]$difficulty
    )
    Write-Host -ForegroundColor Gray "========================================================================"
    Write-Host -ForegroundColor White "Global stats:"
    # wallets
    Write-Host -ForegroundColor White -NoNewline "Wallets: "
    Write-Host -ForegroundColor Cyan "$($wallets)"
    # Block solution stats
    Write-Host -ForegroundColor White -NoNewline "Solved/Unsolved blocks: "
    Write-Host -ForegroundColor Cyan -NoNewline "$($solved)"
    Write-Host -ForegroundColor White -NoNewline "/"
    Write-Host -ForegroundColor Green -NoNewline "$($unsolved)"
    $ratio = 100.0 * $solved / $unsolved
    Write-Host -ForegroundColor White -NoNewline " ("
    $col = [ConsoleColor]::Cyan
    if($solved -eq $unsolved) {
        $col = [ConsoleColor]::Green
    } elseif($solved -gt $unsolved) {
        $col = [ConsoleColor]::Yellow
    }
    Write-Host -ForegroundColor $col -NoNewline "$($ratio.ToString('0.00'))"
    Write-Host -ForegroundColor White ")"
    # difficulty
    Write-Host -ForegroundColor White -NoNewline "Difficulty: "
    Write-Host -ForegroundColor Cyan -NoNewline "$($difficulty)"
    Write-Host -ForegroundColor White -NoNewline " ("
    $hex_diff = [System.BitConverter]::ToString([System.BitConverter]::GetBytes($difficulty)).Replace("-", "")
    Write-Host -ForegroundColor Green -NoNewline "$($hex_diff)"
    Write-Host -ForegroundColor White ")"

}


function Display-ClientStatus {
    Param(
        [System.Collections.ArrayList]$Clients
    )
    $count = 0
    $dlt_statuses = New-Object System.Collections.ArrayList (13)
    $bp_statuses = New-Object System.Collections.ArrayList (13)
    $node_bhs = New-Object System.Collections.ArrayList (13)
    $consensus = New-Object System.Collections.ArrayList (13)
    $in_connections = New-Object System.Collections.ArrayList (13)
    $out_connections = New-Object System.Collections.ArrayList (13)
    $applied_txs = New-Object System.Collections.ArrayList (13)
    $unapplied_txs = New-Object System.Collections.ArrayList (13)
    # Miner:
    $hashrates = New-Object System.Collections.ArrayList (13)
    $solved_counts = New-Object System.Collections.ArrayList (13)

    # Globals
    $global_solved = 0
    $global_unsolved = 0
    $global_wallets = 0
    $global_difficulty = 0
    $mining_globals_captured = $false

    foreach($node in $Clients) {
        $ns = Get-DLTNodeStatus -Clients $Clients -NodeIdx $node.idx
        # DLT Status
        [void]$dlt_statuses.Add(($ns.'DLT Status').Substring(0,3).ToUpper())
        # Block processor status
        $bp_status = ($ns.'Block Processor Status')
        if($bp_status -eq "Stopped") { $bp_status = "STOP" }
        if($bp_status -eq "Running") { $bp_status = "OK" }
        [void]$bp_statuses.Add($bp_status)
        # Block Height
        [void]$node_bhs.Add(($ns.'Block Height').ToString().PadLeft(10))
        # Consensus
        [void]$consensus.Add(($ns.'Required Consensus').ToString().PadLeft(10))
        # In / Out connections
        [void]$in_connections.Add(($ns.'Network Clients').Count.ToString())
        [void]$out_connections.Add(($ns.'Network Servers').Count.ToString())
        # Applied / Unapplied TXs
        [void]$applied_txs.Add(($ns.'Applied TX Count').ToString())
        [void]$unapplied_txs.Add(($ns.'Unapplied TX Count').ToString())
        # Miner
        $ms = Get-DLTMinerStatus -Clients $Clients -NodeIdx $node.idx
        # Hashrates
        [void]$hashrates.Add(($ms.'Hashrate'))
        # Solved
        [void]$solved_counts.Add(($ms.'Solved Blocks (Local)').ToString().PadLeft(10))
        if($count -eq 0) {
            # Caputer global stats from first node
            $global_wallets = $ns.'Wallets'
        }
        if($node.Miner -eq $true -and $mining_globals_captured -eq $false) {
            # Capture mining globals from the first miner
            $global_solved = $ms.'Solved Blocks (Network)'
            $global_unsolved = $ms.'Empty Blocks'
            $global_difficulty = $ms.'Current Difficulty'
            $mining_globals_captured = $true
        }
        $count++
        if($Count -ge 13) {
            break
        }       
    }
    #
    Clear-Host
    Render-Header $Clients.Count
    Render-Indexes $Clients
    Render-Status $dlt_statuses $bp_statuses    
    Render-BlockHeight $node_bhs
    Render-Consensus $consensus
    Render-Connections $in_connections $out_connections
    Render-TXs $applied_txs $unapplied_txs
    Render-MinerHashrate $hashrates
    Render-MinerSolved $solved_counts
    Render-GlobalStats $global_solved $global_unsolved $global_wallets $global_difficulty
    #Render-Footer
}
