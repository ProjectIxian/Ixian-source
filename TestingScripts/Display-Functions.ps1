
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

    foreach($node in $Clients) {
        $ns = Get-DLTNodeStatus -Clients $Clients -NodeIdx $node.idx
        # DLT Status
        [void]$dlt_statuses.Add(($ns.'DLT Status').Substring(0,3).ToUpper().PadLeft(10))
        # Block processor status
        $bp_status = ($ns.'Block Processor Status')
        if($bp_status -eq "Stopped") { $bp_status = "STOP" }
        if($bp_status -eq "Running") { $bp_status = "OK" }
        [void]$bp_statuses.Add($bp_status.PadLeft(10))
        # Block Height
        [void]$node_bhs.Add(($ns.'Block Height').PadLeft(10))
        # Consensus
        [void]$consensus.Add(($ns.'Required Consensus').PadLeft(10))
        # In / Out connections
        [void]$in_connections.Add(($ns.'Network Clients').PadLeft(10))
        [void]$out_connections.Add(($ns.'Network Servers').PadLeft(10))
        # Applied / Unapplied TXs
        [void]$applied_txs.Add(($ns.'Applied TX Count').PadLeft(10))
        [void]$unapplied_txs.Add(($ns.'Unapplied TX Count').PadLeft(10))
        # Miner
        $ms = Get-DLTMinerStatus -Clients $Clients -NodeIdx $node.idx
        # Hashrates
        [void]$hashrates.Add(($ms.'Hashrate').PadLeft(10))
        # Solved
        [void]$solved_counts.Add(($ms.'Solved Blocks (Local)').PadLeft(10))
        $count++
        if($Count -ge 13) {
            break
        }       
    }
    #
    #Render-Header
    #Render-Indexes $Clients
    #Render-Status $dlt_statuses $bp_statuses    
    #Render-BlockHeight $node_bhs
    #Render-Consensus $consensus
    #Render-Connections $in_connections $out_connections
    #Render-TXs $applied_txs $unapplied_txs
    #Render-MinerHashrate $hashrates
    #Render-MinerSolved $solved_counts
    #Render-GlobalStats
    #Render-Footer
}
