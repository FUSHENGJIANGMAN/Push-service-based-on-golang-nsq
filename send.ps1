param(
    [string]$BaseUrl = "http://localhost:8080"
)

# ===== 构造 10 条 OrderUpdate 消息，优先级不同 =====
$jobs = @()

for ($i=1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "OrderUpdate"
        Mode     = "async"
        Topic    = "OrderUpdate"
        UserId   = "1001"
        Body     = "订单更新通知第 $i 条"
        Priority = $i   # 每条消息优先级不同
        Extra    = @{
            orderId           = "ORDER-2025-$($i.ToString().PadLeft(6,'0'))"
            orderNumber       = "ORD$($i.ToString().PadLeft(6,'0'))"
            status            = if ($i % 4 -eq 0) { "delivered" } elseif ($i % 3 -eq 0) { "shipped" } elseif ($i % 2 -eq 0) { "confirmed" } else { "pending" }
            previousStatus    = "pending"
            updateTime        = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            amount            = [double](199.99 + $i * 20)
            trackingNumber    = "TRK$($i.ToString().PadLeft(10,'0'))"
            estimatedDelivery = (Get-Date).AddDays(5).ToString("yyyy-MM-ddTHH:mm:ssZ")
        }
    }
}

# ===== 发送函数（沿用原版） =====
function Send-PushMessage {
    param(
        [string]$BaseUrl,
        [string]$Type,
        [string]$Mode,
        [string]$Topic,
        [string]$UserId,
        [string]$Body,
        [int]$Priority,
        [hashtable]$Extra
    )

    if ($Type -eq "OrderUpdate") {
        $msg = [ordered]@{
            orderId           = $Extra.orderId
            orderNumber       = $Extra.orderNumber
            status            = $Extra.status
            previousStatus    = $Extra.previousStatus
            updateTime        = $Extra.updateTime
            amount            = $Extra.amount
            trackingNumber    = $Extra.trackingNumber
            estimatedDelivery = $Extra.estimatedDelivery
            userIds           = @( [uint64]$UserId )
            sender            = [uint64]10001
            priority          = $Priority
            kind              = "inapp"
            to                = @(@{ user_id = "$UserId" })
            subject           = "订单 $($Extra.orderNumber) 状态更新"
            body              = "您的订单状态已更新为: $($Extra.status)"
        }
    }

    $payload = [ordered]@{
        topic = $Type
        mode  = $Mode
        msg   = $msg
        plan  = [ordered]@{
            primary  = @("web")
            fallback = @()
            timeout  = 5000000000
            retry    = [ordered]@{
                MaxAttempts = 1
                Backoff     = 300000000
            }
        }
    }

    $json = $payload | ConvertTo-Json -Depth 8 -Compress
    $irmParams = @{
        Uri         = "$BaseUrl/v1/push"
        Method      = 'Post'
        ContentType = 'application/json; charset=utf-8'
        Body        = [System.Text.Encoding]::UTF8.GetBytes($json)
    }

    return Invoke-RestMethod @irmParams
}

# ===== 执行发送 =====
Write-Host "=== OrderUpdate 消息发送（不同优先级） ===" -ForegroundColor Green
$results = foreach ($j in $jobs) {
    try {
        $resp = Send-PushMessage -BaseUrl $BaseUrl -Type $j.Type -Mode $j.Mode -Topic $j.Topic -UserId $j.UserId -Body $j.Body -Priority $j.Priority -Extra $j.Extra
        [pscustomobject]@{ UserId=$j.UserId; Type=$j.Type; Priority=$j.Priority; Success=$true; Resp=$resp }
    }
    catch {
        [pscustomobject]@{ UserId=$j.UserId; Type=$j.Type; Priority=$j.Priority; Success=$false; Error=$_.Exception.Message }
    }
}

$results | Format-Table -AutoSize
