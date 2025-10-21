param(
    [string]$BaseUrl = "http://localhost:8080"
)

# ===== 批量消息定义：每种类型 10 条 =====
$jobs = @()

# 10 条 Msg (高优先级)
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "Msg"
        Mode     = "async"
        Topic    = "Msg"
        UserId   = "1001"
        Body     = "第 $i +111条 InApp 消息 [高优先级]"
        Priority = $i
    }
}

# 10 条 Notification (中优先级)
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "Notification"
        Mode     = "async"
        Topic    = "Notification"
        UserId   = "1001"
        Body     = "第 $i 条 通知 [中优先级]"
        Priority = $i
    }
}

# 10 条 Alert (低优先级)
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type      = "Alert"
        Mode      = "sync"
        Topic     = "Alert"
        UserId    = "1001"
        Body      = "第 $i 条警报 [低优先级]"
        Priority  = 1
        Severity  = "High"
        Timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
}

# 10 条 SystemMaintenance（系统维护）
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "SystemMaintenance"
        Mode     = "async"
        Topic    = "SystemMaintenance"
        UserId   = "1001"
        Body     = "系统维护通知第 $i 条"
        Priority = 2
        Extra    = @{
            maintenanceId    = "MAINT-2024-$($i.ToString().PadLeft(3,'0'))"
            title            = "系统维护通知 #$i"
            description      = "为了提升系统性能，我们将进行第 $i 次维护。"
            startTime        = (Get-Date).AddDays($i).ToString("yyyy-MM-ddTHH:mm:ssZ")
            endTime          = (Get-Date).AddDays($i).AddHours(4).ToString("yyyy-MM-ddTHH:mm:ssZ")
            affectedServices = @("用户服务", "订单服务")
            priority         = 3
        }
    }
}

# 10 条 OrderUpdate（订单更新）
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "OrderUpdate"
        Mode     = "async"
        Topic    = "OrderUpdate"
        UserId   = "1001"
        Body     = "订单更新通知第 $i 条"
        Priority = 3
        Extra    = @{
            orderId           = "ORDER-2024-$($i.ToString().PadLeft(6,'0'))"
            orderNumber       = "ORD$($i.ToString().PadLeft(6,'0'))"
            status            = if ($i % 4 -eq 0) { "delivered" } elseif ($i % 3 -eq 0) { "shipped" } elseif ($i % 2 -eq 0) { "confirmed" } else { "pending" }
            previousStatus    = "pending"
            updateTime        = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            amount            = [double](100.50 + $i * 10)
            trackingNumber    = "TRK$($i.ToString().PadLeft(10,'0'))"
            estimatedDelivery = (Get-Date).AddDays(3).ToString("yyyy-MM-ddTHH:mm:ssZ")
        }
    }
}

# 10 条 AccountSecurity（账户安全）
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "AccountSecurity"
        Mode     = "async"
        Topic    = "AccountSecurity"
        UserId   = "1001"
        Body     = "账户安全提醒第 $i 条内容"
        Priority = 1
        Extra    = @{
            alertType      = if ($i % 4 -eq 0) { "login_attempt" } elseif ($i % 3 -eq 0) { "password_change" } elseif ($i % 2 -eq 0) { "permission_change" } else { "suspicious_activity" }
            title          = "账户安全提醒 #$i"
            message        = "检测到可疑活动，请及时处理第 $i 项安全问题。"
            timestamp      = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
            ipAddress      = "192.168.1.$($i + 100)"
            location       = "北京市"
            device         = "Windows PC"
            riskLevel      = if ($i % 3 -eq 0) { "high" } elseif ($i % 2 -eq 0) { "medium" } else { "low" }
            actionRequired = ($i % 2 -eq 0)
            actionUrl      = "https://example.com/security/action/$i"
        }
    }
}

# 10 条 ProductLaunch（产品发布通知）
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "ProductLaunch"
        Mode     = "async"
        Topic    = "ProductLaunch"
        UserId   = "1001"
        Body     = "产品发布通知第 $i 条"
        Priority = 2
        Extra    = @{
            productId        = "PROD-2025-$($i.ToString().PadLeft(3,'0'))"
            productName      = if ($i % 4 -eq 0) { "智能助手 Pro v$i.0" } elseif ($i % 3 -eq 0) { "数据分析工具 v$i.2" } elseif ($i % 2 -eq 0) { "企业管理系统 v$i.1" } else { "移动办公应用 v$i.0" }
            version          = "v$i.$(($i % 5) + 1).0"
            launchDate       = (Get-Date).AddDays($i * 2).ToString("yyyy-MM-dd")
            description      = "全新发布的第 $i 代产品，带来更强大的功能和更优秀的用户体验。"
            category         = if ($i % 3 -eq 0) { "software" } elseif ($i % 2 -eq 0) { "service" } else { "hardware" }
            price            = [double](99.99 + $i * 50)
            features         = @("AI智能", "云端同步", "多平台支持", "数据加密")
            targetAudience   = @("企业用户", "个人开发者", "教育机构")
            availableRegions = @("中国", "美国", "欧洲", "亚太地区")
            downloadUrl      = "https://example.com/products/download/prod-$i"
            priority         = if ($i % 2 -eq 0) { 4 } else { 3 }
        }
    }
}

# 10 条 PromotionCampaign（营销活动通知）
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "PromotionCampaign"
        Mode     = "async"
        Topic    = "PromotionCampaign"
        UserId   = "1001"
        Body     = "营销活动通知第 $i 条"
        Priority = 3
        Extra    = @{
            campaignId       = "CAMP-2025-$($i.ToString().PadLeft(3,'0'))"
            campaignName     = if ($i % 4 -eq 0) { "春节特惠活动 #$i" } elseif ($i % 3 -eq 0) { "会员专享折扣 #$i" } elseif ($i % 2 -eq 0) { "限时闪购 #$i" } else { "新用户优惠 #$i" }
            campaignType     = if ($i % 4 -eq 0) { "flash_sale" } elseif ($i % 3 -eq 0) { "discount" } elseif ($i % 2 -eq 0) { "promotion" } else { "new_member" }
            title            = "限时优惠活动 #$i - 不容错过！"
            description      = "第 $i 个超值优惠活动来袭！精选商品限时特价，数量有限，先到先得。"
            startTime        = (Get-Date).AddHours($i).ToString("yyyy-MM-ddTHH:mm:ssZ")
            endTime          = (Get-Date).AddDays($i + 3).ToString("yyyy-MM-ddTHH:mm:ssZ")
            discountRate     = [double](0.5 + ($i % 5) * 0.1)
            couponCode       = "SAVE$($i)OFF"
            minAmount        = [double](50.0 + $i * 20)
            maxDiscount      = [double](100.0 + $i * 25)
            targetProducts   = @("智能手机", "笔记本电脑", "智能手表", "无线耳机")
            targetCategories = @("电子产品", "数码配件", "智能家居")
            bannerUrl        = "https://example.com/banners/campaign-$i.jpg"
            actionUrl        = "https://example.com/campaigns/camp-$i"
            priority         = if ($i % 2 -eq 0) { 4 } else { 3 }
        }
    }
}

# 10 条 TaskReminder（任务提醒通知）
for ($i = 1; $i -le 10; $i++) {
    $jobs += @{
        Type     = "TaskReminder"
        Mode     = "async"
        Topic    = "TaskReminder"
        UserId   = "1001"
        Body     = "任务提醒通知第 $i 条"
        Priority = 2
        Extra    = @{
            taskId               = "TASK-2025-$($i.ToString().PadLeft(4,'0'))"
            taskTitle            = if ($i % 4 -eq 0) { "代码审查任务 #$i" } elseif ($i % 3 -eq 0) { "项目进度汇报 #$i" } elseif ($i % 2 -eq 0) { "客户需求分析 #$i" } else { "系统测试任务 #$i" }
            taskDescription      = "这是第 $i 个重要任务，需要在截止日期前完成。请及时查看详细信息并安排工作计划。"
            dueDate              = (Get-Date).AddDays($i + 2).ToString("yyyy-MM-dd")
            reminderType         = if ($i % 4 -eq 0) { "deadline_approaching" } elseif ($i % 3 -eq 0) { "overdue" } elseif ($i % 2 -eq 0) { "completed" } else { "assigned" }
            assignedTo           = "用户$($i + 1000)"
            assignedBy           = "管理员"
            projectName          = if ($i % 3 -eq 0) { "电商系统升级项目" } elseif ($i % 2 -eq 0) { "移动端开发项目" } else { "数据分析平台项目" }
            category             = if ($i % 4 -eq 0) { "review" } elseif ($i % 3 -eq 0) { "meeting" } elseif ($i % 2 -eq 0) { "work" } else { "personal" }
            urgencyLevel         = if ($i % 4 -eq 0) { "critical" } elseif ($i % 3 -eq 0) { "high" } elseif ($i % 2 -eq 0) { "medium" } else { "low" }
            estimatedHours       = [double](2.0 + ($i % 8) * 0.5)
            completionPercentage = ($i % 10) * 10
            attachments          = @("https://example.com/files/task-$i-doc.pdf", "https://example.com/files/task-$i-spec.docx")
            tags                 = @("重要", "紧急", "开发", "测试")
            actionUrl            = "https://example.com/tasks/task-$i"
        }
    }
}

# ===== 公共函数 =====
function Send-PushMessage {
    param(
        [string]$BaseUrl,
        [ValidateSet("Msg", "Notification", "Alert", "SystemMaintenance", "OrderUpdate", "AccountSecurity", "ProductLaunch", "PromotionCampaign", "TaskReminder")] [string]$Type,
        [ValidateSet("sync", "async")] [string]$Mode,
        [string]$Topic,
        [string]$UserId,
        [string]$Body,
        [int]$Priority,
        [string]$Severity,
        [string]$Timestamp,
        [hashtable]$Extra
    )

    if ($null -eq $Extra) { $Extra = @{} }

    if ($Type -eq "Msg") {
        $msg = [ordered]@{
            appId     = "demo-app"
            kind      = "inapp"
            subject   = "PowerShell演示"
            body      = $Body
            to        = @(@{ user_id = "$UserId" })
            priority  = $Priority
            content   = @{ text = $Body }
            receivers = @( [uint64]$UserId )
            userIds   = @( [uint64]$UserId )
            sender    = 10001
            type      = "Info"
        }
    }
    elseif ($Type -eq "Notification") {
        $msg = [ordered]@{
            title    = "PowerShell通知"
            body     = $Body
            userIds  = @( [uint64]$UserId )
            sender   = [uint64]10001
            kind     = "inapp"
            to       = @(@{ user_id = "$UserId" })
            subject  = "PowerShell通知"
            priority = $Priority
        }
    }
    elseif ($Type -eq "Alert") {
        $msg = [ordered]@{
            severity  = $Severity
            message   = $Body
            timestamp = $Timestamp
            userIds   = @( [uint64]$UserId )
            sender    = [uint64]10001
            kind      = "inapp"
            to        = @(@{ user_id = "$UserId" })
            subject   = "PowerShell 警报"
            body      = $Body
            priority  = $Priority
        }
    }
    elseif ($Type -eq "SystemMaintenance") {
        $msg = [ordered]@{
            maintenanceId    = $Extra.maintenanceId
            title            = $Extra.title
            description      = $Extra.description
            startTime        = $Extra.startTime
            endTime          = $Extra.endTime
            affectedServices = $Extra.affectedServices
            priority         = $Priority
            userIds          = @( [uint64]$UserId )
            sender           = [uint64]10001
            # 添加必要的通用字段
            kind             = "inapp"
            to               = @(@{ user_id = "$UserId" })
            subject          = $Extra.title
            body             = $Extra.description
        }
    }
    elseif ($Type -eq "OrderUpdate") {
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
            # 添加必要的通用字段
            kind              = "inapp"
            to                = @(@{ user_id = "$UserId" })
            subject           = "订单 $($Extra.orderNumber) 状态更新"
            body              = "您的订单状态已更新为: $($Extra.status)"
        }
    }
    elseif ($Type -eq "AccountSecurity") {
        $msg = [ordered]@{
            alertType      = $Extra.alertType
            title          = $Extra.title
            message        = $Extra.message
            timestamp      = $Extra.timestamp
            ipAddress      = $Extra.ipAddress
            location       = $Extra.location
            device         = $Extra.device
            riskLevel      = $Extra.riskLevel
            actionRequired = $Extra.actionRequired
            actionUrl      = $Extra.actionUrl
            userIds        = @( [uint64]$UserId )
            sender         = [uint64]10001
            priority       = $Priority
            # 添加必要的通用字段
            kind           = "inapp"
            to             = @(@{ user_id = "$UserId" })
            subject        = $Extra.title
            body           = $Extra.message
        }
    }
    elseif ($Type -eq "ProductLaunch") {
        $msg = [ordered]@{
            productId        = $Extra.productId
            productName      = $Extra.productName
            version          = $Extra.version
            launchDate       = $Extra.launchDate
            description      = $Extra.description
            category         = $Extra.category
            price            = $Extra.price
            features         = $Extra.features
            targetAudience   = $Extra.targetAudience
            availableRegions = $Extra.availableRegions
            downloadUrl      = $Extra.downloadUrl
            priority         = $Priority
            userIds          = @( [uint64]$UserId )
            sender           = [uint64]10001
            # 添加唯一标识确保不被幂等性过滤
            subject          = "新产品发布：$($Extra.productName) ($($Extra.productId))"
            body             = "$($Extra.description) - 产品ID: $($Extra.productId)"
        }
    }
    elseif ($Type -eq "PromotionCampaign") {
        $msg = [ordered]@{
            campaignId       = $Extra.campaignId
            campaignName     = $Extra.campaignName
            campaignType     = $Extra.campaignType
            title            = $Extra.title
            description      = $Extra.description
            startTime        = $Extra.startTime
            endTime          = $Extra.endTime
            discountRate     = $Extra.discountRate
            couponCode       = $Extra.couponCode
            minAmount        = $Extra.minAmount
            maxDiscount      = $Extra.maxDiscount
            targetProducts   = $Extra.targetProducts
            targetCategories = $Extra.targetCategories
            bannerUrl        = $Extra.bannerUrl
            actionUrl        = $Extra.actionUrl
            priority         = $Priority
            userIds          = @( [uint64]$UserId )
            sender           = [uint64]10001
            # 添加唯一标识确保不被幂等性过滤
            subject          = "$($Extra.title) - 活动ID: $($Extra.campaignId)"
            body             = "$($Extra.description) | 优惠码: $($Extra.couponCode) | 活动ID: $($Extra.campaignId)"
        }
    }
    elseif ($Type -eq "TaskReminder") {
        $msg = [ordered]@{
            taskId               = $Extra.taskId
            taskTitle            = $Extra.taskTitle
            taskDescription      = $Extra.taskDescription
            dueDate              = $Extra.dueDate
            reminderType         = $Extra.reminderType
            assignedTo           = $Extra.assignedTo
            assignedBy           = $Extra.assignedBy
            projectName          = $Extra.projectName
            category             = $Extra.category
            urgencyLevel         = $Extra.urgencyLevel
            estimatedHours       = $Extra.estimatedHours
            completionPercentage = $Extra.completionPercentage
            attachments          = $Extra.attachments
            tags                 = $Extra.tags
            actionUrl            = $Extra.actionUrl
            userIds              = @( [uint64]$UserId )
            sender               = [uint64]10001
            priority             = $Priority
            # 添加唯一标识确保不被幂等性过滤
            subject              = "任务提醒：$($Extra.taskTitle) - 任务ID: $($Extra.taskId)"
            body                 = "$($Extra.taskDescription) | 截止日期: $($Extra.dueDate) | 任务ID: $($Extra.taskId)"
        }
    }

    # 所有消息类型都使用 /v1/push 端点，但构造不同的消息格式
    if ($Type -in @("SystemMaintenance", "OrderUpdate", "AccountSecurity", "ProductLaunch", "PromotionCampaign", "TaskReminder")) {
        # 新的动态消息类型：包装为标准格式
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
    }
    else {
        # 传统消息类型：包装格式
        $payload = [ordered]@{
            topic = $Topic
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
    }

    $resp = Invoke-RestMethod @irmParams
    return $resp
}   # function Send-PushMessage 结束

function Get-Inbox {
    param(
        [string]$BaseUrl,
        [string]$UserId
    )
    return Invoke-RestMethod -Uri "$BaseUrl/api/inbox?user_id=$UserId" -Method Get
}   # function Get-Inbox 结束

# ===== 批量执行 =====
Write-Host "=== 消息发送顺序和队列映射 ===" -ForegroundColor Green
$jobs | ForEach-Object {
    $queueType = if ($_.Priority -ge 4) { "高优先级队列" } elseif ($_.Priority -ge 2) { "中优先级队列" } else { "低优先级队列" }
    Write-Host "- $($_.Type) (Priority: $($_.Priority) -> $queueType)" -ForegroundColor Cyan
}
Write-Host ""

$results = foreach ($j in $jobs) {
    $topic = if ([string]::IsNullOrWhiteSpace($j.Topic)) { $j.Type } else { $j.Topic }
    $priority = if ($null -eq $j.Priority -or $j.Priority -eq "") { 1 } else { [int]$j.Priority }
    $severity = if ([string]::IsNullOrWhiteSpace($j.Severity)) { "High" } else { $j.Severity }
    $timestamp = if ([string]::IsNullOrWhiteSpace($j.Timestamp)) { (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ") } else { $j.Timestamp }
    $body = if ([string]::IsNullOrWhiteSpace($j.Body)) { "" } else { $j.Body }
    $extra = if ($null -eq $j.Extra) { @{} } else { $j.Extra }

    try {
        $params = @{
            BaseUrl   = $BaseUrl
            Type      = $j.Type
            Mode      = $j.Mode
            Topic     = $topic
            UserId    = $j.UserId
            Body      = $body
            Priority  = $priority
            Severity  = $severity
            Timestamp = $timestamp
            Extra     = $extra
        }
        $resp = Send-PushMessage @params

        [pscustomobject]@{
            UserId  = $j.UserId
            Type    = $j.Type
            Success = $true
            Resp    = $resp
            Error   = $null
        }
    }
    catch {
        # 把后端响应体也抓出来，便于定位错误
        $errMsg = $_.Exception.Message
        if ($_.Exception -is [System.Net.WebException] -and $_.Exception.Response) {
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $respText = $sr.ReadToEnd()
                $sr.Close()
                if (-not [string]::IsNullOrWhiteSpace($respText)) {
                    $errMsg = "$errMsg | Response: $respText"
                }
            }
            catch { }
        }

        [pscustomobject]@{
            UserId  = $j.UserId
            Type    = $j.Type
            Success = $false
            Resp    = $null
            Error   = $errMsg
        }
    }
}   # foreach 结束

"批量推送结果："
$results | Format-Table -Property UserId, Type, Success, Resp, Error -AutoSize

# 可选：查看某个用户的收件箱
# "inbox 1001:"
# Get-Inbox -BaseUrl $BaseUrl -UserId "1001" | Format-List
