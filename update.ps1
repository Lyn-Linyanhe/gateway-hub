param(
    [switch]$CheckOnly,
    [switch]$NoRestart
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$Repo = 'Lyn-Linyanhe/gateway-hub'
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Here

function Get-CurrentVersion {
    $line = Select-String -Path (Join-Path $Here 'router.py') -Pattern '^VERSION\s*=\s*"([^"]+)"' |
        Select-Object -First 1
    if (-not $line) { throw '无法从 router.py 读取 VERSION' }
    return $line.Matches[0].Groups[1].Value
}

function Get-LatestRelease {
    $headers = @{ 'User-Agent' = 'gateway-hub-updater'; 'Accept' = 'application/vnd.github+json' }
    try {
        return Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest" -Headers $headers
    } catch {
        # API 有匿名频率限制；退回 release 页面的跳转目标，不消耗 API 配额。
        Write-Host '[INFO] API 不可用，改用 release 页面检查版本 ...' -ForegroundColor Yellow
        $page = Invoke-WebRequest -Uri "https://github.com/$Repo/releases/latest" `
            -MaximumRedirection 5 -UseBasicParsing -Headers @{ 'User-Agent' = 'gateway-hub-updater' }
        $final = $page.BaseResponse.ResponseUri.AbsoluteUri
        if (-not $final) { $final = $page.BaseResponse.RequestMessage.RequestUri.AbsoluteUri }
        if ($final -notmatch '/tag/([^/?#]+)') { throw '无法从 release 页面解析 tag' }
        $tag = $Matches[1]
        return [pscustomobject]@{
            tag_name = $tag
            zipball_url = "https://github.com/$Repo/archive/refs/tags/$tag.zip"
        }
    }
}

function Copy-CodeFiles([string]$Source) {
    $files = @('router.py', 'gateway_update.py', 'dashboard.html', 'audit_platform.py',
               'test_regressions.py', 'README.md', 'run-router.bat',
               'start-buddy2api.bat', '.gitignore')
    $backup = Join-Path $Here ('backup-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
    New-Item -ItemType Directory -Path $backup | Out-Null
    foreach ($name in $files) {
        $old = Join-Path $Here $name
        if (Test-Path $old) { Copy-Item $old $backup -Force }
        $new = Join-Path $Source $name
        if (Test-Path $new) { Copy-Item $new $Here -Force }
    }
    return $backup
}

function Test-PythonSyntax {
    $python = Get-Command python -ErrorAction SilentlyContinue
    if (-not $python) {
        Write-Host '[WARN] 未找到 python，跳过语法检查' -ForegroundColor Yellow
        return $true
    }
    & $python.Source -m py_compile (Join-Path $Here 'router.py')
    return ($LASTEXITCODE -eq 0)
}

function Restart-Gateway {
    $listener = Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($listener) {
        Write-Host "停止旧网关进程 $($listener.OwningProcess) ..."
        Stop-Process -Id $listener.OwningProcess -Force
        Start-Sleep -Seconds 1
    }
    Write-Host '启动网关 ...'
    Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', ('"' + (Join-Path $Here 'run-router.bat') + '"') -WorkingDirectory $Here -WindowStyle Hidden
    Start-Sleep -Seconds 2
    try {
        $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8787/health' -TimeoutSec 5
        if ($health.ok) { Write-Host '网关已启动。' -ForegroundColor Green }
        else { Write-Host '[WARN] 网关已启动但健康检查未通过' -ForegroundColor Yellow }
    } catch {
        Write-Host '[WARN] 网关启动后健康检查失败，请查看 router.log' -ForegroundColor Yellow
    }
}

try {
    $current = Get-CurrentVersion
    $release = Get-LatestRelease
    $latest = $release.tag_name.TrimStart('v')
    Write-Host "当前版本: $current"
    Write-Host "最新版本: $latest"

    if ([version]$latest -le [version]$current) {
        Write-Host '已经是最新版本，无需更新。' -ForegroundColor Green
        return
    }
    if ($CheckOnly) {
        Write-Host "发现新版本 $latest；重新运行不带 -CheckOnly 即可更新。"
        return
    }

    $updated = $false
    if ((Test-Path (Join-Path $Here '.git')) -and (Get-Command git -ErrorAction SilentlyContinue)) {
        $dirty = & git status --porcelain
        if (-not $dirty) {
            Write-Host '通过 git 更新 ...'
            & git fetch --tags origin
            & git pull --ff-only origin main
            if ($LASTEXITCODE -eq 0) { $updated = $true }
        } else {
            Write-Host '[WARN] 本地有未提交修改，改用文件覆盖（保留 config.json/日志/账号）' -ForegroundColor Yellow
        }
    }

    if (-not $updated) {
        $zip = Join-Path $env:TEMP ("gateway-hub-$latest.zip")
        $temp = Join-Path $env:TEMP ("gateway-hub-" + [guid]::NewGuid().ToString('N'))
        Write-Host '下载最新源码 ...'
        Invoke-WebRequest -Uri $release.zipball_url -OutFile $zip -UseBasicParsing
        Expand-Archive -Path $zip -DestinationPath $temp -Force
        $source = Get-ChildItem -Path $temp -Directory | Select-Object -First 1
        if (-not $source) { throw '下载的压缩包结构异常' }
        $backup = Copy-CodeFiles $source.FullName
        if (-not (Test-PythonSyntax)) {
            Write-Host '[ERROR] 新代码语法检查失败，正在恢复旧文件 ...' -ForegroundColor Red
            Get-ChildItem -Path $backup -File | Copy-Item -Destination $Here -Force
            throw '更新失败，已恢复备份'
        }
        Write-Host "旧文件已备份到: $backup"
        Remove-Item $zip, $temp -Recurse -Force -ErrorAction SilentlyContinue
    }

    Write-Host "更新完成：$current -> $latest" -ForegroundColor Green
    if (-not $NoRestart) { Restart-Gateway }
    else { Write-Host '请手动重启网关（run-router.bat）。' }
} catch {
    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
