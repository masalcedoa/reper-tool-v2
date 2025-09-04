
param(
  [string]$ProjectDir = "C:\Users\msalcedo\Documents\sss\projects\01-gestion-perdidas\fraud-automation-pipeline",
  [string]$MetaPath = "",                  # Opcional: META.xlsx o META.csv
  [string]$ConsumosPath = "",              # Opcional: XLSX/CSV (si no se pasa, toma el último job de /jobs)
  [switch]$Rebuild,                        # Si se pasa, hace down -v y build --no-cache
  [int]$TimeoutSec = 900,                  # 15 minutos
  [int]$PollSec = 3                        # Intervalo de polling
)

function ComposeCmd {
  param([string[]]$Args)
  $override = Join-Path $ProjectDir "docker-compose.override.yml"
  if (Test-Path $override) {
    $cmd = @("docker","compose","-f",(Join-Path $ProjectDir "docker-compose.yml"),"-f",$override) + $Args
  } else {
    $cmd = @("docker","compose","-f",(Join-Path $ProjectDir "docker-compose.yml")) + $Args
  }
  Write-Host ("`n> " + ($cmd -join " ")) -ForegroundColor DarkGray
  & $cmd
  if ($LASTEXITCODE -ne 0) { throw "Comando falló: docker compose $($Args -join ' ')" }
}

function ExecSql {
  param([string]$Query)
  ComposeCmd @("exec","-T","postgres","psql","-U","postgres","-d","frauddb","-t","-c",$Query) | Out-String
}

function HealthCheck {
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSec) {
    try {
      $h = Invoke-RestMethod -Uri "http://localhost:8000/health" -TimeoutSec 5
      if ($h.status -eq "ok") { 
        Write-Host "API saludable (/health = ok)" -ForegroundColor Green
        return
      }
    } catch {}
    Start-Sleep -Seconds 2
  }
  throw "No obtuve /health: ok dentro de $TimeoutSec segundos"
}

function EnsureUp {
  if ($Rebuild) {
    ComposeCmd @("down","-v","--remove-orphans")
    ComposeCmd @("build","--no-cache","api","worker")
  }
  ComposeCmd @("up","-d")
  ComposeCmd @("ps")
  # PING Redis
  ComposeCmd @("exec","-T","redis","redis-cli","PING") | ForEach-Object {
    if ($_ -match "PONG") { Write-Host "Redis PING OK" -ForegroundColor Green }
  }
  # Worker registrado
  ComposeCmd @("exec","-T","worker","celery","-A","app.workers.celery_app.celery_app","inspect","registered") | Out-String | ForEach-Object {
    if ($_ -match "app.workers.tasks.mcurvas_prepare") { Write-Host "Worker registra tareas OK" -ForegroundColor Green }
  }
  HealthCheck
}

function UploadMeta {
  param([string]$Path)
  if (-not (Test-Path $Path)) { throw "META no existe: $Path" }
  Write-Host "Subiendo META: $Path" -ForegroundColor Cyan
  $resp = & curl.exe -sS -X POST -F ("file=@{0}" -f $Path) "http://localhost:8000/meta/upload"
  Write-Host $resp
}

function UploadConsumos {
  param([string]$Path)
  if (-not (Test-Path $Path)) { throw "Consumos no existe: $Path" }
  Write-Host "Subiendo Consumos: $Path" -ForegroundColor Cyan
  $resp = & curl.exe -sS -X POST -F ("file=@{0}" -f $Path) "http://localhost:8000/ingest/upload"
  Write-Host "Respuesta: $resp"
  try { return ($resp | ConvertFrom-Json).job_id } catch { throw "No pude extraer job_id de la respuesta" }
}

function GetLatestJobId {
  try {
    $jobs = Invoke-RestMethod -Uri "http://localhost:8000/jobs" -TimeoutSec 10
    return $jobs[0].job_id
  } catch {
    throw "No pude consultar /jobs para obtener el último job"
  }
}

function WaitJob {
  param([string]$JobId)
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSec) {
    try {
      $st = Invoke-RestMethod -Uri ("http://localhost:8000/jobs/{0}" -f $JobId) -TimeoutSec 10
      $now = (Get-Date).ToString("HH:mm:ss")
      Write-Host ("[{0}] status={1}" -f $now, $st.status)
      if ($st.status -in @("done","failed","error")) { return $st.status }
    } catch {
      Write-Host "Error consultando /jobs/$JobId, reintento..." -ForegroundColor Yellow
    }
    Start-Sleep -Seconds $PollSec
  }
  throw "Timeout esperando job $JobId"
}

# ========== MAIN ==========

Write-Host "==== FRAUD PIPELINE RUNNER ====" -ForegroundColor Cyan
Write-Host "Proyecto: $ProjectDir" -ForegroundColor Cyan
EnsureUp

# META opcional
if ($MetaPath -and $MetaPath.Trim() -ne "") {
  UploadMeta -Path $MetaPath
  $metaCount = ExecSql "SELECT COUNT(*) FROM meta_fraude;" | Select-Object -Last 1
  Write-Host ("meta_fraude rows: {0}" -f $metaCount.Trim()) -ForegroundColor Green
}

# Consumos (sube archivo o toma último job)
$jobId = ""
if ($ConsumosPath -and $ConsumosPath.Trim() -ne "") {
  $jobId = UploadConsumos -Path $ConsumosPath
} else {
  Write-Host "No se pasó ConsumosPath, tomaré el último job de /jobs..." -ForegroundColor Yellow
  $jobId = GetLatestJobId
}
Write-Host ("JobId: {0}" -f $jobId) -ForegroundColor Green

# Esperar a que termine
$status = WaitJob -JobId $jobId
Write-Host ("Job finalizado con estado: {0}" -f $status) -ForegroundColor Green

# Métricas de BD
$stg = ExecSql "SELECT COUNT(*) FROM stg_consumo;" | Select-Object -Last 1
$feat = ExecSql "SELECT COUNT(*) FROM features_curvas;" | Select-Object -Last 1
$resu = ExecSql ("SELECT COUNT(*) FROM resultados WHERE job_id = '{0}';" -f $jobId) | Select-Object -Last 1
Write-Host ("stg_consumo : {0}" -f $stg.Trim())
Write-Host ("features_curvas : {0}" -f $feat.Trim())
Write-Host ("resultados (job) : {0}" -f $resu.Trim())

# Top 20 resultados
Write-Host "`nTop 20 resultados por score_hibrido:" -ForegroundColor Cyan
ExecSql @"
SELECT cuenta,
       ROUND(COALESCE(score_supervisado,0)::numeric,4) AS score_sup,
       ROUND(COALESCE(score_hibrido,0)::numeric,4)     AS score_hib,
       umbral_aplicado,
       decision
FROM resultados
WHERE job_id = '$jobId'
ORDER BY score_hibrido DESC NULLS LAST
LIMIT 20;
"@ | Write-Host

Write-Host "`nListo." -ForegroundColor Green
