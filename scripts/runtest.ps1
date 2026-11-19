$ErrorActionPreference = "Stop"

function Remove-Force {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Paths
    )
    foreach ($path in $Paths) {
        Get-Item -Path $path -Force -ErrorAction Ignore | Remove-Item -Recurse -Force
    }
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$ScriptBlock
    )
    $global:LASTEXITCODE = 0
    & $ScriptBlock
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Invoke-Optional {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$ScriptBlock
    )
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $global:LASTEXITCODE = 0
        & $ScriptBlock 2>$null | Out-Null
        return ($LASTEXITCODE -eq 0)
    } finally {
        $ErrorActionPreference = $prevEap
    }
}

function Invoke-WithPythonPath {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$ScriptBlock
    )
    $prevPythonPath = $env:PYTHONPATH
    try {
        $env:PYTHONPATH = "."
        Invoke-Checked $ScriptBlock
    } finally {
        if ($null -eq $prevPythonPath) {
            Remove-Item Env:PYTHONPATH -ErrorAction Ignore
        } else {
            $env:PYTHONPATH = $prevPythonPath
        }
    }
}

function ConvertTo-Lf {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )
    $fullPath = (Resolve-Path -LiteralPath $Path).Path
    $content = [System.IO.File]::ReadAllText($fullPath)
    $normalized = $content.Replace("`r`n", "`n")
    if ($normalized -ne $content) {
        [System.IO.File]::WriteAllText($fullPath, $normalized, (New-Object System.Text.UTF8Encoding($false)))
    }
}

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PrevKeyringBackend = $env:PYTHON_KEYRING_BACKEND

Push-Location $RepoRoot
try {
    $env:PYTHON_KEYRING_BACKEND = "keyring.backends.null.Keyring"

    $CpuCores = (
        Get-CimInstance -ClassName Win32_Processor -ErrorAction Ignore |
        Measure-Object -Property NumberOfCores -Sum
    ).Sum
    if (-not ($CpuCores -as [int]) -or [int]$CpuCores -lt 1) {
        $CpuCores = [Environment]::ProcessorCount
    }
    if (-not ($CpuCores -as [int]) -or [int]$CpuCores -lt 1) {
        $CpuCores = 1
    }
    $CpuCores = [int]$CpuCores

    # cleanup (works like `rm -rf`: ignores missing paths, removes hidden/read-only items)
    Get-ChildItem -Path neosqlite, tests, scripts, packages -Recurse -Directory -Force -Filter "__pycache__" -ErrorAction Ignore |
        Remove-Item -Recurse -Force
    Remove-Force "*\__pycache__", ".pytest_cache", ".coverage", ".coverage.*"

    # update existing packages as defined in pyproject.toml
    Invoke-Checked { poetry update }

    # install current project as editable
    Invoke-Checked { poetry run pip install -q -e ".[docs,memory-constrained]" }

    # install jsbox if a prebuilt wheel is available for this platform
    if (-not (Invoke-Optional { poetry run pip install -q --only-binary=jsbox -e ".[js]" })) {
        Write-Host "Note: prebuilt jsbox wheel not available for this platform; skipping jsbox."
    }

    # regenerate poetry.lock based on installed/updated packages
    Remove-Force "poetry.lock"
    Invoke-Checked { poetry lock --no-cache }
    ConvertTo-Lf "poetry.lock"

    # run unit tests
    Invoke-WithPythonPath {
        poetry run pytest tests/ -n $CpuCores --cov=neosqlite --cov-report=term-missing --cov-fail-under=80
    }

    # NX-27017
    Push-Location (Join-Path $RepoRoot "packages\nx_27017")
    try {
        Get-ChildItem -Path src, tests -Recurse -Directory -Force -Filter "__pycache__" -ErrorAction Ignore |
            Remove-Item -Recurse -Force
        Remove-Force "*\__pycache__", ".pytest_cache", ".coverage", ".coverage.*"

        # Install both the local NeoSQLite tree and nx-27017 as editable, plus
        # the test runner. uvloop (the [speed] extra) does not support Windows,
        # so install `-e .` instead of `-e .[speed]`.
        Invoke-Checked {
            poetry run pip install -q -e ../.. -e . pytest pytest-asyncio pytest-cov pytest-xdist
        }

        # regenerate poetry.lock based on installed/updated packages
        Remove-Force "poetry.lock"
        Invoke-Checked { poetry lock --no-cache }
        ConvertTo-Lf "poetry.lock"

        # run unit tests
        Invoke-WithPythonPath {
            poetry run pytest tests/ -n $CpuCores --cov=nx_27017 --cov-report=term-missing
        }
    } finally {
        Pop-Location
    }
} finally {
    if ($null -eq $PrevKeyringBackend) {
        Remove-Item Env:PYTHON_KEYRING_BACKEND -ErrorAction Ignore
    } else {
        $env:PYTHON_KEYRING_BACKEND = $PrevKeyringBackend
    }
    Pop-Location
}
