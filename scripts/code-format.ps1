$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    $global:LASTEXITCODE = 0
    poetry run black -t py312 -l 80 .
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    # Remove trailing horizontal whitespace in all .py files while preserving
    # existing line endings (LF stays LF) and skipping VCS/env/build folders.
    $skipDirs = @(
        ".hg", ".git", ".venv", "venv", "build", "dist",
        ".tox", ".pytest_cache", "__pycache__"
    )
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    $trailingWs = [regex]"(?m)[ \t]+(?=\r?$)"

    Get-ChildItem -Path $RepoRoot -Recurse -File -Filter "*.py" | ForEach-Object {
        $rel = $_.FullName.Substring($RepoRoot.Length).TrimStart("\", "/")
        $parts = $rel -split "[\\/]"
        if ($parts.Length -gt 1) {
            foreach ($part in $parts[0..($parts.Length - 2)]) {
                if ($skipDirs -contains $part) {
                    return
                }
            }
        }

        $content = [System.IO.File]::ReadAllText($_.FullName, $utf8NoBom)
        $cleaned = $trailingWs.Replace($content, "")
        if ($cleaned -ne $content) {
            [System.IO.File]::WriteAllText($_.FullName, $cleaned, $utf8NoBom)
        }
    }
} finally {
    Pop-Location
}
