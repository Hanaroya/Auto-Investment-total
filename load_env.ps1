# .env 파일 내용을 읽어서 환경 변수로 설정
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        [Environment]::SetEnvironmentVariable($name, $value, 'Process')
        Write-Host "환경 변수 설정: $name"
    }
}

# 가상환경 활성화
.\.venv\Scripts\Activate.ps1

Write-Host "환경 변수 로드 및 가상환경 활성화 완료" 