@echo off
REM 가상환경 활성화를 위한 배치 파일

REM 실행 정책 변경 (관리자 권한으로 실행)
powershell -Command "Start-Process powershell -Verb RunAs -ArgumentList 'Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned -Force'"

REM 잠시 대기
timeout /t 2

REM 가상환경 활성화
powershell -Command "& {Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass; .\.venv\Scripts\Activate.ps1}"

REM 활성화 성공 메시지 출력
echo 가상환경이 활성화되었습니다.

REM 명령 프롬프트 유지
cmd /k 