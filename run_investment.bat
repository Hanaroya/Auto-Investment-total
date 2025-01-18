@echo off
echo Investment Center 시작...

:: 프로젝트 디렉토리로 이동
cd /d C:\Users\ASUS\Documents\GitHub\Auto-Investment-total

:: 가상환경 활성화 및 환경변수 로드 확인
echo 환경변수 로드 중...
if exist .env (
    echo .env 파일이 존재합니다.
) else (
    echo .env 파일이 없습니다. 프로그램이 정상적으로 동작하지 않을 수 있습니다.
    pause
)

call .venv\Scripts\activate.bat

:: 메인 프로그램 실행
python main.py

:: 실행 완료 후 일시 정지
pause 