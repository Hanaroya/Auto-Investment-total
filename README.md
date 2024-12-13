# 자동 투자 연구 프로젝트 🚀

## 프로젝트 개요
이 프로젝트는 자동화된 투자 전략 연구 및 개발을 위한 레포지토리입니다.

## 주요 연구 분야
### 1. 기술적 분석
- 이동평균선 전략
- RSI (상대강도지수) 활용
- MACD (이동평균수렴확산) 분석
- 볼린저 밴드 전략

### 2. 퀀트 투자 전략
- 모멘텀 전략
- 밸류 투자 전략
- 팩터 투자 방법론
- 리스크 패리티 전략

### 3. 머신러닝 적용
- 가격 예측 모델
- 감성 분석을 통한 시장 동향 파악
- 포트폴리오 최적화
- 리스크 관리 모델

## 설정 파일 관리

### 설정 파일 구조
프로젝트는 `resource/application.yml` 파일을 통해 다음 설정들을 관리합니다:
- API 키 (거래소, 외부 서비스)
- 메신저 설정 (Slack, Gmail 등)
- 데이터베이스 연결 정보
- 사용자 정보

### 초기 설정 방법
1. 설정 파일 템플릿을 복사합니다:
```bash
cp resource/application.yml.bak resource/application.yml
```

2. 복사한 파일에서 실제 값을 설정합니다:
```bash
vim resource/application.yml
```

### 새로운 설정 추가 방법
1. `application.yml.bak` 파일에 새로운 설정 구조를 먼저 추가합니다:
```yaml
api_keys:
  new_exchange:    # 새로운 거래소 추가 예시
    access_key: "your_access_key"
    secret_key: "your_secret_key"
```

2. 팀원들에게 설정 변경 사항을 공유합니다.
3. 각자의 `application.yml`에 실제 값을 추가합니다.

### 주의사항
- `application.yml`은 `.gitignore`에 포함되어 있어 깃허브에 업로드되지 않습니다.
- 실제 API 키와 비밀번호는 절대 `application.yml.bak`에 포함하지 마세요.
- 설정 구조를 변경할 때는 반드시 팀원들과 상의 후 진행하세요.

## 기술 스택
- Python
- pandas
- numpy
- scikit-learn
- TensorFlow/PyTorch
- TA-Lib

## 설치 방법

- git clone https://github.com/Hanaroya/Auto-Investment-total
- cd Auto-Investment-total
- pip install -r requirements.txt

## 사용 방법
1. 환경 설정
2. 데이터 수집
3. 전략 백테스팅
4. 결과 분석

## 주의사항
- 이 프로젝트는 연구 목적으로만 사용됩니다
- 실제 투자는 본인의 책임하에 진행하시기 바랍니다
- 과거의 수익이 미래의 수익을 보장하지 않습니다

## 기여 방법
1. Fork the Project
2. Create your Feature Branch
3. Commit your Changes
4. Push to the Branch
5. Open a Pull Request

## 라이선스
MIT License

## 연락처
- Email: kimmo6072@gmail.com
- Blog: https://hanaroya.github.io/
