FROM node:18-alpine

WORKDIR /app

# 패키지 파일 복사 및 설치
COPY package*.json ./
RUN npm install

# 소스 코드 복사
COPY . .

# Next.js 앱 빌드
RUN npm run build

# 포트 설정
EXPOSE 3000

# 실행 명령
CMD ["npm", "start"] 