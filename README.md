# 🚀 Altibase 7.3 JDBC Automation Test Suite

본 프로젝트는 **Altibase 7.3 DBMS**의 JDBC 드라이버 품질 검증을 위한 자동화 테스트 프레임워크입니다.  
**ISO/IEC 25023** 및 **25051** 국제 표준의 품질 지표를 기반으로 설계되었습니다.

---

## 📌 Project Overview
* **대상 DBMS:** Altibase 7.3 (Hybrid DB: Memory & Disk)
* **테스트 범위:** JDBC API (Driver, Connection, Statement, ResultSet, Metadata 등 총 246건)
* **주요 목표:** * JDBC 4.2 스펙 준수 여부 검증
    * 하이브리드 아키텍처에 따른 데이터 정합성 확인
    * 성능 효율성(Time-behaviour) 및 신뢰성 측정

---

## 🛠 Tech Stack
* **Language:** Java 17
* **Test Framework:** JUnit 5 (Jupiter)
* **Assertion:** AssertJ
* **Build Tool:** Maven
* **Reporting:** Allure Report
* **DB Client:** DBeaver (Verification & Mock Data)

---

## 📂 Project Structure
```text
altibase-jdbc-test/
├── src/test/java/com/altibase/qa/
│   ├── base/           # 공통 인프라 (ConnectionFactory, BaseClass)
│   ├── suites/         # TC 카테고리별 테스트 클래스
│   └── utils/          # 성능 측정 및 로깅 유틸리티
└── src/test/resources/
    ├── application.yml # DB 접속 정보 (Git 제외 대상)
    └── sql/            # 테스트 스키마/프로시저 DDL
```

---

## ⚙️ Prerequisites
1. **Java 17** 이상 설치
2. **Altibase JDBC Driver** 설치
   * `lib/Altibase.jar` 파일을 로컬 메이븐 레포지토리에 등록해야 합니다.
   ```bash
   mvn install:install-file -Dfile=lib/Altibase.jar -DgroupId=com.altibase -DartifactId=altibase-jdbc -Dversion=7.3 -Dpackaging=jar
   ```
3. **접속 정보 설정**
   * `src/test/resources/application.yml` 파일을 생성하고 접속 정보를 입력하세요. (보안상의 이유로 깃허브에는 포함되지 않습니다.)

---

## 🏃 How to Run
### 1. 전체 테스트 실행
```bash
mvn test
```

### 2. 특정 카테고리 테스트 실행 (JUnit Tags)
```bash
mvn test -Dgroups="connection"
```

---

## 📊 Reporting
본 프로젝트는 Allure를 통해 시각화된 품질 리포트를 생성합니다.
```bash
# 테스트 완료 후 리포트 생성 및 확인
mvn allure:serve
```
리포트에서는 **성능 응답 시간**, **테스트 성공률**, **에러 상세 로그**를 확인할 수 있으며 이는 **ISO/IEC 25023** 품질 측정 지표로 활용됩니다.

---

## 📝 License
Copyright (c) 2026. Kang Junhyeok. All rights reserved.

---
