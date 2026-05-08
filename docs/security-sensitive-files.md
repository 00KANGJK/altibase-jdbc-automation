# Security-Sensitive Files

이 문서는 포트폴리오 공개 전에 저장소에서 제외하거나 내용을 점검해야 하는 파일을 분류합니다.

## 절대 공개하지 않는 파일

| 분류 | 경로 또는 패턴 | 이유 | 처리 |
|---|---|---|---|
| 로컬 실행 설정 | `src/test/resources/config/application-test.yml`, `src/test/resources/config/application-local.yml` | 실제 DB 주소, 계정, 비밀번호, 서버 경로가 들어갈 수 있음 | `.gitignore` 유지 |
| 원본 명세/검토 자료 | `*.xls`, `*.xlsx` | 고객/프로젝트 식별자와 원본 테스트 명세가 포함될 수 있음 | 저장소 밖에 보관 |
| 개인 도구 설정 | `.claude/`, `.idea/`, `.vscode/` | 개인 작업 이력, 로컬 경로, IDE 상태가 포함될 수 있음 | `.gitignore` 유지 |
| 환경변수 파일 | `.env`, `.env.*` | 계정, 비밀번호, endpoint가 포함될 수 있음 | `.gitignore` 유지 |
| 테스트 실행 결과 | `allure-results/`, `allure-report/`, `*.log` | SQL, endpoint, 오류 stack trace, 서버 경로가 포함될 수 있음 | 재생성 가능한 산출물로 취급 |

## 라이선스 확인이 필요한 파일

| 분류 | 경로 또는 패턴 | 이유 | 처리 |
|---|---|---|---|
| JDBC 드라이버 | `lib/Altibase.jar` | 벤더 바이너리이므로 재배포 가능 여부를 라이선스로 확인해야 함 | 공개 저장소에는 포함하지 않음 |
| 외부 클라이언트 도구 | `client/`, `bin/`, `*.jar` | 설치 패키지나 실행 바이너리는 소스 코드가 아님 | 설치 절차만 문서화 |

## 공개 전 점검 대상

| 분류 | 경로 또는 패턴 | 확인할 내용 |
|---|---|---|
| 기본 테스트 설정 템플릿 | `src/test/resources/config/application-test.example.yml` | 공개 가능한 placeholder만 포함되어야 함 |
| 문서 | `README.md`, `docs/*.md` | 고객명, 프로젝트 코드명, 실제 서버 주소, 계정 정보가 없어야 함 |
| 재현 스크립트 | `scripts/*.java`, `scripts/*.py` | hard-coded 접속 정보와 로컬 절대 경로가 없어야 함 |
| 샘플 SQL | `sql/`, `samples/` | 운영 데이터나 개인 계정명이 없어야 함 |

## 공개 전 체크리스트

1. 원본 workbook과 재가공 산출물이 저장소 밖에 있는지 확인한다.
2. `application-test.yml`과 `application-local.yml`이 git 추적 대상이 아닌지 확인한다.
3. 공개 템플릿에는 loopback 주소와 placeholder 비밀번호만 남긴다.
4. `lib/Altibase.jar`를 공개 저장소에 포함하지 않는다.
5. Allure 결과, log, compiled class, Python cache를 제거한다.
6. 다음 유형의 문자열이 남아 있지 않은지 검색한다.

```bash
git grep -n -E "([0-9]{1,3}\.){3}[0-9]{1,3}|/home/[^/[:space:]]+|password:[[:space:]]+[^<[:space:]]+"
```

검색 결과가 나오면 공개 가능한 placeholder인지 확인하고, 실제 환경값이면 로컬 override나 환경변수로 옮긴다.
