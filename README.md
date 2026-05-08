# Altibase JDBC Test Automation

Altibase 7.3 DBMS 기능을 JDBC 기반으로 자동 검증하는 테스트 자동화 프로젝트입니다.  
포트폴리오 공개용 문서와 실제 로컬 테스트 설정을 분리해, 테스트는 기존 환경에서 그대로 실행하고 민감 정보는 Git에 올리지 않는 구조로 정리했습니다.

## 프로젝트 목표

- Altibase JDBC 드라이버와 DBMS 주요 기능의 회귀 테스트 자동화
- 정상 케이스뿐 아니라 실패 케이스, 경계값, 권한 오류, 메타데이터 정합성 검증
- 환경 의존 기능을 feature flag로 분리해 안전하게 실행 범위 제어
- DBMS 결함 탐지를 위한 metamorphic, 동시성, 복구, replication 확장 기반 마련

## 기술 스택

| 항목 | 버전 |
|---|---|
| Java | 17 |
| Maven | 3.x |
| JUnit Jupiter | 5.10.2 |
| AssertJ | 3.27.7 |
| SnakeYAML | 2.2 |
| Allure JUnit5 | 2.25.0 |
| Target DBMS | Altibase 7.3 |

## 현재 상태

최근 검증 환경 기준으로 `1,000+`개의 JUnit 테스트가 구성되어 있습니다. DB Link, replication, backup/recovery처럼 서버 설정이 필요한 영역은 `features.*`와 `execution.*` flag를 모두 만족할 때만 실행됩니다.

| 영역 | 주요 검증 |
|---|---|
| Smoke | 설정 로드, JDBC 드라이버 로드, 기본 접속 |
| Schema | tablespace, table, constraint, index, view, sequence, directory |
| JDBC API | Connection, Statement, PreparedStatement, CallableStatement, ResultSet, metadata |
| Data Type | 숫자/문자/날짜 타입 경계값, 암묵 변환, 실패 케이스 |
| SQL Function | 집계, 문자열, 날짜, 변환, 암호화, window/set 연산 |
| Security | user, role, grant/revoke, synonym/view/procedure 권한 경계 |
| Transaction | commit, rollback, savepoint, lock, batch rollback |
| Communication | stored packages, TCP, SMTP 기능 게이트 |
| Optimizer | hints, plan cache, result cache, statistics, explain plan |
| DB Link | self target link, remote query, 오류 경계 |
| Replication | replication DDL, metadata, self-hosted smoke |
| Backup/Recovery | checkpoint, online backup, tablespace backup, archive mode gate |

## 구조

```text
altibase-jdbc-automation/
├─ pom.xml
├─ lib/
│  └─ Altibase.jar
├─ docs/
│  ├─ dbms-defect-discovery-test-plan.md
│  └─ security-sensitive-files.md
├─ src/main/java/com/altibase/qa/
│  ├─ config/
│  └─ infra/
└─ src/test/
   ├─ java/com/altibase/qa/
   │  ├─ smoke/
   │  ├─ schema/
   │  ├─ jdbc/
   │  ├─ security/
   │  ├─ transaction/
   │  ├─ communication/
   │  ├─ optimizer/
   │  ├─ dblink/
   │  ├─ replication/
   │  └─ recovery/
   └─ resources/config/
      ├─ application-test.yml          # 로컬 테스트용 실제 설정, Git 제외
      ├─ application-test.example.yml  # 공개 가능한 템플릿
      └─ application-local.example.yml # 선택적 로컬 override 템플릿
```

## 설정 방식

로컬 테스트 실행은 기존처럼 `config/application-test.yml`을 사용합니다.

```text
-Daltibase.test.config=config/application-test.yml
```

중요한 점은 `src/test/resources/config/application-test.yml`이 실제 테스트 환경값을 담는 **로컬 전용 파일**이라는 것입니다. 이 파일은 `.gitignore`에 포함되어 있으므로 Git에 올리지 않습니다.

새 환경에서 시작할 때는 공개 템플릿을 복사해 실제 값을 채웁니다.

```bash
cp src/test/resources/config/application-test.example.yml \
   src/test/resources/config/application-test.yml
```

실제 설정에는 다음 값이 포함됩니다.

- DB host, port, database, user, password, JDBC URL
- client/server binary 경로
- backup, datafile, export, script, log 작업 디렉터리
- DB Link, replication, backup/recovery, stored package feature flag
- TCP/SMTP 테스트 endpoint

환경변수로도 주요 값을 덮어쓸 수 있습니다.

```bash
ALTIBASE_TEST_DB_HOST=127.0.0.1
ALTIBASE_TEST_DB_PORT=20300
ALTIBASE_TEST_DB_NAME=mydb
ALTIBASE_TEST_DB_USER=sys
ALTIBASE_TEST_DB_PASSWORD=<local-password>
ALTIBASE_TEST_JDBC_URL=jdbc:Altibase://127.0.0.1:20300/mydb

ALTIBASE_ENABLE_DB_TESTS=true
ALTIBASE_ENABLE_CLI_TESTS=false
ALTIBASE_ENABLE_DESTRUCTIVE_TESTS=false
ALTIBASE_FEATURE_DATABASE_LINK=true
ALTIBASE_FEATURE_REPLICATION=true
ALTIBASE_FEATURE_BACKUP_RECOVERY=true
```

## 실행

Maven CLI:

```bash
mvn test -Daltibase.test.config=config/application-test.yml
```

특정 테스트만 실행:

```bash
mvn test -Dtest="com.altibase.qa.schema.TablespaceJdbcTest" -Daltibase.test.config=config/application-test.yml
mvn test -Dtest="com.altibase.qa.dblink.DatabaseLinkJdbcTest" -Daltibase.test.config=config/application-test.yml
```

Allure 리포트:

```bash
mvn allure:serve
```

## 테스트 작성 패턴

모든 DB 테스트는 공통 base와 cleanup stack을 사용합니다.

```java
class ExampleJdbcTest extends BaseDbTest {

    @Test
    void createsAndReadsTable() {
        String tableName = DbTestSupport.uniqueName("QA_EXAMPLE");
        registerCleanup(() -> DbTestSupport.dropTableQuietly(jdbc, connection, tableName));

        jdbc.executeUpdate(connection, "create table " + tableName + "(c1 integer)");
        jdbc.executeUpdate(connection, "insert into " + tableName + " values(1)");

        assertThat(jdbc.queryForString(connection, "select count(*) from " + tableName))
                .isEqualTo("1");
    }
}
```

## 환경 의존 기능

다음 영역은 공유 DB에서 무심코 실행되지 않도록 별도 flag로 보호합니다.

| 기능 | 필요한 준비 |
|---|---|
| DB Link | DB Link 활성화, AltiLinker 실행, target 설정 |
| Replication | replication port 활성화, 테스트용 replication target |
| Backup/Recovery | archive mode, 백업 디렉터리, 서버 파일 권한 |
| TCP/SMTP package | 테스트용 local endpoint |
| CLI utility | 서버 또는 클라이언트의 iSQL/iLoader/aexport 경로 |
| Destructive lifecycle | 전용 Altibase instance |

## 보안 및 공개 기준

공개 전에는 [docs/security-sensitive-files.md](docs/security-sensitive-files.md)를 기준으로 원본 명세, 로컬 설정, 실행 결과, 벤더 바이너리, IDE 설정이 포함되지 않았는지 확인합니다.

포트폴리오에는 테스트 자동화 코드와 공개 가능한 템플릿만 포함합니다. 실제 테스트 환경 정보는 `application-test.yml`, `application-local.yml`, 환경변수로만 관리합니다.
