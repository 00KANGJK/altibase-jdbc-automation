# Altibase DBMS 결함 탐지 테스트 기획서

## 1. 목적

기존 기능 검증 자동화를 넘어, DBMS 내부 결함을 발견하기 위한 심화 테스트 전략을 정의한다.  
중점은 “SQL이 성공하는가”가 아니라 “동일해야 하는 결과가 언제 달라지는가”, “실패해야 하는 작업이 성공하지 않는가”, “오류 이후에도 세션과 메타데이터가 정상 상태로 회복되는가”이다.

## 2. 공통 원칙

- 동일 의미의 SQL을 여러 형태로 실행하고 결과 set을 비교한다.
- memory table과 disk table, index 있음과 없음, 통계 수집 전후를 나누어 비교한다.
- 정상 케이스와 실패 케이스를 같은 fixture에서 다룬다.
- 오류 이후 같은 connection으로 후속 SQL을 실행해 세션 오염을 확인한다.
- 환경 의존 테스트는 feature flag와 execution flag를 모두 만족할 때만 실행한다.
- 실패 시 seed, schema, data, SQL, 기대 결과, 실제 결과를 재현 가능하게 남긴다.

## 3. 결함 후보 판정 기준

다음 중 하나라도 발생하면 결함 후보로 기록한다.

- 논리적으로 동치인 두 SQL의 결과 row set이 다르다.
- index 사용 여부, hint, 통계 수집 여부에 따라 row count 또는 값이 달라진다.
- rollback 이후 데이터가 남거나, commit 이후 다른 세션에서 보이지 않는다.
- 권한 revoke 이후에도 기존 statement 또는 synonym/view 경유 접근이 계속 성공한다.
- 실패 SQL 이후 connection이 정상 SQL을 처리하지 못한다.
- DB Link, replication, backup/recovery 작업 이후 시스템 메타데이터가 남거나 꼬인다.
- crash/restart 이후 table scan과 index scan 결과가 다르다.

## 4. 우선순위

| 우선순위 | 영역 | 이유 | 현재 환경 실행 가능성 |
|---:|---|---|---|
| 1 | Optimizer Wrong Result | DBMS 결함 가치가 가장 높고 JDBC만으로 검증 가능 | 높음 |
| 2 | Transaction / Concurrency | 데이터 정합성 결함을 직접 탐지 | 높음 |
| 3 | Type Boundary / Implicit Conversion | 경계값, 변환, 정렬 오류 탐지 | 높음 |
| 4 | DB Link Transaction Boundary | 원격 오류와 local session 오염 탐지 | 중간 |
| 5 | Privilege Escalation / Definer Rights | 보안 결함 탐지 | 높음 |
| 6 | Replication Consistency | 설정 준비 후 metadata와 데이터 전파 검증 | 중간 |
| 7 | Crash / Recovery Consistency | 결함 가치는 높지만 전용 instance 필요 | 낮음 |
| 8 | SQL Fuzzing / Randomized Metamorphic | 장기적으로 강력한 자동 결함 탐지 | 중간 |

## 5. 영역별 테스트 설계

### 5.1 Optimizer Wrong Result

목적: optimizer rewrite, join order, predicate pushdown, index access path 변화로 잘못된 결과가 나오는지 찾는다.

주요 케이스:

| ID | 시나리오 | 비교 방식 | 기대 |
|---|---|---|---|
| OPT-WR-001 | `INNER JOIN` vs `EXISTS` | canonical row set 비교 | 동일 |
| OPT-WR-002 | `NOT EXISTS` vs `LEFT JOIN IS NULL` | null-heavy data 비교 | 동일 |
| OPT-WR-003 | `NOT IN` + `NULL` | SQL semantics를 명시적으로 검증 | 정의된 차이만 허용 |
| OPT-WR-004 | outer join predicate 위치 | `ON` 조건과 `WHERE` 조건 차이 분리 | 의도한 결과 |
| OPT-WR-005 | `GROUP BY` + `HAVING` rewrite | subquery 집계와 직접 집계 비교 | 동일 |
| OPT-WR-006 | index scan vs full scan | index 생성 전후 결과 비교 | 동일 |
| OPT-WR-007 | 통계 수집 전후 | 같은 SQL 결과 비교 | 동일 |
| OPT-WR-008 | top-N query | 전체 정렬 후 slicing과 직접 top-N 비교 | 동일 |

구현 순서:

1. small, null-heavy, duplicate-heavy fixture를 만든다.
2. row set canonicalizer를 만든다.
3. index/no-index, memory/disk 조합을 순회한다.
4. 실패 시 SQL과 fixture seed를 남긴다.

### 5.2 Transaction / Concurrency

목적: 동시 세션에서 lock, isolation, commit/rollback, DDL/DML 경합이 일관적으로 처리되는지 검증한다.

주요 케이스:

| ID | 시나리오 | 절차 | 기대 |
|---|---|---|---|
| TX-001 | commit visibility | A insert commit, B select | commit 후 보임 |
| TX-002 | rollback invisibility | A insert rollback, B select | 보이지 않음 |
| TX-003 | failed statement state | 중복 key 오류 후 후속 insert | transaction 정상 |
| TX-004 | savepoint 반복 rollback | savepoint/rollback 반복 후 count 확인 | 누락 없음 |
| TX-005 | row lock conflict | A update hold, B update 시도 | timeout 또는 lock 오류 |
| TX-006 | deadlock detection | A/B 교차 update | 한쪽이 명확히 실패 |
| TX-007 | DDL 중 DML | alter/create index와 select 경합 | 데이터 정합성 유지 |
| TX-008 | batch rollback | batch 중 일부 실패 | transaction 정책에 맞는 반영 |

구현 순서:

1. multi-connection helper와 barrier를 만든다.
2. timeout을 짧게 고정해 flaky 가능성을 줄인다.
3. 오류 이후 같은 connection으로 sanity query를 실행한다.

### 5.3 Type Boundary / Implicit Conversion

목적: 숫자, 문자, 날짜, national character, implicit conversion 경계에서 저장/비교/정렬 오류를 찾는다.

주요 케이스:

| ID | 시나리오 | 입력 | 기대 |
|---|---|---|---|
| TYPE-001 | integer overflow | max+1, min-1 | 실패 |
| TYPE-002 | numeric rounding | precision/scale 경계값 | 정의된 반올림 또는 실패 |
| TYPE-003 | string-to-number | `001`, `+1`, `1.0`, `1e2` | 명시 변환과 동일 |
| TYPE-004 | invalid numeric string | `1a`, empty string | 실패 |
| TYPE-005 | date boundary | leap day, month-end | 정상/실패 분리 |
| TYPE-006 | invalid date | month 13, invalid leap day | 실패 |
| TYPE-007 | CHAR trailing space | `A`, `A   ` | DB semantics 고정 |
| TYPE-008 | byte length vs char length | multibyte string | length 계열 일관성 |

구현 순서:

1. value matrix를 정의한다.
2. insert, select, compare, order by를 같은 데이터로 실행한다.
3. 실패 케이스는 오류 코드와 메시지 패턴을 검증한다.

### 5.4 DB Link Transaction Boundary

목적: DB Link를 통한 remote query와 오류가 local transaction/session에 영향을 주지 않는지 확인한다.

주요 케이스:

| ID | 시나리오 | 절차 | 기대 |
|---|---|---|---|
| DBL-001 | remote SELECT 반복 | 같은 remote query 100회 | 결과 동일, session 누수 없음 |
| DBL-002 | remote 오류 후 local SQL | missing table 조회 후 local insert | local session 정상 |
| DBL-003 | wrong credential link | 잘못된 link 반복 사용 | 정상 link 사용 가능 |
| DBL-004 | link drop 후 재사용 | drop 이후 remote query 재시도 | 명확히 실패 |
| DBL-005 | session count 추적 | `v$dblink_altilinker_status` 확인 | 누수 없음 |

필요 환경:

- DB Link 활성화
- AltiLinker 실행
- 테스트용 self target 또는 별도 target

### 5.5 Privilege Escalation / Definer Rights

목적: grant/revoke, role, synonym, view, procedure 경계에서 권한 우회나 stale privilege가 남는지 검증한다.

주요 케이스:

| ID | 시나리오 | 절차 | 기대 |
|---|---|---|---|
| SEC-001 | revoke 후 prepared select | grant, prepare, revoke, execute | 실패 |
| SEC-002 | synonym 권한 우회 | synonym 생성 후 원본 권한 revoke | 실패 |
| SEC-003 | view definer 권한 | owner view를 consumer가 select | 허용 범위만 성공 |
| SEC-004 | procedure definer 권한 | procedure 내부 DML | 권한 모델대로 동작 |
| SEC-005 | role vs direct grant | role grant와 direct grant 비교 | 차이를 명확히 고정 |
| SEC-006 | public/private synonym shadowing | 이름 충돌 resolution | 우선순위 일관 |

구현 순서:

1. owner, consumer, attacker 세 계정을 fixture로 만든다.
2. 각 테스트 끝에서 user/object를 역순 정리한다.
3. 권한 오류는 SQLState보다 DBMS 오류 메시지 패턴을 우선 검증한다.

### 5.6 Replication Consistency

목적: replication DDL, metadata cleanup, start/stop, 데이터 전파 일관성을 확인한다.

현재 self-hosted 환경에서는 metadata와 negative 중심으로 검증하고, 2-node 환경이 준비되면 데이터 전파 검증으로 확장한다.

주요 케이스:

| ID | 시나리오 | 환경 | 기대 |
|---|---|---|---|
| REPL-001 | create/drop metadata | self-hosted 가능 | metadata 정리 |
| REPL-002 | add/drop table | self-hosted 가능 | item metadata 일관 |
| REPL-003 | start/stop 오류 경계 | self-hosted 가능 | 명확한 오류 또는 상태 |
| REPL-004 | insert/update/delete 전파 | 2-node 필요 | source/target 동일 |
| REPL-005 | conflict 처리 | 2-node 필요 | conflict 상태 명확 |
| REPL-006 | network 중단 후 catch-up | 2-node 필요 | 누락 없음 |

### 5.7 Crash / Recovery Consistency

목적: commit, checkpoint, backup, crash, recovery 경계에서 데이터와 index가 일관성을 유지하는지 검증한다.

이 영역은 공유 DB가 아니라 전용 instance에서만 실행한다.

주요 케이스:

| ID | 시나리오 | 절차 | 기대 |
|---|---|---|---|
| REC-001 | commit 직후 crash | commit, 강제 종료, restart | row 존재 |
| REC-002 | rollback 중 crash | rollback 전후 crash | rollback row 없음 |
| REC-003 | checkpoint 중 crash | checkpoint 경합 중 종료 | recovery 후 count 정상 |
| REC-004 | index consistency | recovery 후 index scan/full scan 비교 | 동일 |
| REC-005 | online backup restore | backup, restore, checksum 비교 | 동일 |
| REC-006 | missing archive log | 일부 log 누락 | 명확한 복구 실패 |

### 5.8 SQL Fuzzing / Randomized Metamorphic

목적: 제한된 grammar 안에서 무작위 SQL을 생성해 optimizer/type/expression 결함을 장기적으로 찾는다.

전략:

- 완전 무작위가 아니라 schema와 data type을 알고 있는 constrained random을 사용한다.
- seed 기반으로 재현 가능하게 만든다.
- 동치 변환 가능한 SQL pair만 생성한다.
- 짧은 smoke seed와 긴 nightly seed를 분리한다.

주요 케이스:

| ID | 시나리오 | 방식 | 기대 |
|---|---|---|---|
| FUZZ-001 | predicate commutativity | `a AND b` vs `b AND a` | 동일 |
| FUZZ-002 | arithmetic identity | `c + 0`, `c * 1` | 동일 |
| FUZZ-003 | join reorder | join 순서 변경 | 동일 |
| FUZZ-004 | subquery vs join | `IN`, `EXISTS`, `JOIN` 변환 | 동일 |
| FUZZ-005 | aggregate decomposition | 직접 집계 vs subquery 집계 | 동일 |
| FUZZ-006 | index/no-index | 같은 data에서 index 유무 비교 | 동일 |

## 6. 실행 프로파일

| 프로파일 | 목적 | 실행 위치 | 예상 시간 |
|---|---|---|---|
| `defect-smoke` | 빠른 결함 탐지 smoke | JDBC runner | 5분 이내 |
| `defect-deep` | 반복, 동시성, seed 확대 | JDBC runner | 30분 이상 |
| `defect-linux-cli` | CLI와 서버 파일 검증 | Linux server | 환경별 |
| `defect-destructive` | crash/recovery/lifecycle | 전용 instance | 환경별 |
| `defect-replication-2node` | 실제 replication consistency | 2-node 또는 2-instance | 환경별 |

## 7. 1차 구현 범위

1. Optimizer wrong-result metamorphic test
2. Transaction/concurrency anomaly test
3. Type/implicit conversion boundary test

이 세 영역은 현재 JDBC 중심 환경에서 바로 확장 가능하며, 실패 시 DBMS 결함 후보로서 가치가 높다.

## 8. 완료 기준

- 각 영역별 smoke test가 존재한다.
- 실패 케이스가 정상 케이스와 함께 검증된다.
- feature flag로 환경 의존 테스트가 분리된다.
- 실패 시 최소 재현 SQL과 seed를 남길 수 있다.
- 전체 테스트는 반복 실행 시 flaky 없이 유지된다.
