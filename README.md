# Altibase JDBC Automation

Workbook-driven Altibase test automation for the `altibase_TC` test case set.

## Scope

This repository focuses on the parts that can be executed safely from a Windows + JDBC environment.

Included:

- JDBC API tests
- schema DDL and DML tests
- privilege grant and revoke tests
- stored program lifecycle tests
- function and conversion tests
- multi-session and transaction tests

Excluded or separated for now:

- Linux-only CLI scenarios
- server control, recovery, backup, and replication
- filesystem-heavy validation that requires direct server-side inspection
- environment-dependent scenarios that need special charset or multi-node setup

## Environment

- DBMS: Altibase 7.3.0.1.8
- Database: `mydb`
- IDE: IntelliJ IDEA on Windows

Config files:

- [application-test.yml](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/resources/config/application-test.yml)
- [application-local.example.yml](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/resources/config/application-local.example.yml)

Recommended local override:

```yaml
execution:
  enableDbTests: true
  enableCliTests: false
  enableDestructiveTests: false
  enableRecoveryTests: false
  enableReplicationTests: false
```

## Executable Test Suites

Windows / JDBC safe suites:

- [ConfigLoadTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/smoke/ConfigLoadTest.java)
- [DriverLoadTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/smoke/DriverLoadTest.java)
- [BasicQuerySmokeTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/smoke/BasicQuerySmokeTest.java)
- [AutocommitSmokeTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/smoke/AutocommitSmokeTest.java)
- [MultiSessionBasicTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/transaction/MultiSessionBasicTest.java)
- [TransactionJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/transaction/TransactionJdbcTest.java)
- [NonPartitionedTableJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/NonPartitionedTableJdbcTest.java)
- [TemporaryTableJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/TemporaryTableJdbcTest.java)
- [PartitionedTableJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/PartitionedTableJdbcTest.java)
- [ConstraintJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/ConstraintJdbcTest.java)
- [IndexViewSequenceJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/IndexViewSequenceJdbcTest.java)
- [DirectoryJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/DirectoryJdbcTest.java)
- [DataTypeJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/DataTypeJdbcTest.java)
- [JobJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/JobJdbcTest.java)
- [StoredProgramJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/StoredProgramJdbcTest.java)
- [StoredProgramLifecycleJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/schema/StoredProgramLifecycleJdbcTest.java)
- [UserRoleJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/security/UserRoleJdbcTest.java)
- [PrivilegeGrantJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/security/PrivilegeGrantJdbcTest.java)
- [PrivilegeRevokeJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/security/PrivilegeRevokeJdbcTest.java)
- [DriverJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/DriverJdbcTest.java)
- [JdbcMetadataAndFunctionTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/JdbcMetadataAndFunctionTest.java)
- [ConversionFunctionJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/ConversionFunctionJdbcTest.java)
- [EncryptionFunctionJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/EncryptionFunctionJdbcTest.java)
- [MiscFunctionJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/MiscFunctionJdbcTest.java)
- [ConnectionDatabaseMetaDataJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/ConnectionDatabaseMetaDataJdbcTest.java)
- [PreparedStatementJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/PreparedStatementJdbcTest.java)
- [CallableStatementJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/CallableStatementJdbcTest.java)
- [ResultSetJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/ResultSetJdbcTest.java)
- [StatementJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/jdbc/StatementJdbcTest.java)
- [OptimizerJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/optimizer/OptimizerJdbcTest.java)
- [StatisticsJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/optimizer/StatisticsJdbcTest.java)
- [AuditJdbcTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/utility/AuditJdbcTest.java)

Environment-limited suites:

- [IsqlSmokeTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/smoke/IsqlSmokeTest.java)
- [ProcessSmokeTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/smoke/ProcessSmokeTest.java)
- [AuditPropertyTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/utility/AuditPropertyTest.java)
- [ServerControlSmokeTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/recovery/ServerControlSmokeTest.java)
- [ReplicationPropertySmokeTest.java](/C:/Users/TTA/IdeaProjects/altibase-jdbc-automation/src/test/java/com/altibase/qa/replication/ReplicationPropertySmokeTest.java)

## Coverage

- Workbook direct mapping count: `754`
- Workbook total: `934`
- Coverage ratio: `80.7%`

The ratio above is based on direct `TC_xxx_xxx` matches between the workbook and automated test display names / source.

## Recent Expansion

- Added `TC_181_001` to `TC_205_001` conversion-function coverage except `TC_190_001`
- Added `TC_206_001` to `TC_207_004` for AES, DES, and 3DES encryption functions
- Added `TC_208_001` to `TC_247_001` for stable misc-function cases
- Added `TC_227_001` to `TC_230_001` and `TC_239_001` for message queue and datagram communication functions
- Added `TC_249_001` to `TC_269_001` for data type support
- Added `TC_270_001` to `TC_279_001` for stored procedure / function / trigger lifecycle
- Added `TC_281_001` to `TC_283_001` for directory objects
- Added `TC_284_001` for directory privilege provisioning
- Added `TC_285_001` to `TC_286_002` for directory-backed stored procedure scenarios
- Added `TC_047_001`, `TC_083_004`, and `TC_095_003` for temporary tablespace, direct-key index, and materialized-view tablespace coverage
- Added `TC_048_001` and `TC_054_001` for explicit owner-schema table creation and tablespace-targeted table creation
- Added `TC_084_001` to `TC_084_003`, `TC_086_001`, and `TC_098_003` for direct-key alteration, index rename, local-index, and sync-sequence coverage
- Added `TC_060_001` to `TC_072_001` except the TPC-H scale scenarios for partitioned table lifecycle coverage
- Added `TC_161_010`, `TC_161_011`, `TC_190_001`, `TC_234_001`, and `TC_235_001` for PKCS7, RAW, and quoted-printable conversion coverage
- Added `TC_292_001` to `TC_299_001` and `TC_302_001` for user property DDL
- Added `TC_305_002` and `TC_306_002` to `TC_306_007` for job lifecycle details
- Added `TC_316_001` to `TC_316_003` and `TC_331_001` to `TC_331_003` for tablespace privilege grant and revoke
- Added `TC_361_001` to `TC_362_001` for DB-side audit option lifecycle
- Added `TC_367_001` for SQL plan cache SQL-text and child PCO inspection
- Added `TC_370_001`, `TC_370_003`, and `TC_370_004` for statistics gathering workflows
- Added `TC_363_001` to `TC_364_002` for encrypted table storage and recovery scenarios

Failure-path design added alongside happy paths:

- invalid login and account lock behavior
- TCP-disabled connection failure
- old-password login failure after password change
- duplicate directory and duplicate job failures
- disabled trigger no-op behavior before enable
- read-only and stale materialized view checks
- invalid format rejection for `TO_DATE` and `TO_NUMBER`
- type length / precision enforcement

## Temporarily Skipped Role-Grant Scenarios

These four cases remain skipped in the current Windows + JDBC harness, but Linux-side server validation showed normal behavior, so they are not being treated as confirmed product defects:

- role-granted `CREATE PROCEDURE`
- role-granted `CREATE TRIGGER`
- role-granted `CREATE SYNONYM`
- role-granted `CREATE MATERIALIZED VIEW`

They remain separated from the stable passing count until the harness discrepancy is reconciled.
