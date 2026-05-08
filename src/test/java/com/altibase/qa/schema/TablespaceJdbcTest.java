package com.altibase.qa.schema;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TablespaceJdbcTest extends BaseDbTest {

    private boolean tablespaceExists(String name) {
        return jdbc.exists(connection,
                "select name from v$tablespaces where name = '" + name.toUpperCase() + "'");
    }

    private String normalizeDirectoryPath(String path) {
        return path == null ? null : path.trim().replace('\\', '/').replaceAll("/+$", "");
    }

    private String datafilePath(String fileName) {
        String dbDir = jdbc.queryForString(connection,
                "select value1 from v$property where name = 'DEFAULT_DISK_DB_DIR'");
        if (dbDir == null || dbDir.isBlank()) {
            dbDir = "/tmp";
        }
        return dbDir.trim().replaceAll("/$", "") + "/" + fileName;
    }

    // ========================================================================
    // Disk Tablespace
    // ========================================================================
    @Nested
    @DisplayName("Disk Tablespace")
    class DiskTablespace {

        @Test
        @DisplayName("TC_001_001 디스크 테이블스페이스 데이터 파일의 경로 및 이름을 지정할 수 있는지 확인")
        void tc001001CreateDiskTablespaceWithDatafile() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_TBS");
            String df = datafilePath("qa_disk_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "'");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_001_002 디스크 테이블스페이스 데이터 파일을 다수 생성할 수 있는지 확인")
        void tc001002CreateDiskTablespaceWithMultipleDatafiles() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_MULTI");
            String df1 = datafilePath("qa_dm1_" + tbs.toLowerCase() + ".dbf");
            String df2 = datafilePath("qa_dm2_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df1 + "', '" + df2 + "'");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_001_003 디스크 테이블스페이스 데이터 파일의 크기를 지정할 수 있는지 확인")
        void tc001003CreateDiskTablespaceWithSize() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_SIZE");
            String df = datafilePath("qa_ds_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 2M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_002_001 디스크 테이블스페이스의 데이터 파일 자동 확장여부를 설정할 수 있는지 확인")
        void tc002001CreateDiskTablespaceAutoextendOn() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_AE");
            String df = datafilePath("qa_dae_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 2M autoextend on");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_002_002 디스크 테이블스페이스가 자동 확장 될 때 증가하는 크기를 설정할 수 있는지 확인")
        void tc002002CreateDiskTablespaceAutoextendNext() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_AEN");
            String df = datafilePath("qa_daen_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 2M autoextend on next 1M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_003_001 디스크 테이블스페이스 데이터 파일이 확장될 수 있는 최대 크기를 설정할 수 있는지 확인")
        void tc003001CreateDiskTablespaceMaxsize() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_MAX");
            String df = datafilePath("qa_dmax_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 2M autoextend on next 1M maxsize 10M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_004_001 디스크 테이블스페이스에 익스텐트의 사이즈를 정의할 수 있는지 확인")
        void tc004001CreateDiskTablespaceExtentsize() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_EXT");
            String df = datafilePath("qa_dext_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' extentsize 100K");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_005_001 디스크 테이블스페이스의 데이터 파일을 추가할 수 있는지 확인")
        void tc005001AlterDiskTablespaceAddDatafile() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_ADD");
            String df1 = datafilePath("qa_dadd1_" + tbs.toLowerCase() + ".dbf");
            String df2 = datafilePath("qa_dadd2_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df1 + "'");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " add datafile '" + df2 + "'");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_006_001 디스크 테이블스페이스의 데이터 파일 크기를 변경할 수 있는지 확인")
        void tc006001AlterDiskTablespaceResizeDatafile() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_RSZ");
            String df = datafilePath("qa_drsz_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 100M");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " alter datafile '" + df + "' size 1M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_007_001 디스크 테이블스페이스의 데이터 파일을 삭제할 수 있는지 확인")
        void tc007001AlterDiskTablespaceDropDatafile() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_DDF");
            String df1 = datafilePath("qa_ddf1_" + tbs.toLowerCase() + ".dbf");
            String df2 = datafilePath("qa_ddf2_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df1 + "', '" + df2 + "'");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " drop datafile '" + df2 + "'");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_008_001 디스크 테이블스페이스의 데이터 파일 이름을 변경할 수 있는지 확인")
        @Disabled("Requires server-side datafile rename preparation on the DB host; not supportable in the JDBC-only harness")
        void tc008001AlterDiskTablespaceRenameDatafile() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_REN");
            String df1 = datafilePath("qa_dren1_" + tbs.toLowerCase() + ".dbf");
            String df2 = datafilePath("qa_dren2_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> {
                try { jdbc.executeUpdate(connection, "alter tablespace " + tbs + " online"); } catch (Exception ignored) {}
                DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs);
            });

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df1 + "'");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " offline");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " rename datafile '" + df1 + "' to '" + df2 + "'");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " online");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_009_001 디스크 테이블스페이스를 제거할 수 있는지 확인")
        void tc009001DropDiskTablespace() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_DRP");
            String df = datafilePath("qa_ddrp_" + tbs.toLowerCase() + ".dbf");

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "'");
            assertThat(tablespaceExists(tbs)).isTrue();

            jdbc.executeUpdate(connection, "drop tablespace " + tbs);
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_009_002 디스크 테이블스페이스에 데이터가 있을 때 기본 요소만으로 삭제되지 않게 막는지 확인")
        void tc009002DropDiskTablespaceWithDataFails() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_DRPF");
            String df = datafilePath("qa_ddrpf_" + tbs.toLowerCase() + ".dbf");
            String table = DbTestSupport.uniqueName("QA_DTBL");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 32M");
            jdbc.executeUpdate(connection,
                    "create table " + table + "(c1 int) tablespace " + tbs);

            assertThatThrownBy(() -> jdbc.executeUpdate(connection, "drop tablespace " + tbs))
                    .isInstanceOf(Exception.class);

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_010_001 디스크 테이블스페이스 삭제 시 테이블스페이스 내 모든 객체가 삭제되도록할 수 있는지 확인")
        void tc010001DropDiskTablespaceIncludingContents() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_DIC");
            String df = datafilePath("qa_ddic_" + tbs.toLowerCase() + ".dbf");
            String table = DbTestSupport.uniqueName("QA_DTBLI");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 32M");
            jdbc.executeUpdate(connection,
                    "create table " + table + "(c1 int) tablespace " + tbs);

            jdbc.executeUpdate(connection, "drop tablespace " + tbs + " including contents");
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_011_001 디스크 테이블스페이스 삭제 시 참조하는 참조 제약을 제거할 수 있는지 확인")
        void tc011001DropDiskTablespaceCascadeConstraints() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_DCC");
            String df = datafilePath("qa_ddcc_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 32M");

            jdbc.executeUpdate(connection, "drop tablespace " + tbs + " including contents cascade constraints");
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_012_001 디스크 테이블스페이스 삭제 시 데이터 파일을 같이 삭제할 수 있는지 확인")
        void tc012001DropDiskTablespaceAndDatafiles() {
            String tbs = DbTestSupport.uniqueName("QA_DISK_DAD");
            String df = datafilePath("qa_ddad_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create disk data tablespace " + tbs + " datafile '" + df + "' size 2M");

            jdbc.executeUpdate(connection, "drop tablespace " + tbs + " including contents and datafiles");
            assertThat(tablespaceExists(tbs)).isFalse();
        }
    }

    // ========================================================================
    // Memory Tablespace
    // ========================================================================
    @Nested
    @DisplayName("Memory Tablespace")
    class MemoryTablespace {

        @Test
        @DisplayName("TC_013_001 메모리 테이블스페이스의 초기 크기를 설정할 수 있는지 확인")
        void tc013001CreateMemoryTablespaceWithSize() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_SIZE");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 8M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_014_001 메모리 테이블스페이스의 자동확장모드를 설정할 수 있는지 확인")
        void tc014001CreateMemoryTablespaceAutoextend() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_AE");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M autoextend on");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_015_001 메모리 테이블스페이스의 최대 크기를 설정할 수 있는지 확인")
        void tc015001CreateMemoryTablespaceMaxsize() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_MAX");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create memory data tablespace " + tbs + " size 4M autoextend on maxsize 8M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_016_001 메모리 테이블스페이스의 체크포인트 경로를 설정할 수 있는지 확인")
        void tc016001CreateMemoryTablespaceCheckpointPath() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_CP");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            String dbDir = jdbc.queryForString(connection,
                    "select value1 from v$property where name = 'MEM_DB_DIR'");
            if (dbDir == null || dbDir.isBlank()) {
                dbDir = "/tmp";
            }

            jdbc.executeUpdate(connection,
                    "create memory data tablespace " + tbs + " size 4M checkpoint path '" + dbDir.trim() + "'");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_017_001 메모리 테이블스페이스 체크포인트 이미지 파일의 분할 크기를 설정할 수 있는지 확인")
        void tc017001LegacyDisplayNamePlaceholder() {
            tc017001CreateMemoryTablespaceSplitEach();
        }

        @Test
        @DisplayName("Additional monitoring case: memory tablespace checkpoint path is visible in V$MEM_TABLESPACE_CHECKPOINT_PATHS")
        void memoryTablespaceCheckpointPathIsVisibleInMetadata() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_CP_META");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            String dbDir = jdbc.queryForString(connection,
                    "select value1 from v$property where name = 'MEM_DB_DIR'");
            if (dbDir == null || dbDir.isBlank()) {
                dbDir = "/tmp";
            }

            jdbc.executeUpdate(connection,
                    "create memory data tablespace " + tbs + " size 4M checkpoint path '" + dbDir.trim() + "'");

            String checkpointPath = jdbc.queryForString(
                    connection,
                    "select c.checkpoint_path " +
                            "from v$mem_tablespaces m, v$mem_tablespace_checkpoint_paths c " +
                            "where m.space_id = c.space_id and m.space_name = '" + tbs + "'"
            );

            assertThat(normalizeDirectoryPath(checkpointPath)).isEqualTo(normalizeDirectoryPath(dbDir));
        }

        @Test
        @DisplayName("TC_017_001  메모리 테이블스페이스 체크포인트 이미지 파일의 분할 크기를 설정할 수 있는지 확인")
        void tc017001CreateMemoryTablespaceSplitEach() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_SPLIT");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            String dbDir = jdbc.queryForString(connection,
                    "select value1 from v$property where name = 'MEM_DB_DIR'");
            if (dbDir == null || dbDir.isBlank()) {
                dbDir = "/tmp";
            }

            jdbc.executeUpdate(connection,
                    "create memory data tablespace " + tbs + " size 4M checkpoint path '" + dbDir.trim() + "' split each 4096K");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_019_001 메모리 테이블스페이스의 자동확장 여부를 변경할 수 있는지 확인")
        void tc019001AlterMemoryTablespaceAutoextend() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_ALT");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " alter autoextend on");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " alter autoextend off");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_019_002 ALTER MEMORY TABLESPACE turns AUTOEXTEND off")
        void tc019002AlterMemoryTablespaceAutoextendOff() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_AOFF");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M autoextend on");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " alter autoextend off");

            var result = jdbc.query(
                    connection,
                    "select autoextend_mode from v$mem_tablespaces where space_name = '" + tbs + "'"
            );

            assertThat(result.size()).isEqualTo(1);
            assertThat(Integer.parseInt(String.valueOf(result.value(0, "AUTOEXTEND_MODE")))).isEqualTo(0);
        }

        @Test
        @DisplayName("TC_019_003 메모리 테이블스페이스의 자동확장 NEXT/MAXSIZE를 변경할 수 있는지 확인")
        void tc019003AlterMemoryTablespaceAutoextendNextMaxsize() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_ANM");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " alter autoextend on next 4M maxsize 20M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_023_001 메모리 테이블스페이스를 오프라인 상태로 변경할 수 있는지 확인")
        void tc023001LegacyDisplayNamePlaceholder() {
            tc023002AlterMemoryTablespaceOnline();
        }

        @Test
        @DisplayName("Additional monitoring case: V$MEM_TABLESPACES exposes memory tablespace autoextend metadata")
        void memoryTablespaceAutoextendMetadataIsVisible() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_META");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create memory data tablespace " + tbs + " size 4M autoextend on maxsize 8M");

            var result = jdbc.query(
                    connection,
                    "select autoextend_mode, maxsize from v$mem_tablespaces where space_name = '" + tbs + "'"
            );

            assertThat(result.size()).isEqualTo(1);
            assertThat(Integer.parseInt(String.valueOf(result.value(0, "AUTOEXTEND_MODE")))).isEqualTo(1);
            assertThat(Long.parseLong(String.valueOf(result.value(0, "MAXSIZE")))).isGreaterThan(0L);
        }

        @Test
        @DisplayName("TC_023_001 硫붾え由??뚯씠釉붿뒪?섏씠?ㅻ? ?ㅽ봽?쇱씤 ?곹깭濡?蹂寃쏀븷 ???덈뒗吏 ?뺤씤")
        void tc023001AlterMemoryTablespaceOffline() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_OFL");
            registerCleanup(() -> {
                try { jdbc.executeUpdate(connection, "alter tablespace " + tbs + " online"); } catch (Exception ignored) {}
                DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs);
            });

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " offline");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_023_002 메모리 테이블스페이스를 온라인 상태로 변경할 수 있는지 확인")
        void tc023002AlterMemoryTablespaceOnline() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_ONL");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " offline");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " online");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_025_001 메모리 테이블스페이스를 제거할 수 있는지 확인")
        void tc025001DropMemoryTablespace() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_DRP");

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");
            assertThat(tablespaceExists(tbs)).isTrue();

            jdbc.executeUpdate(connection, "drop tablespace " + tbs);
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_026_001 메모리 테이블스페이스 삭제 시 모든 객체가 삭제되도록할 수 있는지 확인")
        void tc026001DropMemoryTablespaceIncludingContents() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_DIC");
            String table = DbTestSupport.uniqueName("QA_MTBLI");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection, "create table " + table + "(c1 int) tablespace " + tbs);

            jdbc.executeUpdate(connection, "drop tablespace " + tbs + " including contents");
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_027_001 메모리 테이블스페이스 삭제 시 참조 제약을 제거할 수 있는지 확인")
        void tc027001DropMemoryTablespaceCascadeConstraints() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_DCC");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");

            jdbc.executeUpdate(connection,
                    "drop tablespace " + tbs + " including contents cascade constraints");
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_028_001 메모리 테이블스페이스 삭제 시 체크포인트 파일을 같이 삭제할 수 있는지 확인")
        void tc028001DropMemoryTablespaceAndDatafiles() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_DAD");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");

            jdbc.executeUpdate(connection, "drop tablespace " + tbs + " including contents and datafiles");
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_018_001 create memory tablespace in OFFLINE state")
        void tc018001CreateMemoryTablespaceOffline() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_CREATE_OFL");
            registerCleanup(() -> {
                try { jdbc.executeUpdate(connection, "alter tablespace " + tbs + " online"); } catch (Exception ignored) {}
                DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs);
            });

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M offline");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @Disabled("ALTER TABLESPACE DISCARD is only executable in the required server start-up phase, not in the JDBC service-phase harness")
        @DisplayName("TC_024_001 alter memory tablespace to DISCARD state")
        void tc024001AlterMemoryTablespaceDiscard() {
            String tbs = DbTestSupport.uniqueName("QA_MEM_DISCARD");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create memory data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " discard");

            assertThat(tablespaceExists(tbs)).isTrue();
        }
    }

    // ========================================================================
    // Volatile Tablespace
    // ========================================================================
    @Nested
    @DisplayName("Volatile Tablespace")
    class VolatileTablespace {

        @Test
        @DisplayName("TC_029_001 휘발성 테이블스페이스의 초기 크기를 설정할 수 있는지 확인")
        void tc029001CreateVolatileTablespaceWithSize() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_SIZE");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create volatile data tablespace " + tbs + " size 4M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_030_001 휘발성 테이블스페이스의 자동확장모드를 설정할 수 있는지 확인")
        void tc030001CreateVolatileTablespaceAutoextend() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_AE");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create volatile data tablespace " + tbs + " size 4M autoextend on");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_031_001 휘발성 테이블스페이스의 최대 크기를 설정할 수 있는지 확인")
        void tc031001CreateVolatileTablespaceMaxsize() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_MAX");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create volatile data tablespace " + tbs + " size 4M autoextend on next 4M maxsize 12M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_032_001 휘발성 테이블스페이스의 자동확장모드를 변경할 수 있는지 확인")
        void tc032001LegacyDisplayNamePlaceholder() {
            tc032001AlterVolatileTablespaceAutoextend();
        }

        @Test
        @DisplayName("Additional monitoring case: user volatile tablespace is visible in V$VOL_TABLESPACES")
        void volatileTablespaceIsVisibleInMetadata() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_META");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create volatile data tablespace " + tbs + " size 4M autoextend on maxsize 8M");

            var result = jdbc.query(
                    connection,
                    "select space_name, autoextend_mode from v$vol_tablespaces where space_name = '" + tbs + "'"
            );

            assertThat(result.size()).isEqualTo(1);
            assertThat(String.valueOf(result.value(0, "SPACE_NAME"))).isEqualTo(tbs);
            assertThat(Integer.parseInt(String.valueOf(result.value(0, "AUTOEXTEND_MODE")))).isEqualTo(1);
        }

        @Test
        @DisplayName("TC_032_001 ?섎컻???뚯씠釉붿뒪?섏씠?ㅼ쓽 ?먮룞?뺤옣紐⑤뱶瑜?蹂寃쏀븷 ???덈뒗吏 ?뺤씤")
        void tc032001AlterVolatileTablespaceAutoextend() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_ALT");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create volatile data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " alter autoextend on");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_033_001 휘발성 테이블스페이스의 최대 크기를 변경할 수 있는지 확인")
        void tc033001AlterVolatileTablespaceMaxsize() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_MXA");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create volatile data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " alter autoextend on maxsize 8M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_034_001 휘발성 테이블스페이스를 제거할 수 있는지 확인")
        void tc034001DropVolatileTablespace() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_DRP");

            jdbc.executeUpdate(connection, "create volatile data tablespace " + tbs + " size 4M");
            assertThat(tablespaceExists(tbs)).isTrue();

            jdbc.executeUpdate(connection, "drop tablespace " + tbs);
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_035_001 휘발성 테이블스페이스 삭제 시 모든 객체가 삭제되도록할 수 있는지 확인")
        void tc035001DropVolatileTablespaceIncludingContents() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_DIC");
            String table = DbTestSupport.uniqueName("QA_VTBLI");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create volatile data tablespace " + tbs + " size 4M");
            jdbc.executeUpdate(connection,
                    "create table " + table + "(c1 int) tablespace " + tbs);

            jdbc.executeUpdate(connection, "drop tablespace " + tbs + " including contents");
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_036_001 휘발성 테이블스페이스 삭제 시 참조 제약을 제거할 수 있는지 확인")
        void tc036001DropVolatileTablespaceCascadeConstraints() {
            String tbs = DbTestSupport.uniqueName("QA_VOL_DCC");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection, "create volatile data tablespace " + tbs + " size 4M");

            jdbc.executeUpdate(connection,
                    "drop tablespace " + tbs + " including contents cascade constraints");
            assertThat(tablespaceExists(tbs)).isFalse();
        }
    }

    // ========================================================================
    // Temporary Tablespace
    // ========================================================================
    @Nested
    @DisplayName("Temporary Tablespace")
    class TemporaryTablespaceTests {

        @Test
        @DisplayName("TC_037_001 임시 테이블스페이스를 구성하는 데이터파일을 설정할 수 있는지 확인")
        void tc037001CreateTempTablespaceWithTempfile() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_TF");
            String tf = datafilePath("qa_ttf_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf + "'");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_038_001 임시 테이블스페이스의 자동확장을 설정할 수 있는지 확인")
        void tc038001CreateTempTablespaceAutoextend() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_AE");
            String tf = datafilePath("qa_tae_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf + "' autoextend on");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_039_001 임시 테이블스페이스가 자동확장 시 최대 크기를 설정할 수 있는지 확인")
        void tc039001CreateTempTablespaceMaxsize() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_MAX");
            String tf = datafilePath("qa_tmax_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf + "' autoextend on next 4M maxsize 400M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_040_001 임시 테이블스페이스에 임시 파일을 추가할 수 있는지 확인")
        void tc040001AlterTempTablespaceAddTempfile() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_ADD");
            String tf1 = datafilePath("qa_tadd1_" + tbs.toLowerCase() + ".dbf");
            String tf2 = datafilePath("qa_tadd2_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf1 + "'");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " add tempfile '" + tf2 + "'");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_041_001 임시 테이블스페이스에 임시 파일을 삭제할 수 있는지 확인")
        void tc041001AlterTempTablespaceDropTempfile() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_DRF");
            String tf1 = datafilePath("qa_tdrf1_" + tbs.toLowerCase() + ".dbf");
            String tf2 = datafilePath("qa_tdrf2_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf1 + "', '" + tf2 + "'");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " drop tempfile '" + tf2 + "'");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_042_001 임시 테이블스페이스의 임시 파일 크기를 변경할 수 있는지 확인")
        void tc042001AlterTempTablespaceResizeTempfile() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_RSZ");
            String tf = datafilePath("qa_trsz_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf + "'");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " alter tempfile '" + tf + "' size 20M");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_043_001 임시 테이블스페이스의 임시 파일 이름을 변경할 수 있는지 확인")
        @Disabled("Temporary tablespace rename needs non-service/server-side file handling that the JDBC-only harness cannot provide")
        void tc043001AlterTempTablespaceRenameTempfile() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_REN");
            String tf1 = datafilePath("qa_tren1_" + tbs.toLowerCase() + ".dbf");
            String tf2 = datafilePath("qa_tren2_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> {
                try { jdbc.executeUpdate(connection, "alter tablespace " + tbs + " online"); } catch (Exception ignored) {}
                DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs);
            });

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf1 + "'");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " offline");
            jdbc.executeUpdate(connection,
                    "alter tablespace " + tbs + " rename tempfile '" + tf1 + "' to '" + tf2 + "'");
            jdbc.executeUpdate(connection, "alter tablespace " + tbs + " online");

            assertThat(tablespaceExists(tbs)).isTrue();
        }

        @Test
        @DisplayName("TC_044_001 임시 테이블스페이스 삭제 시 모든 객체가 삭제되도록할 수 있는지 확인")
        void tc044001DropTempTablespaceIncludingContents() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_DIC");
            String tf = datafilePath("qa_tdic_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf + "'");

            jdbc.executeUpdate(connection, "drop tablespace " + tbs + " including contents");
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_045_001 임시 테이블스페이스 삭제 시 참조 제약을 제거할 수 있는지 확인")
        void tc045001DropTempTablespaceCascadeConstraints() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_DCC");
            String tf = datafilePath("qa_tdcc_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf + "'");

            jdbc.executeUpdate(connection,
                    "drop tablespace " + tbs + " including contents cascade constraints");
            assertThat(tablespaceExists(tbs)).isFalse();
        }

        @Test
        @DisplayName("TC_046_001 임시 테이블스페이스 삭제 시 임시 파일을 같이 삭제할 수 있는지 확인")
        void tc046001DropTempTablespaceAndDatafiles() {
            String tbs = DbTestSupport.uniqueName("QA_TMP_DAD");
            String tf = datafilePath("qa_tdad_" + tbs.toLowerCase() + ".dbf");
            registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tbs));

            jdbc.executeUpdate(connection,
                    "create temporary tablespace " + tbs + " tempfile '" + tf + "'");

            jdbc.executeUpdate(connection,
                    "drop tablespace " + tbs + " including contents and datafiles");
            assertThat(tablespaceExists(tbs)).isFalse();
        }
    }
}
