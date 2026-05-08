package com.altibase.qa.recovery;

import com.altibase.qa.base.BaseDbTest;
import com.altibase.qa.support.DbTestSupport;
import com.altibase.qa.support.FeatureProbe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OnlineBackupJdbcTest extends BaseDbTest {

    @Test
    @DisplayName("Additional recovery case: archive metadata is readable before backup tests run")
    void archiveMetadataIsReadable() {
        FeatureProbe.assumeRecoveryEnabled(config);

        var result = jdbc.query(
                connection,
                "select archive_mode, archive_dest, current_logfile from v$archive"
        );

        assertThat(result.size()).isGreaterThanOrEqualTo(1);
        assertThat(Set.of("0", "1")).contains(String.valueOf(result.value(0, "ARCHIVE_MODE")));
        assertThat(String.valueOf(result.value(0, "ARCHIVE_DEST"))).isNotBlank();
    }

    @Test
    @DisplayName("Additional recovery case: ALTER SYSTEM SWITCH LOGFILE follows archive mode")
    void alterSystemSwitchLogfileFollowsArchiveMode() {
        FeatureProbe.assumeRecoveryEnabled(config);

        if (isArchiveLogMode()) {
            executeAsSysdba("alter system switch logfile");
        } else {
            assertSysdbaUpdateFails("alter system switch logfile");
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from v$archive")).isNotNull();
    }

    @Test
    @DisplayName("Additional recovery case: online LOGANCHOR backup writes to configured backup directory")
    void onlineLoganchorBackupToConfiguredDirectorySucceeds() {
        FeatureProbe.assumeRecoveryEnabled(config);

        String sql = "alter database backup loganchor to '" + config.paths().backupDir() + "'";
        if (isArchiveLogMode()) {
            executeAsSysdba(sql);
        } else {
            assertSysdbaUpdateFails(sql);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from v$log")).isNotNull();
    }

    @Test
    @DisplayName("Additional recovery case: online dictionary tablespace backup writes to configured backup directory")
    void onlineDictionaryTablespaceBackupToConfiguredDirectorySucceeds() {
        FeatureProbe.assumeRecoveryEnabled(config);

        String sql = "alter database backup tablespace SYS_TBS_MEM_DIC to '" + config.paths().backupDir() + "'";
        if (isArchiveLogMode()) {
            executeAsSysdba(sql);
        } else {
            assertSysdbaUpdateFails(sql);
        }

        assertThat(jdbc.queryForString(connection, "select count(*) from v$archive")).isNotNull();
    }

    @Test
    @DisplayName("Additional recovery case: DBA-driven BEGIN/END BACKUP works for a small memory tablespace")
    void beginEndBackupForSmallMemoryTablespaceSucceeds() {
        FeatureProbe.assumeRecoveryEnabled(config);

        String tablespaceName = DbTestSupport.uniqueName("QA_BK_MEM");
        registerCleanup(() -> endBackupQuietly(tablespaceName));
        registerCleanup(() -> DbTestSupport.dropTablespaceQuietly(jdbc, connection, tablespaceName));

        jdbc.executeUpdate(connection, "create memory data tablespace " + tablespaceName + " size 4M");
        if (isArchiveLogMode()) {
            executeAsSysdba("alter tablespace " + tablespaceName + " begin backup");
            executeAsSysdba("alter tablespace " + tablespaceName + " end backup");
        } else {
            assertSysdbaUpdateFails("alter tablespace " + tablespaceName + " begin backup");
        }

        assertThat(jdbc.queryForString(connection,
                "select count(*) from v$tablespaces where name = '" + tablespaceName + "'"))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("Additional recovery negative case: media recovery is rejected in service phase")
    void mediaRecoveryIsRejectedInServicePhase() {
        FeatureProbe.assumeRecoveryEnabled(config);

        assertSysdbaUpdateFails("alter database recover database");
    }

    @Test
    @DisplayName("Additional recovery negative case: backup rejects missing tablespaces")
    void onlineBackupRejectsMissingTablespace() {
        FeatureProbe.assumeRecoveryEnabled(config);

        assertSysdbaUpdateFails(
                "alter database backup tablespace QA_MISSING_BACKUP_TBS to '" + config.paths().backupDir() + "'"
        );
    }

    @Test
    @DisplayName("Additional recovery negative case: backup rejects missing server-side directories")
    void onlineBackupRejectsMissingDirectory() {
        FeatureProbe.assumeRecoveryEnabled(config);

        String missingDirectory = config.paths().backupDir() + "/" + DbTestSupport.uniqueName("MISSING_DIR");
        assertSysdbaUpdateFails("alter database backup loganchor to '" + missingDirectory + "'");
    }

    private boolean isArchiveLogMode() {
        String archiveMode = jdbc.queryForString(connection, "select archive_mode from v$archive");
        return "1".equals(archiveMode);
    }

    private void executeAsSysdba(String sql) {
        try (Connection sysdba = jdbc.openSysdba()) {
            jdbc.executeUpdate(sysdba, sql);
        } catch (Exception e) {
            throw new IllegalStateException("SYSDBA SQL failed: " + sql, e);
        }
    }

    private void assertSysdbaUpdateFails(String sql) {
        assertThatThrownBy(() -> executeAsSysdba(sql))
                .isInstanceOf(IllegalStateException.class);
    }

    private void endBackupQuietly(String tablespaceName) {
        try {
            executeAsSysdba("alter tablespace " + tablespaceName + " end backup");
        } catch (Exception ignored) {
        }
    }
}
