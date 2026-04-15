package com.altibase.qa.config;

public record TestConfig(
        EnvConfig env,
        DbConfig db,
        ClientConfig client,
        ServerConfig server,
        PathsConfig paths,
        TimeoutConfig timeouts,
        ExecutionConfig execution
) {
    public record EnvConfig(String name) {}

    public record DbConfig(
            String host,
            int port,
            String database,
            String user,
            String password,
            String jdbcUrl,
            String driverClass
    ) {}

    public record ClientConfig(
            String altibaseHome,
            String isql,
            String iloader,
            String aexport,
            String jdbcJar
    ) {}

    public record ServerConfig(
            String altibaseHome,
            String isql,
            String server,
            String iloader,
            String aexport
    ) {}

    public record PathsConfig(
            String memDbDir,
            String diskDbDir,
            String logDir,
            String archiveDir,
            String auditDir,
            String workRoot,
            String backupDir,
            String datafileDir,
            String exportDir,
            String scriptDir,
            String logCaptureDir
    ) {}

    public record TimeoutConfig(
            int connectSeconds,
            int querySeconds,
            int commandSeconds
    ) {}

    public record ExecutionConfig(
            boolean enableDbTests,
            boolean enableCliTests,
            boolean enableDestructiveTests,
            boolean enableRecoveryTests,
            boolean enableReplicationTests
    ) {}
}
