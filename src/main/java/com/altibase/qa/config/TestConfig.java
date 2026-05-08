package com.altibase.qa.config;

public record TestConfig(
        EnvConfig env,
        DbConfig db,
        ClientConfig client,
        ServerConfig server,
        PathsConfig paths,
        FeaturesConfig features,
        DatabaseLinkConfig databaseLink,
        ReplicationConfig replication,
        NetworkConfig network,
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

    public record FeaturesConfig(
            boolean directoryFileIo,
            boolean databaseLink,
            boolean storedPackages,
            boolean utlTcp,
            boolean utlSmtp,
            boolean queue,
            boolean spatial,
            boolean replication,
            boolean backupRecovery,
            boolean serverLifecycle,
            boolean cliUtilities
    ) {}

    public record DatabaseLinkConfig(
            String targetName,
            String remoteUser,
            String remotePassword
    ) {}

    public record ReplicationConfig(
            String remoteHost,
            int remotePort
    ) {}

    public record NetworkConfig(
            String tcpHost,
            int tcpPort,
            String smtpHost,
            int smtpPort
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
