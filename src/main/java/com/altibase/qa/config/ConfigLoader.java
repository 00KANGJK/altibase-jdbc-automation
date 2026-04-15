package com.altibase.qa.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class ConfigLoader {
    private static final String DEFAULT_CONFIG = "config/application-test.yml";
    private static final String LOCAL_OVERRIDE = "config/application-local.yml";

    private ConfigLoader() {
    }

    public static TestConfig load() {
        String configPath = System.getProperty("altibase.test.config", DEFAULT_CONFIG);
        Map<String, Object> merged = new LinkedHashMap<>();
        merge(merged, loadMap(configPath));
        merge(merged, loadMap(LOCAL_OVERRIDE));
        applyEnvOverrides(merged);
        return toConfig(merged);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadMap(String resourcePath) {
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (input == null) {
                return new LinkedHashMap<>();
            }
            Object loaded = new Yaml().load(input);
            if (loaded instanceof Map<?, ?> map) {
                return (Map<String, Object>) map;
            }
            return new LinkedHashMap<>();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load config resource: " + resourcePath, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void merge(Map<String, Object> target, Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            Object existing = target.get(entry.getKey());
            Object incoming = entry.getValue();
            if (existing instanceof Map<?, ?> existingMap && incoming instanceof Map<?, ?> incomingMap) {
                Map<String, Object> nested = new LinkedHashMap<>((Map<String, Object>) existingMap);
                merge(nested, (Map<String, Object>) incomingMap);
                target.put(entry.getKey(), nested);
            } else {
                target.put(entry.getKey(), incoming);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void applyEnvOverrides(Map<String, Object> root) {
        setIfPresent(root, "db", "host", "ALTIBASE_TEST_DB_HOST");
        setIfPresent(root, "db", "port", "ALTIBASE_TEST_DB_PORT");
        setIfPresent(root, "db", "database", "ALTIBASE_TEST_DB_NAME");
        setIfPresent(root, "db", "user", "ALTIBASE_TEST_DB_USER");
        setIfPresent(root, "db", "password", "ALTIBASE_TEST_DB_PASSWORD");
        setIfPresent(root, "db", "jdbcUrl", "ALTIBASE_TEST_JDBC_URL");
        setIfPresent(root, "client", "isql", "ALTIBASE_TEST_CLIENT_ISQL");
        setIfPresent(root, "server", "isql", "ALTIBASE_TEST_SERVER_ISQL");
        setIfPresent(root, "server", "server", "ALTIBASE_TEST_SERVER_BIN");

        Map<String, Object> execution = map(root, "execution");
        String dbTests = System.getenv("ALTIBASE_ENABLE_DB_TESTS");
        if (dbTests != null && !dbTests.isBlank()) {
            execution.put("enableDbTests", Boolean.parseBoolean(dbTests));
        }
        String cliTests = System.getenv("ALTIBASE_ENABLE_CLI_TESTS");
        if (cliTests != null && !cliTests.isBlank()) {
            execution.put("enableCliTests", Boolean.parseBoolean(cliTests));
        }
        String destructive = System.getenv("ALTIBASE_ENABLE_DESTRUCTIVE_TESTS");
        if (destructive != null && !destructive.isBlank()) {
            execution.put("enableDestructiveTests", Boolean.parseBoolean(destructive));
        }
    }

    private static void setIfPresent(Map<String, Object> root, String section, String key, String envName) {
        String value = System.getenv(envName);
        if (value == null || value.isBlank()) {
            return;
        }
        map(root, section).put(key, value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Map<String, Object> root, String key) {
        return (Map<String, Object>) root.computeIfAbsent(key, ignored -> new LinkedHashMap<>());
    }

    private static TestConfig toConfig(Map<String, Object> root) {
        Map<String, Object> env = map(root, "env");
        Map<String, Object> db = map(root, "db");
        Map<String, Object> client = map(root, "client");
        Map<String, Object> server = map(root, "server");
        Map<String, Object> paths = map(root, "paths");
        Map<String, Object> timeouts = map(root, "timeouts");
        Map<String, Object> execution = map(root, "execution");

        return new TestConfig(
                new TestConfig.EnvConfig(string(env, "name", "local")),
                new TestConfig.DbConfig(
                        required(db, "host"),
                        integer(db, "port", 20300),
                        string(db, "database", ""),
                        required(db, "user"),
                        required(db, "password"),
                        required(db, "jdbcUrl"),
                        string(db, "driverClass", "Altibase.jdbc.driver.AltibaseDriver")
                ),
                new TestConfig.ClientConfig(
                        required(client, "altibaseHome"),
                        required(client, "isql"),
                        string(client, "iloader", ""),
                        string(client, "aexport", ""),
                        string(client, "jdbcJar", "")
                ),
                new TestConfig.ServerConfig(
                        required(server, "altibaseHome"),
                        required(server, "isql"),
                        required(server, "server"),
                        string(server, "iloader", ""),
                        string(server, "aexport", "")
                ),
                new TestConfig.PathsConfig(
                        string(paths, "memDbDir", ""),
                        string(paths, "diskDbDir", ""),
                        string(paths, "logDir", ""),
                        string(paths, "archiveDir", ""),
                        string(paths, "auditDir", ""),
                        required(paths, "workRoot"),
                        required(paths, "backupDir"),
                        required(paths, "datafileDir"),
                        required(paths, "exportDir"),
                        required(paths, "scriptDir"),
                        required(paths, "logCaptureDir")
                ),
                new TestConfig.TimeoutConfig(
                        integer(timeouts, "connectSeconds", 10),
                        integer(timeouts, "querySeconds", 60),
                        integer(timeouts, "commandSeconds", 300)
                ),
                new TestConfig.ExecutionConfig(
                        bool(execution, "enableDbTests", false),
                        bool(execution, "enableCliTests", false),
                        bool(execution, "enableDestructiveTests", false),
                        bool(execution, "enableRecoveryTests", false),
                        bool(execution, "enableReplicationTests", false)
                )
        );
    }

    private static String required(Map<String, Object> map, String key) {
        String value = string(map, key, "");
        if (value.isBlank()) {
            throw new IllegalStateException("Missing required config value: " + key);
        }
        return value;
    }

    private static String string(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value == null ? defaultValue : Objects.toString(value, defaultValue);
    }

    private static int integer(Map<String, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private static boolean bool(Map<String, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        return Boolean.parseBoolean(value.toString());
    }
}
