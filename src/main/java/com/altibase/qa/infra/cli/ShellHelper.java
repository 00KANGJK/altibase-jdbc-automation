package com.altibase.qa.infra.cli;

import com.altibase.qa.config.TestConfig;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ShellHelper {
    private final TestConfig config;

    public ShellHelper(TestConfig config) {
        this.config = config;
    }

    public CommandResult execute(List<String> command) {
        return execute(command, null, Duration.ofSeconds(config.timeouts().commandSeconds()));
    }

    public CommandResult execute(List<String> command, String stdin) {
        return execute(command, stdin, Duration.ofSeconds(config.timeouts().commandSeconds()));
    }

    public CommandResult execute(List<String> command, String stdin, Duration timeout) {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        try {
            Process process = processBuilder.start();
            if (stdin != null) {
                try (Writer writer = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8)) {
                    writer.write(stdin);
                }
            }
            boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                process.destroyForcibly();
            }
            String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String stderr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            int exitCode = finished ? process.exitValue() : -1;
            return new CommandResult(exitCode, stdout, stderr, !finished);
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Command execution failed: " + String.join(" ", command), e);
        }
    }
}
