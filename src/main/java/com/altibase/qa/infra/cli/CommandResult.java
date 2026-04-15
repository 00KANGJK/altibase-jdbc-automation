package com.altibase.qa.infra.cli;

public record CommandResult(int exitCode, String stdout, String stderr, boolean timedOut) {
    public boolean isSuccess() {
        return exitCode == 0 && !timedOut;
    }
}
