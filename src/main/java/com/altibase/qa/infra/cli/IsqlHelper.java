package com.altibase.qa.infra.cli;

import com.altibase.qa.config.TestConfig;

import java.util.ArrayList;
import java.util.List;

public class IsqlHelper {
    private final TestConfig config;
    private final ShellHelper shellHelper;

    public IsqlHelper(TestConfig config, ShellHelper shellHelper) {
        this.config = config;
        this.shellHelper = shellHelper;
    }

    public CommandResult executeSql(String sql) {
        return shellHelper.execute(buildClientCommand(false), wrapSql(sql));
    }

    public CommandResult executeSqlAsSysdba(String sql) {
        return shellHelper.execute(buildServerCommand(true), wrapSql(sql));
    }

    public CommandResult startupControl() {
        return executeSqlAsSysdba("startup control;");
    }

    public CommandResult startupService() {
        return executeSqlAsSysdba("startup service;");
    }

    public CommandResult shutdownImmediate() {
        return executeSqlAsSysdba("shutdown immediate;");
    }

    private List<String> buildClientCommand(boolean sysdba) {
        List<String> command = new ArrayList<>();
        command.add(config.client().isql());
        command.add("-s");
        command.add(config.db().host());
        command.add("-u");
        command.add(config.db().user());
        command.add("-p");
        command.add(config.db().password());
        command.add("-port");
        command.add(String.valueOf(config.db().port()));
        if (sysdba) {
            command.add("-sysdba");
        }
        return command;
    }

    private List<String> buildServerCommand(boolean sysdba) {
        List<String> command = new ArrayList<>();
        command.add(config.server().isql());
        command.add("-s");
        command.add(config.db().host());
        command.add("-u");
        command.add(config.db().user());
        command.add("-p");
        command.add(config.db().password());
        command.add("-port");
        command.add(String.valueOf(config.db().port()));
        if (sysdba) {
            command.add("-sysdba");
        }
        return command;
    }

    private String wrapSql(String sql) {
        String normalized = sql.endsWith(";") ? sql : sql + ";";
        return normalized + System.lineSeparator() + "exit;" + System.lineSeparator();
    }
}
