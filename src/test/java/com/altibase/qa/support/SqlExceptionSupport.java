package com.altibase.qa.support;

import java.sql.SQLException;

public final class SqlExceptionSupport {
    private SqlExceptionSupport() {
    }

    public static SQLException requireSqlException(Throwable throwable) {
        SQLException sqlException = findSqlException(throwable);
        if (sqlException == null) {
            throw new IllegalStateException("No SQLException found in the cause chain", throwable);
        }
        return sqlException;
    }

    public static SQLException findSqlException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException sqlException) {
                return sqlException;
            }
            current = current.getCause();
        }
        return null;
    }
}
