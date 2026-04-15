package com.altibase.qa.infra.jdbc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public record QueryResult(List<String> columns, List<Map<String, Object>> rows) {
    public QueryResult {
        columns = Collections.unmodifiableList(columns);
        rows = Collections.unmodifiableList(rows);
    }

    public int size() {
        return rows.size();
    }

    public Object value(int rowIndex, String columnName) {
        return rows.get(rowIndex).get(columnName.toUpperCase());
    }
}
