package io.elastic.jdbc.utils;

import io.elastic.jdbc.query_builders.MSSQLOld;
import io.elastic.jdbc.query_builders.MySQLOld;
import io.elastic.jdbc.query_builders.OracleOld;
import io.elastic.jdbc.query_builders.QueryOld;

public class TriggerQueryFactory {
    public QueryOld getQuery(String dbEngine) {
        if (dbEngine.toLowerCase().equals("oracle")) {
            return new OracleOld();
        }
        if (dbEngine.toLowerCase().equals("mssql")) {
            return new MSSQLOld();
        }
        return new MySQLOld();
    }
}
