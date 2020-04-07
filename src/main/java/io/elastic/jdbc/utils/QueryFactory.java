package io.elastic.jdbc.utils;

import io.elastic.jdbc.query_builders.MSSQL;
import io.elastic.jdbc.query_builders.MySQL;
import io.elastic.jdbc.query_builders.Firebird;
import io.elastic.jdbc.query_builders.Oracle;
import io.elastic.jdbc.query_builders.PostgreSQL;
import io.elastic.jdbc.query_builders.Query;

public class QueryFactory {

  public Query getQuery(String dbEngine) {
    if (dbEngine.toLowerCase().equals("oracle")) {
      return new Oracle();
    }
    if (dbEngine.toLowerCase().equals("mssql")) {
      return new MSSQL();
    }
    if (dbEngine.toLowerCase().equals("postgresql")) {
      return new PostgreSQL();
    }
    if (dbEngine.toLowerCase().equals("mysql")) {
      return new MySQL();
    }
    if (dbEngine.toLowerCase().equals("firebirdsql")) {
      return new Firebird();
    }
    return null;
  }
}
