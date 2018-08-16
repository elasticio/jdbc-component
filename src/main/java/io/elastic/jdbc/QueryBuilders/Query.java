package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import javax.json.JsonObject;

public abstract class Query {

  protected Integer skipNumber = 0;
  protected Integer countNumber = 5000;
  protected String tableName = null;
  protected String orderField = null;
  protected String pollingField = null;
  protected Timestamp pollingValue = null;
  protected String lookupField = null;
  protected String lookupValue = null;

  public Query skip(Integer skip) {
    this.skipNumber = skip;
    return this;
  }

  public Query from(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public Query orderBy(String fieldName) {
    this.orderField = fieldName;
    return this;
  }

  public Query rowsPolling(String fieldName, Timestamp fieldValue) {
    this.pollingField = fieldName;
    this.pollingValue = fieldValue;
    return this;
  }

  public Query lookup(String fieldName, String fieldValue) {
    this.lookupField = fieldName;
    this.lookupValue = fieldValue;
    return this;
  }

  public Query selectPolling(String sqlQuery, Timestamp fieldValue) {
    this.pollingValue = fieldValue;
    return this;
  }

  abstract public ResultSet executePolling(Connection connection) throws SQLException;

  abstract public ResultSet executeLookup(Connection connection, JsonObject body)
      throws SQLException;

  abstract public boolean executeRecordExists(Connection connection, JsonObject body) throws SQLException;

  abstract public int executeDelete(Connection connection, JsonObject body) throws SQLException;

  abstract public void executeInsert(Connection connection, String tableName, JsonObject body)
      throws SQLException;

  abstract public void executeUpdate(Connection connection, String tableName, String idColumn,
      String idValue, JsonObject body) throws SQLException;

  abstract public ResultSet executeSelectQuery(Connection connection, String sqlQuery,
      JsonObject body) throws SQLException;

  abstract public ResultSet executeSelectTrigger(Connection connection, String sqlQuery)
      throws SQLException;

  public void validateQuery() {
    if (tableName == null) {
      throw new RuntimeException("Table name is required field");
    }
  }

  public static String preProcessSelect(String sqlQuery) {
    sqlQuery = sqlQuery.trim();
    if (!isSelect(sqlQuery)) {
      throw new RuntimeException("Unresolvable SELECT query");
    }
    return sqlQuery.replaceAll(Utils.VARS_REGEXP, "?");
  }

  public static boolean isSelect(String sqlQuery) {
    String pattern = "select";
    return sqlQuery.toLowerCase().startsWith(pattern);
  }

}
