package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map.Entry;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

public abstract class Query {

  protected Integer skipNumber = 0;
  protected Integer countNumber = 5000;
  protected String tableName = null;
  protected String orderField = null;
  protected String pollingField = null;
  protected Timestamp pollingValue = null;
  protected Timestamp maxPollingValue = null;
  protected String lookupField = null;
  protected String lookupValue = null;

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

  public Timestamp getMaxPollingValue() {
    return maxPollingValue;
  }

  public void setMaxPollingValue(Timestamp maxPollingValue) {
    this.maxPollingValue = maxPollingValue;
  }

  abstract public ArrayList executePolling(Connection connection) throws SQLException;

  abstract public JsonObject executeLookup(Connection connection, JsonObject body)
      throws SQLException;

  abstract public boolean executeRecordExists(Connection connection, JsonObject body)
      throws SQLException;

  abstract public int executeDelete(Connection connection, JsonObject body) throws SQLException;

  abstract public void executeInsert(Connection connection, String tableName, JsonObject body)
      throws SQLException;

  abstract public void executeUpdate(Connection connection, String tableName, String idColumn,
      String idValue, JsonObject body) throws SQLException;

  abstract public void executeUpsert(Connection connection, String idColumn,
      JsonObject body) throws SQLException;

  public ArrayList executeSelectTrigger(Connection connection, String sqlQuery)
      throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
      if (pollingValue != null) {
        stmt.setTimestamp(1, pollingValue);
      }
      try (ResultSet rs = stmt.executeQuery()) {
        ArrayList listResult = new ArrayList();
        JsonObjectBuilder row = Json.createObjectBuilder();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
          for (int i = 1; i <= metaData.getColumnCount(); i++) {
            row = Utils.getColumnDataByType(rs, metaData, i, row);
          }
          listResult.add(row.build());
        }
        return listResult;
      }
    }
  }

  public ArrayList executeSelectQuery(Connection connection, String sqlQuery, JsonObject body)
      throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
      int i = 1;
      for (Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        i++;
      }
      try (ResultSet rs = stmt.executeQuery()) {
        JsonObjectBuilder row = Json.createObjectBuilder();
        ArrayList listResult = new ArrayList();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
          for (i = 1; i <= metaData.getColumnCount(); i++) {
            row = Utils.getColumnDataByType(rs, metaData, i, row);
          }
          listResult.add(row.build());
        }
        return listResult;
      }
    }
  }

  public void validateQuery() {
    if (tableName == null) {
      throw new RuntimeException("Table name is required field");
    }
  }

}
