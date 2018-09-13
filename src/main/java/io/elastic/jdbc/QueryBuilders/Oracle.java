package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

public class Oracle extends Query {

  public ArrayList executePolling(Connection connection) throws SQLException {
    validateQuery();
    String sql = "SELECT * FROM " +
        " (SELECT  b.*, rank() over (order by " + pollingField + ") as rnk FROM " +
        tableName + " b) WHERE " + pollingField + " > ?" +
        " AND rnk BETWEEN ? AND ?" +
        " ORDER BY " + pollingField;
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      /* data types mapping https://docs.oracle.com/cd/B19306_01/java.102/b14188/datamap.htm */
      stmt.setTimestamp(1, pollingValue);
      stmt.setInt(2, skipNumber);
      stmt.setInt(3, countNumber);
      try (ResultSet rs = stmt.executeQuery()) {
        ArrayList listResult = new ArrayList();
        JsonObjectBuilder row = Json.createObjectBuilder();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
          for (int i = 1; i <= metaData.getColumnCount(); i++) {
            row = Utils.getColumnDataByType(rs, metaData, i, row);
            if (metaData.getColumnName(i).toUpperCase().equals(pollingField.toUpperCase())) {
              if (maxPollingValue.before(rs.getTimestamp(i))) {
                if (rs.getString(metaData.getColumnName(i)).length() > 10) {
                  maxPollingValue = java.sql.Timestamp
                      .valueOf(rs.getString(metaData.getColumnName(i)));
                } else {
                  maxPollingValue = java.sql.Timestamp
                      .valueOf(rs.getString(metaData.getColumnName(i)) + " 00:00:00");
                }
              }
            }
          }
          listResult.add(row.build());
        }
        return listResult;
      }
    }
  }

  public JsonObject executeLookup(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "SELECT * FROM " +
        "(SELECT  b.*, rank() OVER (ORDER BY " + lookupField + ") AS rnk FROM " +
        tableName + " b) WHERE " + lookupField + " = ? " +
        "AND rnk BETWEEN ? AND ? " +
        "ORDER BY " + lookupField;
    return Utils.getLookupRow(connection, body, sql, skipNumber, countNumber);
  }

  public int executeDelete(Connection connection, JsonObject body) throws SQLException {
    String sql = "DELETE" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, lookupValue);
      return stmt.executeUpdate();
    }
  }

  public boolean executeRecordExists(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "SELECT COUNT(*)" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    return Utils.isRecordExists(connection, body, sql, lookupField);
  }

  public void executeInsert(Connection connection, String tableName, JsonObject body)
      throws SQLException {
    validateQuery();
    StringBuilder keys = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      if (keys.length() > 0) {
        keys.append(",");
      }
      keys.append(entry.getKey());
      if (values.length() > 0) {
        values.append(",");
      }
      values.append("?");
    }
    String sql = "INSERT INTO " + tableName +
        " (" + keys.toString() + ")" +
        " VALUES (" + values.toString() + ")";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      int i = 1;
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        i++;
      }
      stmt.execute();
    }
  }

  public void executeUpdate(Connection connection, String tableName, String idColumn,
      String idValue, JsonObject body) throws SQLException {
    validateQuery();
    StringBuilder setString = new StringBuilder();
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      if (setString.length() > 0) {
        setString.append(",");
      }
      setString.append(entry.getKey()).append(" = ?");
    }
    String sql = "UPDATE " + tableName +
        " SET " + setString.toString() +
        " WHERE " + idColumn + " = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      int i = 1;
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        i++;
      }
      Utils.setStatementParam(stmt, i, idColumn, body);
      stmt.execute();
    }
  }

  public void executeUpsert(Connection connection, String idColumn, JsonObject body)
      throws SQLException {
    validateQuery();
    StringBuilder keys = new StringBuilder();
    StringBuilder values = new StringBuilder();
    StringBuilder setString = new StringBuilder();
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      if (setString.length() > 0) {
        setString.append(",");
      }
      setString.append(entry.getKey()).append(" = ?");
      if (keys.length() > 0) {
        keys.append(",");
      }
      keys.append(entry.getKey());
      if (values.length() > 0) {
        values.append(",");
      }
      values.append("?");
    }
    String sql = "BEGIN " +
        " INSERT INTO " + tableName +
        " (" + keys.toString() + ")" +
        " VALUES (" + values.toString() + ");" +
        " EXCEPTION" +
        " WHEN DUP_VAL_ON_INDEX THEN" +
        " UPDATE " + tableName +
        " SET " + setString.toString() +
        " WHERE " + idColumn + " = ?;" +
        " END;";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      //set Statement parameters for Insert (i) and Update operation (i + countBodyEntry)
      int i = 1;
      int countBodyEntry = body.size();
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, i, entry.getKey(), body);
        Utils.setStatementParam(stmt, i + countBodyEntry, entry.getKey(), body);
        i++;
      }
      Utils.setStatementParam(stmt, i + countBodyEntry, idColumn, body);
      stmt.execute();
    }
  }
}
