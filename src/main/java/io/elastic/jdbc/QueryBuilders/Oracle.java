package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import javax.json.JsonObject;
import javax.json.JsonValue;

public class Oracle extends Query {

  public ResultSet executeSelectQuery(Connection connection, String sqlQuery, JsonObject body)
      throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(sqlQuery);
    int i = 1;
    for (Entry<String, JsonValue> entry : body.entrySet()) {
      Utils.setStatementParam(stmt, i, entry.getKey(), body);
      i++;
    }
    return stmt.executeQuery();
  }

  public ResultSet executeSelectTrigger(Connection connection, String sqlQuery)
      throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(sqlQuery);
    if (pollingValue != null) {
      stmt.setTimestamp(1, pollingValue);
    }
    return stmt.executeQuery();
  }

  public ResultSet executePolling(Connection connection) throws SQLException {
    validateQuery();
    String sql = "SELECT * FROM " +
        " (SELECT  b.*, rank() over (order by " + pollingField + ") as rnk FROM " +
        tableName + " b) WHERE " + pollingField + " > ?" +
        " AND rnk BETWEEN ? AND ?" +
        " ORDER BY " + pollingField;
    PreparedStatement stmt = connection.prepareStatement(sql);

    /* data types mapping https://docs.oracle.com/cd/B19306_01/java.102/b14188/datamap.htm */
    stmt.setTimestamp(1, pollingValue);
    stmt.setInt(2, skipNumber);
    stmt.setInt(3, countNumber);
    return stmt.executeQuery();
  }

  public ResultSet executeLookup(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "SELECT * FROM " +
        "(SELECT  b.*, rank() OVER (ORDER BY " + lookupField + ") AS rnk FROM " +
        tableName + " b) WHERE " + lookupField + " = ? " +
        "AND rnk BETWEEN ? AND ? " +
        "ORDER BY " + lookupField;
    PreparedStatement stmt = connection.prepareStatement(sql);
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      Utils.setStatementParam(stmt, 1, entry.getKey(), body);
    }
    stmt.setInt(2, skipNumber);
    stmt.setInt(3, countNumber);
    return stmt.executeQuery();
  }

  public int executeDelete(Connection connection, JsonObject body) throws SQLException {
    String sql = "DELETE" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    PreparedStatement stmt = connection.prepareStatement(sql);
    stmt.setString(1, lookupValue);
    return stmt.executeUpdate();
  }

  public boolean executeRecordExists(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "SELECT COUNT(*)" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    PreparedStatement stmt = connection.prepareStatement(sql);
    Utils.setStatementParam(stmt, 1, lookupField, body);
    ResultSet rs = stmt.executeQuery();
    rs.next();
    return rs.getInt(1) > 0;
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
    PreparedStatement stmt = connection.prepareStatement(sql);
    int i = 1;
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      Utils.setStatementParam(stmt, i, entry.getKey(), body);
      i++;
    }
    stmt.execute();
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
    PreparedStatement stmt = connection.prepareStatement(sql);
    int i = 1;
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      Utils.setStatementParam(stmt, i, entry.getKey(), body);
      i++;
    }
    Utils.setStatementParam(stmt, i, idColumn, body);
    stmt.execute();
  }

}
