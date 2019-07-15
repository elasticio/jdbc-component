package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.ProcedureFieldsNameProvider;
import io.elastic.jdbc.ProcedureParameter;
import io.elastic.jdbc.ProcedureParameter.Direction;
import io.elastic.jdbc.Utils;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Query {

  private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

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

  abstract public int executeDelete(Connection connection, JsonObject body) throws SQLException;

  abstract public void executeInsert(Connection connection, String tableName, JsonObject body)
      throws SQLException;

  abstract public void executeUpdate(Connection connection, String tableName, String idColumn,
      String idValue, JsonObject body) throws SQLException;

  public boolean executeRecordExists(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "SELECT COUNT(*)" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      Utils.setStatementParam(stmt, 1, lookupField, body);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1) > 0;
      }
    }
  }

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
      if (stmt.getParameterMetaData().getParameterCount() != 0) {
        for (Entry<String, JsonValue> entry : body.entrySet()) {
          Utils.setStatementParam(stmt, i, entry.getKey(), body);
          i++;
        }
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

  public JsonObject executeUpsert(Connection connection, String idColumn,
      JsonObject body) throws SQLException {
    validateQuery();
    JsonObject foundRow;
    JsonObjectBuilder row = Json.createObjectBuilder();
    int rowsCount = 0;
    int i;
    ResultSet rs;
    ResultSetMetaData metaData;

    StringBuilder keys = new StringBuilder();
    StringBuilder values = new StringBuilder();
    StringBuilder setString = new StringBuilder();
    for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
      if (keys.length() > 0) {
        keys.append(",");
      }
      keys.append(entry.getKey());
      if (values.length() > 0) {
        values.append(",");
      }
      values.append("?");
      if (!entry.getKey().equals(idColumn)) {
        if (setString.length() > 0) {
          setString.append(",");
        }
        setString.append(entry.getKey()).append(" = ?");
      }
    }

    String sqlSELECT =
        "    SELECT" +
            "        *" +
            "    FROM " + tableName +
            "    WHERE " + idColumn + " = ?";
    String sqlInsert = "INSERT INTO " + tableName +
        " (" + keys.toString() + ")" +
        " VALUES (" + values.toString() + ")";
    String sqlUpdate = "UPDATE " + tableName +
        " SET " + setString.toString() +
        " WHERE " + idColumn + " = ?";

    PreparedStatement stmtSelect = null;
    PreparedStatement stmtInsert = null;
    PreparedStatement stmtUpdate = null;

    try {
      connection.setAutoCommit(false);

      stmtSelect = connection.prepareStatement(sqlSELECT);
      Utils.setStatementParam(stmtSelect, 1, idColumn, body);
      rs = stmtSelect.executeQuery();
      metaData = rs.getMetaData();
      while (rs.next()) {
        for (i = 1; i <= metaData.getColumnCount(); i++) {
          row = Utils.getColumnDataByType(rs, metaData, i, row);
        }
        rowsCount++;
        if (rowsCount > 1) {
          throw new RuntimeException("Error: the number of matching rows is not exactly one");
        }
      }
      foundRow = row.build();

      i = 1;
      if (foundRow.size() == 0) {
        stmtInsert = connection.prepareStatement(sqlInsert);
        for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
          Utils.setStatementParam(stmtInsert, i, entry.getKey(), body);
          i++;
        }
        stmtInsert.execute();
      } else {
        stmtUpdate = connection.prepareStatement(sqlUpdate);
        for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
          if (!entry.getKey().equals(idColumn)) {
            Utils.setStatementParam(stmtUpdate, i, entry.getKey(), body);
            i++;
          }
        }
        Utils.setStatementParam(stmtUpdate, i, idColumn, body);
        stmtUpdate.execute();
      }

      rs = stmtSelect.executeQuery();
      metaData = rs.getMetaData();
      rowsCount = 0;
      while (rs.next()) {
        for (i = 1; i <= metaData.getColumnCount(); i++) {
          row = Utils.getColumnDataByType(rs, metaData, i, row);
        }
        rowsCount++;
        if (rowsCount > 1) {
          throw new RuntimeException("Error: the number of matching rows is not exactly one");
        }
      }
      connection.commit();

    } finally {
      if (stmtSelect != null) {
        stmtSelect.close();
      }
      if (stmtInsert != null) {
        stmtInsert.close();
      }
      if (stmtUpdate != null) {
        stmtUpdate.close();
      }
      connection.setAutoCommit(true);
    }
    return row.build();
  }

  public void validateQuery() {
    if (tableName == null) {
      throw new RuntimeException("Table name is required field");
    }
  }

  public ArrayList getRowsExecutePolling(Connection connection, String sql) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setTimestamp(1, pollingValue);
      stmt.setInt(2, countNumber);
      LOGGER.info("SQL statement: {} with params: {}, {}", sql, pollingValue, countNumber);
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

  public static JsonObject getLookupRow(Connection connection, JsonObject body, String sql,
      Integer secondParameter, Integer thirdParameter)
      throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      JsonObjectBuilder row = Json.createObjectBuilder();
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, 1, entry.getKey(), body);
      }
      stmt.setInt(2, secondParameter);
      stmt.setInt(3, thirdParameter);
      try (ResultSet rs = stmt.executeQuery()) {
        ResultSetMetaData metaData = rs.getMetaData();
        int rowsCount = 0;
        while (rs.next()) {
          for (int i = 1; i <= metaData.getColumnCount(); i++) {
            row = Utils.getColumnDataByType(rs, metaData, i, row);
          }
          rowsCount++;
          if (rowsCount > 1) {
            throw new RuntimeException("Error: the number of matching rows is not exactly one");
          }
        }
        return row.build();
      }
    }
  }

  public JsonObject callProcedure(Connection connection, JsonObject body, JsonObject configuration)
      throws SQLException {

    Map<String, ProcedureParameter> procedureParams = ProcedureFieldsNameProvider
        .getProcedureMetadata(configuration).stream()
        .collect(Collectors.toMap(ProcedureParameter::getName, Function.identity()));

    CallableStatement stmt = prepareCallableStatement(connection,
        configuration.getString("procedureName"), procedureParams, body);

    stmt.execute();

    JsonObjectBuilder resultBuilder = Json.createObjectBuilder();

    procedureParams.values().stream()
        .filter(param -> param.getDirection() == Direction.OUT
            || param.getDirection() == Direction.INOUT)
        .forEach(param -> {
          try {
            addValueToResultJson(resultBuilder, stmt, procedureParams, param.getName());
          } catch (SQLException e) {
            e.printStackTrace();
          }
        });

    stmt.close();

    return resultBuilder.build();
  }

  private String generateStatementParamsMask(Map<String, ProcedureParameter> procedureParams) {
    StringBuilder statementArgsStructure = new StringBuilder("(");
    procedureParams.keySet()
        .forEach(p -> statementArgsStructure
            .append(p)
            .append(" => :")
            .append(p)
            .append(", ")
        );

    String result = statementArgsStructure.toString();
    if (procedureParams.size() > 0) {
      result = statementArgsStructure.substring(0, statementArgsStructure.length() - 2);
    }
    return result + ")";
  }

  private CallableStatement prepareCallableStatement(Connection connection, String procedureName,
      Map<String, ProcedureParameter> procedureParams, JsonObject messageBody)
      throws SQLException {
    CallableStatement stmt = connection.prepareCall(
        String.format("{call %s%s}", procedureName,
            generateStatementParamsMask(procedureParams)));

    for (ProcedureParameter parameter : procedureParams.values()) {
      if (parameter.getDirection() == Direction.IN || parameter.getDirection() == Direction.INOUT) {
        if (parameter.getDirection() == Direction.INOUT) {
          stmt.registerOutParameter(parameter.getName(), parameter.getType());
        }

        String type = Utils.cleanJsonType(Utils.detectColumnType(parameter.getType(), ""));
        switch (type) {
          case ("number"):
            stmt.setObject(parameter.getName(),
                messageBody.getJsonNumber(parameter.getName()).toString(),
                parameter.getType());
            break;
          case ("boolean"):
            stmt.setObject(parameter.getName(), messageBody.getBoolean(parameter.getName()),
                parameter.getType());
            break;
          default:
            stmt.setObject(parameter.getName(), messageBody.getString(parameter.getName()),
                parameter.getType());
        }
      } else if (parameter.getDirection() == Direction.OUT) {
        stmt.registerOutParameter(parameter.getName(), parameter.getType());
      }
    }

    return stmt;
  }

  private JsonObjectBuilder addValueToResultJson(JsonObjectBuilder resultBuilder,
      CallableStatement stmt, Map<String, ProcedureParameter> procedureParams, String name)
      throws SQLException {

    String type = Utils
        .cleanJsonType(Utils.detectColumnType(procedureParams.get(name).getType(), ""));

    switch (type) {
      case ("boolean"):
        resultBuilder.add(name, stmt.getBoolean(name));
        break;
      case ("number"):
        resultBuilder.add(name, stmt.getDouble(name));
        break;
      default:
        resultBuilder.add(name, stmt.getString(name));
    }

    return resultBuilder;
  }

}
