package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.ProcedureFieldsNameProvider;
import io.elastic.jdbc.ProcedureParameter;
import io.elastic.jdbc.ProcedureParameter.Direction;
import io.elastic.jdbc.Utils;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

public class PostgreSQL extends Query {

  public ArrayList executePolling(Connection connection) throws SQLException {
    validateQuery();
    String sql = "WITH results_cte AS" +
        "(" +
        "    SELECT" +
        "        *," +
        "        ROW_NUMBER() OVER (ORDER BY " + pollingField + ") AS rownum" +
        "    FROM " + tableName +
        "    WHERE " + pollingField + " > ?" +
        " )" +
        " SELECT *" +
        " FROM results_cte" +
        " WHERE rownum <= ?";
    return getRowsExecutePolling(connection, sql);
  }

  public JsonObject executeLookup(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "WITH results_cte AS" +
        "(" +
        "    SELECT" +
        "        *," +
        "        ROW_NUMBER() OVER (ORDER BY " + lookupField + ") AS rownum" +
        "    FROM " + tableName +
        "    WHERE " + lookupField + " = ?" +
        " )" +
        " SELECT *" +
        " FROM results_cte" +
        " WHERE rownum > ?" +
        " AND rownum < ?";
    return getLookupRow(connection, body, sql, skipNumber, countNumber + skipNumber);
  }

  public int executeDelete(Connection connection, JsonObject body) throws SQLException {
    validateQuery();
    String sql = "DELETE" +
        " FROM " + tableName +
        " WHERE " + lookupField + " = ?";
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
        Utils.setStatementParam(stmt, 1, entry.getKey(), body);
      }
      return stmt.executeUpdate();
    }
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

  protected CallableStatement prepareCallableStatement(Connection connection, String procedureName,
      Map<String, ProcedureParameter> procedureParams, JsonObject messageBody)
      throws SQLException {
    CallableStatement stmt = connection.prepareCall(
        String.format("{call %s%s}", procedureName,
            generateStatementWildcardMask(procedureParams)));

    for (int inc = 1; inc <= procedureParams.size(); inc++) {
      final int order = inc;
      ProcedureParameter parameter = procedureParams.values()
          .stream()
          .filter(p -> p.getOrder() == order)
          .findFirst().orElseThrow(() -> new IllegalStateException("Can't find parameter by order"));

      if (parameter.getDirection() == Direction.IN || parameter.getDirection() == Direction.INOUT) {
        if (parameter.getDirection() == Direction.INOUT) {
          stmt.registerOutParameter(inc, parameter.getType());
        }

        String type = Utils.cleanJsonType(Utils.detectColumnType(parameter.getType(), ""));
        switch (type) {
          case ("number"):
            stmt.setObject(inc,
                messageBody.getJsonNumber(parameter.getName()).toString(),
                parameter.getType());
            break;
          case ("boolean"):
            stmt.setObject(inc, messageBody.getBoolean(parameter.getName()),
                parameter.getType());
            break;
          default:
            stmt.setObject(inc, messageBody.getString(parameter.getName()),
                parameter.getType());
        }
      } else if (parameter.getDirection() == Direction.OUT) {
        stmt.registerOutParameter(inc, parameter.getType());
      }
    }

    return stmt;
  }

  public JsonObject callProcedure(Connection connection, JsonObject body, JsonObject configuration)
      throws SQLException {

    Map<String, ProcedureParameter> procedureParams = ProcedureFieldsNameProvider
        .getProcedureMetadata(configuration)
        .stream()
        .collect(Collectors.toMap(ProcedureParameter::getName, Function.identity()));

    CallableStatement stmt = prepareCallableStatement(connection,
        configuration.getString("procedureName"), procedureParams, body);

    connection.setAutoCommit(false);
    stmt.execute();
    connection.commit();

    JsonObjectBuilder resultBuilder = Json.createObjectBuilder();

    procedureParams.values().stream()
        .filter(param -> param.getDirection() == Direction.OUT
            || param.getDirection() == Direction.INOUT)
        .sorted(Comparator.comparingInt(ProcedureParameter::getOrder))
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

  protected JsonObjectBuilder addValueToResultJson(JsonObjectBuilder resultBuilder,
      CallableStatement stmt, Map<String, ProcedureParameter> procedureParams, String name)
      throws SQLException {

    if (stmt.getObject(procedureParams.get(name).getOrder()) == null) {
      return resultBuilder.addNull(name);
    }

    String type = Utils
        .cleanJsonType(Utils.detectColumnType(procedureParams.get(name).getType(), ""));

    switch (type) {
      case ("boolean"):
        return resultBuilder.add(name, stmt.getBoolean(procedureParams.get(name).getOrder()));
      case ("number"):
        return resultBuilder.add(name, stmt.getDouble(procedureParams.get(name).getOrder()));
      case ("array"):
        ResultSet cursorSet = (ResultSet) stmt.getObject(procedureParams.get(name).getOrder());
        JsonArrayBuilder array = Json.createArrayBuilder();

        Map<String, ProcedureParameter> params =
            IntStream.range(1, cursorSet.getMetaData().getColumnCount() + 1)
                .mapToObj(i -> {
                  try {
                    return new ProcedureParameter(cursorSet.getMetaData().getColumnName(i),
                        Direction.OUT, cursorSet.getMetaData().getColumnType(i), i);
                  } catch (SQLException e) {
                    throw new IllegalArgumentException(e);
                  }
                })
                .collect(Collectors.toMap(ProcedureParameter::getName, Function.identity()));

        Map<String, String> typesMap = params.values().stream()
            .collect(Collectors.toMap(ProcedureParameter::getName, p -> Utils
                .cleanJsonType(
                    Utils.detectColumnType(p.getType(), ""))));

        while (cursorSet.next()) {
          JsonObjectBuilder entity = Json.createObjectBuilder();

          params.keySet().forEach(key -> {
            try {
              if (cursorSet.getObject(params.get(key).getOrder()) == null) {
                entity.addNull(key);
                return;
              }

              switch (typesMap.get(key)) {
                case ("number"):
                  entity.add(key, cursorSet.getDouble(params.get(key).getOrder()));
                  break;
                case ("boolean"):
                  entity.add(key, cursorSet.getBoolean(params.get(key).getOrder()));
                  break;
                default:
                  entity.add(key, cursorSet.getString(params.get(key).getOrder()));
                  break;
              }
            } catch (SQLException e) {
              e.printStackTrace();
            }
          });

          array.add(entity.build());
        }

        return resultBuilder.add(name, array.build());
      default:
        return resultBuilder.add(name, stmt.getString(procedureParams.get(name).getOrder()));
    }
  }
}
