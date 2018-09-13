package io.elastic.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  public static final String CFG_DATABASE_NAME = "databaseName";
  public static final String CFG_PASSWORD = "password";
  public static final String CFG_PORT = "port";
  public static final String CFG_DB_ENGINE = "dbEngine";
  public static final String CFG_HOST = "host";
  public static final String CFG_USER = "user";
  public static final String VARS_REGEXP = "@([\\w_$][\\d\\w_$]*(:(string|boolean|date|number|bigint|float|real))?)";
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  public static Map<String, String> columnTypes = null;

  public static Connection getConnection(final JsonObject config) {
    final String engine = getRequiredNonEmptyString(config, CFG_DB_ENGINE, "Engine is required")
        .toLowerCase();
    final String host = getRequiredNonEmptyString(config, CFG_HOST, "Host is required");
    final String user = getRequiredNonEmptyString(config, CFG_USER, "User is required");
    final Engines engineType = Engines.valueOf(engine.toUpperCase());
    final Integer port = getPort(config, engineType);
    final String password = getPassword(config, engineType);
    final String databaseName = getRequiredNonEmptyString(config, CFG_DATABASE_NAME,
        "Database name is required");
    engineType.loadDriverClass();
    final String connectionString = engineType.getConnectionString(host, port, databaseName);
    LOGGER.info("Connecting to {}", connectionString);
    try {
      return DriverManager.getConnection(connectionString, user, password);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getPassword(final JsonObject config, final Engines engineType) {
    final String password = getNonNullString(config, CFG_PASSWORD);
    if (password.isEmpty() && engineType != Engines.HSQLDB) {
      throw new RuntimeException("Password is required");
    }

    return password;
  }

  private static String getRequiredNonEmptyString(final JsonObject config, final String key,
      final String message) {
    final String value = config.getString(key);
    if (value == null || value.isEmpty()) {
      throw new RuntimeException(message);
    }
    return value;
  }

  public static String getNonNullString(final JsonObject config, final String key) {
    Object value = "";
    try {
      Set<Entry<String, JsonValue>> kvPairs = config.entrySet();
      for (Map.Entry<String, JsonValue> kvPair : kvPairs) {
        if (kvPair.getKey().equals(key)) {
          value = config.get(key);
        }
      }
    } catch (NullPointerException | ClassCastException e) {
      LOGGER.info("key {} doesn't have any mapping: {}", key, e);
    }
    return value.toString().replaceAll("\"", "");
  }

  private static Integer getPort(final JsonObject config, final Engines engineType) {
    final String value = config.getString(CFG_PORT);
    if (value != null && !value.isEmpty()) {
      return Integer.valueOf(value);
    }
    return engineType.defaultPort();
  }

  public static void setStatementParam(PreparedStatement statement, int paramNumber, String colName,
      JsonObject body) throws SQLException {
    try {
      if (isNumeric(colName)) {
        if (body.get(colName) != null) {
          statement.setBigDecimal(paramNumber, body.getJsonNumber(colName).bigDecimalValue());
        } else {
          statement.setBigDecimal(paramNumber, null);
        }
      } else if (isTimestamp(colName)) {
        if (body.get(colName) != null) {
          statement.setTimestamp(paramNumber, Timestamp.valueOf(body.getString(colName)));
        } else {
          statement.setTimestamp(paramNumber, null);
        }
      } else if (isDate(colName)) {
        if (body.get(colName) != null) {
          statement.setDate(paramNumber, Date.valueOf(body.getString(colName)));
        } else {
          statement.setDate(paramNumber, null);
        }
      } else if (isBoolean(colName)) {
        if (body.get(colName) != null) {
          statement.setBoolean(paramNumber, body.getBoolean(colName));
        } else {
          statement.setBoolean(paramNumber, false);
        }
      } else {
        if (body.get(colName) != null) {
          statement.setString(paramNumber, body.getString(colName));
        } else {
          statement.setNull(paramNumber, Types.VARCHAR);
        }
      }
    } catch (java.lang.NumberFormatException e) {
      String message = String
          .format("Provided data: %s can't be cast to the column %s datatype", body.get(colName),
              colName);
      throw new RuntimeException(message);
    }
  }

  private static String detectColumnType(Integer sqlType, String sqlTypeName) {
    if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL || sqlType == Types.TINYINT
        || sqlType == Types.SMALLINT || sqlType == Types.INTEGER || sqlType == Types.BIGINT
        || sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
      return "number";
    }
    if (sqlType == Types.TIMESTAMP) {
      return "timestamp";
    }
    if (sqlType == Types.DATE) {
      return "date";
    }
    if (sqlType == Types.BIT || sqlType == Types.BOOLEAN) {
      return "boolean";
    }
    if (sqlType == Types.OTHER) {
      if (sqlTypeName.toLowerCase().contains("timestamp")) {
        return "timestamp";
      }
    }
    return "string";
  }

  private static String getColumnType(String columnName) {
    return columnTypes.get(columnName.toLowerCase());
  }

  private static boolean isNumeric(String columnName) {
    String type = getColumnType(columnName);
    return type != null && type.equals("number");
  }

  private static boolean isTimestamp(String columnName) {
    String type = getColumnType(columnName);
    return type != null && type.equals("timestamp");
  }

  private static boolean isDate(String columnName) {
    String type = getColumnType(columnName);
    return type != null && type.equals("date");
  }

  private static boolean isBoolean(String columnName) {
    String type = getColumnType(columnName);
    return type != null && type.equals("boolean");
  }

  public static Map<String, String> getColumnTypes(Connection connection, Boolean isOracle,
      String tableName) {
    DatabaseMetaData md;
    ResultSet rs = null;
    Map<String, String> columnTypes = new HashMap<String, String>();
    String schemaName = null;
    try {
      md = connection.getMetaData();
      if (tableName.contains(".")) {
        schemaName = tableName.split("\\.")[0];
        tableName = tableName.split("\\.")[1];
      }
      rs = md.getColumns(null, schemaName, tableName, "%");
      while (rs.next()) {
        String name = rs.getString("COLUMN_NAME").toLowerCase();
        String type = detectColumnType(rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME"));
        columnTypes.put(name, type);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (Exception e) {
          LOGGER.error(e.toString());
        }
      }
    }
    return columnTypes;
  }

  public static Map<String, String> getVariableTypes(String sqlQuery) {
    Map<String, String> columnTypes = new HashMap<String, String>();
    Pattern pattern = Pattern.compile(Utils.VARS_REGEXP);
    Matcher matcher = pattern.matcher(sqlQuery);
    Boolean isEmpty;
    if (matcher.find()) {
      do {
        String result[] = matcher.group().split(":");
        String name = result[0].substring(1);
        String type = result[1];
        columnTypes.put(name, type);
        isEmpty = false;
      } while (matcher.find());
      if (isEmpty) {
        columnTypes.put("empty dataset", "no columns");
      }
    }
    return columnTypes;
  }

  public static JsonObjectBuilder getColumnDataByType(ResultSet rs, ResultSetMetaData metaData,
      int i, JsonObjectBuilder row) {
    try {
      switch (metaData.getColumnType(i)) {
        case Types.BOOLEAN:
        case Types.BIT:
          row.add(metaData.getColumnName(i), rs.getBoolean(metaData.getColumnName(i)));
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          String floatString = Arrays.toString(rs.getBytes(metaData.getColumnName(i)));
          row.add(metaData.getColumnName(i), floatString);
          break;
        case Types.INTEGER:
          row.add(metaData.getColumnName(i), rs.getInt(metaData.getColumnName(i)));
          break;
        case Types.NUMERIC:
        case Types.DECIMAL:
          row.add(metaData.getColumnName(i), rs.getBigDecimal(metaData.getColumnName(i)));
          break;
        case Types.DOUBLE:
          row.add(metaData.getColumnName(i), rs.getDouble(metaData.getColumnName(i)));
          break;
        case Types.FLOAT:
        case Types.REAL:
          row.add(metaData.getColumnName(i), rs.getFloat(metaData.getColumnName(i)));
          break;
        case Types.SMALLINT:
          row.add(metaData.getColumnName(i), rs.getShort(metaData.getColumnName(i)));
          break;
        case Types.TINYINT:
          row.add(metaData.getColumnName(i), rs.getByte(metaData.getColumnName(i)));
          break;
        case Types.BIGINT:
          row.add(metaData.getColumnName(i), rs.getLong(metaData.getColumnName(i)));
          break;
        case Types.TIMESTAMP:
          row.add(metaData.getColumnName(i), rs.getTimestamp(metaData.getColumnName(i)).toString());
          break;
        case Types.DATE:
          row.add(metaData.getColumnName(i), (rs.getDate(metaData.getColumnName(i)) != null) ? rs
              .getDate(metaData.getColumnName(i)).toString() : "");
          break;
        case Types.TIME:
          row.add(metaData.getColumnName(i), rs.getTime(metaData.getColumnName(i)).toString());
          break;
        default:
          String columnName = rs.getString(metaData.getColumnName(i));
          if (columnName != null) {
            row.add(metaData.getColumnName(i), columnName);
          } else {
            row.add(metaData.getColumnName(i), "");
          }
          break;
      }
    } catch (SQLException | java.lang.NullPointerException e) {
      LOGGER.error("Failed to get data by type", e.toString());
      throw new RuntimeException(e);
    }
    return row;
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
            LOGGER.error("Error: the number of matching rows is not exactly one");
            throw new RuntimeException("Error: the number of matching rows is not exactly one");
          }
        }
        return row.build();
      }
    }
  }

  public static boolean isRecordExists (Connection connection, JsonObject body, String sql, String lookupField) throws SQLException{
    try (PreparedStatement stmt = connection.prepareStatement(sql)){
      Utils.setStatementParam(stmt, 1, lookupField, body);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1) > 0;
      }
    }
  }

}
