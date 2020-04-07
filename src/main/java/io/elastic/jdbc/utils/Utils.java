package io.elastic.jdbc.utils;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  private static final String CFG_DATABASE_NAME = "databaseName";
  private static final String CFG_PASSWORD = "password";
  private static final String CFG_PORT = "port";
  public static final String CFG_DB_ENGINE = "dbEngine";
  private static final String CFG_HOST = "host";
  private static final String CFG_USER = "user";
  public static final String VARS_REGEXP = "\\B@([\\w_$][\\d\\w_$]*(:(string|boolean|date|number|bigint|float|real))?)";
  public static final String TEMPLATE_REGEXP = "\\B@(?:(?![=\\)\\(])[\\S])+";
  private static final String PROPERTY_DB_ENGINE = "dbEngine";
  private static final String PROPERTY_TABLE_NAME = "tableName";
  private static final String ENABLED_REBOUND = "Yes";
  private static final String DISABLED_REBOUND = "No";
  private static final String PROPERTY_REBOUND = "reboundEnabled";
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  public static Map<String, String> columnTypes = null;
  public static final Map<String, List<String>> reboundDbState;
  static {
    reboundDbState = new HashMap<>();
    reboundDbState.put(Engines.POSTGRESQL.name(), Collections.singletonList("40P01"));
    reboundDbState.put(Engines.MSSQL.name(), Collections.singletonList("40001"));
    reboundDbState.put(Engines.MYSQL.name(), Arrays.asList("40001", "XA102"));
    reboundDbState.put(Engines.ORACLE.name(), Collections.singletonList("61000"));
    reboundDbState.put(Engines.FIREBIRDSQL.name(), Arrays.asList("10054", "10038"));
  }

  public static Connection getConnection(final JsonObject config) throws SQLException {
    final String engine = getRequiredNonEmptyString(config, CFG_DB_ENGINE, "Engine is required")
        .toLowerCase();
    final String host = getRequiredNonEmptyString(config, CFG_HOST, "Host is required");
    final Engines engineType = Engines.valueOf(engine.toUpperCase());
    final Integer port = getPort(config, engineType);
    final String databaseName = getRequiredNonEmptyString(config, CFG_DATABASE_NAME,
        "Database name is required");
    engineType.loadDriverClass();
    final String connectionString = engineType.getConnectionString(host, port, databaseName);
    Properties properties = getConfigurationProperties(config, engineType);
    LOGGER.info("Connecting to {}", connectionString);
    return DriverManager.getConnection(connectionString, properties);
  }

  private static String getPassword(final JsonObject config, final Engines engineType) {
    final String password = getNonNullString(config, CFG_PASSWORD);
    if (password.isEmpty() && engineType != Engines.HSQLDB) {
      throw new RuntimeException("Password is required");
    }

    return password;
  }

  private static Properties getConfigurationProperties(final JsonObject config,
      final Engines engineType) {
    final String user = getRequiredNonEmptyString(config, CFG_USER, "User is required");
    final String password = getPassword(config, engineType);
    final String configurationProperties = getNonNullString(config, "configurationProperties");
    Properties properties = new Properties();
    if (!configurationProperties.isEmpty()) {
      try {
        properties.load(new StringReader(configurationProperties.replaceAll("&", "\n")));
      } catch (IOException e) {
        LOGGER.error("Failed while parsing configuration properties. Error: " + e.getMessage());
        throw new RuntimeException(e);
      }
    }
    LOGGER.info("Got properties: {}", properties);
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    return properties;
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
    final String value = getNonNullString(config, CFG_PORT);
    if (value != null && !value.isEmpty()) {
      return Integer.valueOf(value);
    }
    return engineType.defaultPort();
  }

  public static void setStatementParam(PreparedStatement statement, int paramNumber, String colName,
      JsonObject body) throws SQLException {
    try {
      if (isNumeric(colName)) {
          if ((body.get(colName) != null) && (body.get(colName) != JsonValue.NULL)) {
            // workaround for Firebase -> isNumeric=true, but value = "true" or "false"
            if (body.get(colName).toString().equals("true") || body.get(colName).toString().equals("false")) {
              statement.setBoolean(paramNumber, body.getBoolean(colName));
            } else {
              statement.setBigDecimal(paramNumber, body.getJsonNumber(colName).bigDecimalValue());
            }
          } else {
            statement.setBigDecimal(paramNumber, null);
          }
      } else if (isTimestamp(colName)) {
        if ((body.get(colName) != null) && (body.get(colName) != JsonValue.NULL)) {
          statement.setTimestamp(paramNumber, Timestamp.valueOf(body.getString(colName)));
        } else {
          statement.setTimestamp(paramNumber, null);
        }
      } else if (isDate(colName)) {
        if ((body.get(colName) != null) && (body.get(colName) != JsonValue.NULL)) {
          statement.setDate(paramNumber, Date.valueOf(body.getString(colName)));
        } else {
          statement.setDate(paramNumber, null);
        }
      } else if (isBoolean(colName)) {
        if ((body.get(colName) != null) && (body.get(colName) != JsonValue.NULL)) {
          statement.setBoolean(paramNumber, body.getBoolean(colName));
        } else {
          statement.setBoolean(paramNumber, false);
        }
      } else {
        if ((body.get(colName) != null) && (body.get(colName) != JsonValue.NULL)) {
          statement.setString(paramNumber, body.getString(colName));
        } else {
          statement.setNull(paramNumber, Types.VARCHAR);
        }
      }
    } catch (java.lang.NumberFormatException | java.lang.ClassCastException e) {
      String message = String
          .format("Provided data: %s can't be cast to the column %s datatype", body.get(colName),
              colName);
      throw new RuntimeException(message);
    }
  }

  public static String detectColumnType(Integer sqlType, String sqlTypeName) {
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
    if (sqlType == -10 || sqlType == Types.REF_CURSOR) {
      return "array";
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

  public static Map<String, String> getColumnTypes(Connection connection, String tableName) {
    DatabaseMetaData md;
    ResultSet rs = null;
    Map<String, String> columnTypes = new HashMap<>();
    String schemaName = "";

    try {
      md = connection.getMetaData();
      if (tableName.contains(".")) {
        schemaName = tableName.split("\\.")[0];
        tableName = tableName.split("\\.")[1];
      }
      rs = md.getColumns("", schemaName, tableName, "%");
      if (!rs.isBeforeFirst()){
        // ResultSet is empty, maybe we need to use null as Catalog?
        rs = md.getColumns(null, schemaName, tableName, "%");
      }
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
    Map<String, String> columnTypes = new HashMap<>();
    Pattern pattern = Pattern.compile(Utils.VARS_REGEXP);
    Matcher matcher = pattern.matcher(sqlQuery);
    Boolean isEmpty;
    if (matcher.find()) {
      do {
        String result[] = matcher.group().split(":");
        String name;
        String type;
        if (result.length > 0 && result.length < 3) {
          name = result[0].substring(1);
          if (result.length == 1) {
            type = "string";
          } else {
            type = result[1];
          }
        } else {
          throw new RuntimeException("Incorrect prepared statement" + matcher.group());
        }
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
      final String columnName = metaData.getColumnName(i);
      if (null == rs.getObject(columnName)) {
        row.add(columnName, JsonValue.NULL);
        return row;
      }
      switch (metaData.getColumnType(i)) {
        case Types.BOOLEAN:
        case Types.BIT:
          row.add(columnName, rs.getBoolean(columnName));
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          String floatString = Arrays.toString(rs.getBytes(columnName));
          row.add(columnName, floatString);
          break;
        case Types.INTEGER:
          row.add(columnName, rs.getInt(columnName));
          break;
        case Types.NUMERIC:
        case Types.DECIMAL:
          row.add(columnName, rs.getBigDecimal(columnName));
          break;
        case Types.DOUBLE:
          row.add(columnName, rs.getDouble(columnName));
          break;
        case Types.FLOAT:
        case Types.REAL:
          row.add(columnName, rs.getFloat(columnName));
          break;
        case Types.SMALLINT:
          row.add(columnName, rs.getShort(columnName));
          break;
        case Types.TINYINT:
          row.add(columnName, rs.getByte(columnName));
          break;
        case Types.BIGINT:
          row.add(columnName, rs.getLong(columnName));
          break;
        case Types.TIMESTAMP:
          row.add(columnName, rs.getTimestamp(columnName).toString());
          break;
        case Types.DATE:
          row.add(columnName, rs.getDate(columnName).toString());
          break;
        case Types.TIME:
          row.add(columnName, rs.getTime(columnName).toString());
          break;
        default:
          row.add(columnName, rs.getString(columnName));
          break;
      }
    } catch (SQLException | java.lang.NullPointerException e) {
      LOGGER.error("Failed to get data by type", e);
      throw new RuntimeException(e);
    }
    return row;
  }

  public static String cleanJsonType(String rawType) {
    switch (rawType) {
      case ("number"):
        return "number";
      case ("boolean"):
        return "boolean";
      case ("array"):
        return "array";
      case ("object"):
        return "object";
      default:
        return "string";
    }
  }

  /**
   * Converts table name according Engine Type
   *
   * @param configuration should contains tableName
   * @param isOracleOrFirebird flag is Engine type equals `oracle`
   */
  public static String getTableName(JsonObject configuration, boolean isOracleOrFirebird) {
    if (configuration.containsKey(PROPERTY_TABLE_NAME)
        && getNonNullString(configuration, PROPERTY_TABLE_NAME).length() != 0) {
      String tableName = configuration.getString(PROPERTY_TABLE_NAME);
      if (tableName.contains(".")) {
        tableName = isOracleOrFirebird ? tableName.split("\\.")[1].toUpperCase() : tableName.split("\\.")[1];
      } else {
        tableName = isOracleOrFirebird ? tableName.toUpperCase() : tableName;
      }
      return tableName;
    } else {
      throw new RuntimeException("Table name is required field");
    }
  }

  /**
   * Checks is Engine Type is presents in configuration and return it
   *
   * @param configuration should contains dbEngine
   */
  public static String getDbEngine(JsonObject configuration) {
    if (getNonNullString(configuration, PROPERTY_DB_ENGINE).length() != 0) {
      return configuration.getString(PROPERTY_DB_ENGINE);
    } else {
      throw new RuntimeException("DB Engine is required field");
    }
  }

  /**
   * Converts JDBC column type name to js type according to http://db.apache.org/ojb/docu/guides/jdbc-types.html
   *
   * @param sqlType JDBC column type
   */
  public static String convertType(Integer sqlType) {
    if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL || sqlType == Types.TINYINT
        || sqlType == Types.SMALLINT || sqlType == Types.INTEGER || sqlType == Types.BIGINT
        || sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
      return "number";
    }
    if (sqlType == Types.BIT || sqlType == Types.BOOLEAN) {
      return "boolean";
    }
    return "string";
  }

  public static ArrayList<String> getPrimaryKeyNames(String catalog, String schemaName, String tableName,
      DatabaseMetaData dbMetaData) {
    ArrayList<String> primaryKeysNames = new ArrayList<>();
    try (ResultSet resultSet = dbMetaData.getPrimaryKeys(catalog, schemaName, tableName)) {
      LOGGER.debug("Getting primary keys names...");
      while (resultSet.next()) {
        primaryKeysNames.add(resultSet.getString("COLUMN_NAME"));
      }
      LOGGER.debug("Found primary key(s): '{}'", primaryKeysNames);
    } catch (SQLException e) {
      String errorMessage = "Cannot get Primary Keys Names";
      LOGGER.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
    return primaryKeysNames;
  }

  /**
   * Converts table name to TableNamePattern
   *
   * @param tableName should contains tableName
   */
  public static String getTableNamePattern(String tableName) {
    if (tableName.contains(".")) {
      tableName = tableName.split("\\.")[1];
    }
    return tableName;
  }

  /**
   * Gets SchemaNamePattern from tableName
   *
   * @param tableName should contains tableName
   */
  public static String getSchemaNamePattern(String tableName) {
    String schemaName = null;
    if (tableName.contains(".")) {
      schemaName = tableName.split("\\.")[0];
    }
    return schemaName;
  }

  /**
   * Defines is field autoincrement
   *
   * @param resultSet contains column properties
   * @param isOracle flag is Engine type equals `oracle`
   */
  public static Boolean isAutoincrement(ResultSet resultSet, final boolean isOracle) {
    boolean isAutoincrement = false;
    if (!isOracle) {
      try {
        isAutoincrement = resultSet.getString("IS_AUTOINCREMENT").equals("YES");
      } catch (SQLException e) {
        String errorMessage = "Cannot get property 'IS_AUTOINCREMENT'";
        LOGGER.error(errorMessage, e);
        throw new RuntimeException(errorMessage, e);
      }
    }
    return isAutoincrement;
  }

  /**
   * Defines is field not null
   *
   * @param resultSet contains column properties
   */
  public static Boolean isNotNull(ResultSet resultSet) {
    try {
      return resultSet.getString("IS_NULLABLE").equals("NO");
    } catch (SQLException e) {
      String errorMessage = "Cannot get property 'IS_NULLABLE'";
      LOGGER.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }

  /**
   * Defines is field calculated
   *
   * @param resultSet contains column properties
   * @param dbEngine Engine type
   */
  public static Boolean isCalculated(ResultSet resultSet, final String dbEngine) {
    switch (dbEngine) {
      case "mysql":
        try {
          return resultSet.getString("IS_GENERATEDCOLUMN").equals("YES");
        } catch (SQLException e) {
          String errorMessage = "Cannot get property 'IS_GENERATEDCOLUMN'";
          LOGGER.error(errorMessage, e);
          throw new RuntimeException(errorMessage, e);
        }
      case "mssql":
        try {
          return resultSet.getString("SS_IS_COMPUTED").equals("1");
        } catch (SQLException e) {
          String errorMessage = "Cannot get property 'SS_IS_COMPUTED'";
          LOGGER.error(errorMessage, e);
          throw new RuntimeException(errorMessage, e);
        }
      case "postgresql":
        String columnDef;
        try {
          columnDef = resultSet.getString("COLUMN_DEF");
        } catch (SQLException e) {
          String errorMessage = "Cannot get property 'COLUMN_DEF'";
          LOGGER.error(errorMessage, e);
          throw new RuntimeException(errorMessage, e);
        }
        return (columnDef != null) && columnDef.contains("nextval(");
      default:
        return false;
    }
  }

  /**
   * Defines is field primary key
   *
   * @param primaryKeys Array of primary keys names
   * @param fieldName column field name
   */
  public static Boolean isPrimaryKey(ArrayList<String> primaryKeys, final String fieldName) {
    return primaryKeys.contains(fieldName);
  }

  /**
   * Defines is field primary key
   *
   * @param isPrimaryKey flag is column Primary Key
   * @param isNotNull flag is column Not Null
   */
  public static Boolean isRequired(final boolean isPrimaryKey, final boolean isNotNull,
      final boolean isAutoincrement, final boolean isCalculated) {
    return isPrimaryKey || (!isAutoincrement && !isCalculated && isNotNull);
  }

  public static boolean reboundIsEnabled(JsonObject config) {
    return ENABLED_REBOUND.equals(config.getString(PROPERTY_REBOUND, DISABLED_REBOUND));
  }
}
