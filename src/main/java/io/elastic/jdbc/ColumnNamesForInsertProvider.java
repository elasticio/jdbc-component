package io.elastic.jdbc;

import io.elastic.api.DynamicMetadataProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnNamesForInsertProvider implements DynamicMetadataProvider {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ColumnNamesForInsertProvider.class);


  /**
   * Returns Columns list as metadata
   */

  @Override
  public JsonObject getMetaModel(JsonObject configuration) {
    String tableName = getTableName(configuration);
    final String dbEngine = configuration.getString("dbEngine");
    Connection connection = null;
    ResultSet resultSet = null;
    ResultSet resultSetPK = null;

    JsonObjectBuilder propertiesIn = Json.createObjectBuilder();
    JsonObjectBuilder propertiesOut = Json.createObjectBuilder();
    boolean isEmpty = true;
    try {
      connection = Utils.getConnection(configuration);
      DatabaseMetaData dbMetaData = connection.getMetaData();
      final String schemaNamePattern = getSchemaNamePattern(tableName);
      final String tableNamePattern = getTableNamePattern(tableName);
      final String catalog =
          dbEngine.equals("mysql") ? configuration.getString("databaseName") : null;
      resultSetPK = dbMetaData.getPrimaryKeys(catalog, schemaNamePattern, tableNamePattern);
      ArrayList<String> primaryKeys = getPrimaryKeysNames(resultSetPK);
      resultSet = dbMetaData.getColumns(null, schemaNamePattern, tableNamePattern, "%");
      while (resultSet.next()) {
        final String fieldName = resultSet.getString("COLUMN_NAME");
        final String type = convertType(resultSet.getInt("DATA_TYPE"));
        final boolean isPrimaryKey = isPrimaryKey(primaryKeys, fieldName);
        final boolean isNotNull = isNotNull(resultSet);
        final boolean isAutoincrement = isAutoincrement(resultSet, dbEngine);
        final boolean isCalculated = isCalculated(resultSet, dbEngine);
        final boolean isRequired = isRequired(isPrimaryKey, isNotNull, isAutoincrement,
            isCalculated);
        JsonObject field = Json.createObjectBuilder()
            .add("required", isRequired)
            .add("title", fieldName)
            .add("type", type)
            .build();
        if (!isAutoincrement && !isCalculated) {
          propertiesIn.add(fieldName, field);
        }
        propertiesOut.add(fieldName, field);
        isEmpty = false;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close result set", e);
        }
      }
      if (resultSetPK != null) {
        try {
          resultSetPK.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close result set PK", e);
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close connection", e);
        }
      }
    }
    if (isEmpty) {
      LOGGER.error("The table doesn't contain columns for inserting values");
      throw new RuntimeException("The table doesn't contain columns for inserting values");
    }
    JsonObject inMetadata = Json.createObjectBuilder()
        .add("type", "object")
        .add("properties", propertiesIn.build())
        .build();
    JsonObject outMetadata = Json.createObjectBuilder()
        .add("type", "object")
        .add("properties", propertiesOut.build())
        .build();
    return Json.createObjectBuilder()
        .add("out", outMetadata)
        .add("in", inMetadata)
        .build();
  }

  /**
   * Converts JDBC column type name to js type according to http://db.apache.org/ojb/docu/guides/jdbc-types.html
   *
   * @param sqlType JDBC column type
   */
  private String convertType(Integer sqlType) {
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

  private String getTableName(JsonObject config) {
    final String tableName = config.getString("tableName");

    if (tableName == null || tableName.toString().isEmpty()) {
      throw new RuntimeException("Table name is required");
    }
    return tableName;
  }

  private String getTableNamePattern(String tableName) {
    if (tableName.contains(".")) {
      tableName = tableName.split("\\.")[1];
    }
    return tableName;
  }

  private String getSchemaNamePattern(String tableName) {
    String schemaName = null;
    if (tableName.contains(".")) {
      schemaName = tableName.split("\\.")[0];
    }
    return schemaName;
  }

  private Boolean isAutoincrement(ResultSet resultSet, final String dbEngine) throws SQLException {
    boolean isAutoincrement = false;
    if (!dbEngine.equals("oracle")) {
      isAutoincrement = resultSet.getString("IS_AUTOINCREMENT").equals("YES");
    }
    return isAutoincrement;
  }

  private Boolean isNotNull(ResultSet resultSet) throws SQLException {
    return resultSet.getString("IS_NULLABLE").equals("NO");
  }

  private Boolean isCalculated(ResultSet resultSet, final String dbEngine)
      throws SQLException {
    switch (dbEngine) {
      case "mysql":
        return resultSet.getString("IS_GENERATEDCOLUMN").equals("YES");
      case "mssql":
        return resultSet.getString("SS_IS_COMPUTED").equals("1");
      case "postgresql":
        String columnDef = resultSet.getString("COLUMN_DEF");
        return (columnDef != null) && columnDef.contains("nextval(");
      default:
        return false;
    }
  }

  private Boolean isPrimaryKey(ArrayList<String> primaryKeys, final String fieldName) {
    return primaryKeys.contains(fieldName);
  }

  private ArrayList<String> getPrimaryKeysNames(ResultSet resultSet) throws SQLException {
    ArrayList<String> primaryKeys = new ArrayList<>();
    while (resultSet.next()) {
      primaryKeys.add(resultSet.getString("COLUMN_NAME"));
    }
    return primaryKeys;
  }

  private Boolean isRequired(final boolean isPrimaryKey, final boolean isNotNull,
      final boolean isAutoincrement, final boolean isCalculated) {
    return isPrimaryKey || (!isAutoincrement && !isCalculated && isNotNull);
  }
}
