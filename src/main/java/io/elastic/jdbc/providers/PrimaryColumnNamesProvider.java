package io.elastic.jdbc.providers;

import io.elastic.api.DynamicMetadataProvider;
import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimaryColumnNamesProvider implements DynamicMetadataProvider, SelectModelProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryColumnNamesProvider.class);

  public JsonObject getSelectModel(JsonObject configuration) {
    JsonObject result = Json.createObjectBuilder().build();
    JsonObject properties = getPrimaryColumns(configuration);
    for (Map.Entry<String, JsonValue> entry : properties.entrySet()) {
      JsonValue field = entry.getValue();
      result = Json.createObjectBuilder().add(entry.getKey(), field.toString()).build();
    }
    return result;
  }

  /**
   * Returns Columns list as metadata
   */

  public JsonObject getMetaModel(JsonObject configuration) {
    JsonObject result = Json.createObjectBuilder().build();
    JsonObject inMetadata = Json.createObjectBuilder().build();
    JsonObject properties = getPrimaryColumns(configuration);
    inMetadata = Json.createObjectBuilder().add("type", "object")
        .add("properties", properties).build();
    result = Json.createObjectBuilder().add("out", inMetadata)
        .add("in", inMetadata).build();
    return result;
  }

  public JsonObject getPrimaryColumns(JsonObject configuration) {
    if (configuration.getString("tableName") == null || configuration.getString("tableName")
        .isEmpty()) {
      throw new RuntimeException("Table name is required");
    }
    String tableName = configuration.getString("tableName");
    JsonObject properties = Json.createObjectBuilder().build();
    Connection connection = null;
    ResultSet rs = null;
    String catalog = null;
    String schemaName = null;
    Boolean isEmpty = true;
    Boolean isOracle = configuration.getString("dbEngine").equals("oracle");
    Boolean isMssql = configuration.getString("dbEngine").equals("mssql");
    Boolean isMysql = configuration.getString("dbEngine").equals("mysql");
    List<String> primaryKeys = new ArrayList();
    try {
      connection = Utils.getConnection(configuration);
      DatabaseMetaData dbMetaData = connection.getMetaData();
      if (isMysql) {
        catalog = configuration.getString("databaseName");
      }
      if (tableName.contains(".")) {
        schemaName =
            (isOracle) ? tableName.split("\\.")[0].toUpperCase() : tableName.split("\\.")[0];
        tableName =
            (isOracle) ? tableName.split("\\.")[1].toUpperCase() : tableName.split("\\.")[1];
      }
      rs = dbMetaData
          .getPrimaryKeys(catalog, ((isOracle && !schemaName.isEmpty()) ? schemaName : null),
              tableName);
      while (rs.next()) {
        primaryKeys.add(rs.getString("COLUMN_NAME"));
        LOGGER.trace("Primary Key: {}", rs.getString("COLUMN_NAME"));
      }
      rs = dbMetaData
          .getColumns(null, ((isOracle && !schemaName.isEmpty()) ? schemaName : null), tableName,
              "%");
      while (rs.next()) {
        if (primaryKeys.contains(rs.getString("COLUMN_NAME"))) {
          JsonObjectBuilder field = Json.createObjectBuilder();
          String name = rs.getString("COLUMN_NAME");
          Boolean isRequired = false;
          if (isMssql) {
            String isAutoincrement =
                (rs.getString("IS_AUTOINCREMENT") != null) ? rs.getString("IS_AUTOINCREMENT") : "";
            Integer isNullable = (rs.getObject("NULLABLE") != null) ? rs.getInt("NULLABLE") : 1;
            isRequired = isNullable == 0 && !isAutoincrement.equals("YES");
          } else {
            isRequired = true;
          }
          field.add("required", isRequired)
              .add("title", name)
              .add("type", convertType(rs.getInt("DATA_TYPE")));
          properties = Json.createObjectBuilder().add(name, field.build()).build();
          isEmpty = false;
        }
      }
      if (isEmpty) {
        LOGGER.info("Empty PK list - no primary keys");
        throw new IllegalStateException("No Primary Keys");
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close result set", e);
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
    return properties;
  }

  /**
   * Converts JDBC column type name to js type according to http://db.apache.org/ojb/docu/guides/jdbc-types.html
   *
   * @param sqlType JDBC column type
   * @url http://db.apache.org/ojb/docu/guides/jdbc-types.html
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
}
