package io.elastic.jdbc;

import io.elastic.api.DynamicMetadataProvider;
import io.elastic.api.SelectModelProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnNamesWithPrimaryKeyProvider implements DynamicMetadataProvider,
    SelectModelProvider {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ColumnNamesWithPrimaryKeyProvider.class);

  @Override
  public JsonObject getSelectModel(JsonObject configuration) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    JsonObject properties = getColumns(configuration);
    for (Map.Entry<String, JsonValue> entry : properties.entrySet()) {
      result.add(entry.getKey(), entry.getKey());
    }
    return result.build();
  }

  /**
   * Returns Columns list as metadata
   */

  @Override
  public JsonObject getMetaModel(JsonObject configuration) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    JsonObjectBuilder inMetadata = Json.createObjectBuilder();
    JsonObjectBuilder outMetadata = Json.createObjectBuilder();
    JsonObject properties = getColumns(configuration);
    inMetadata.add("type", "object").add("properties", properties);
    outMetadata.add("type", "object").add("properties", properties);
    result.add("out", outMetadata.build()).add("in", inMetadata.build());
    return result.build();
  }

  public JsonObject getColumns(JsonObject configuration) {
    if (configuration.getString("tableName") == null || configuration.getString("tableName")
        .isEmpty()) {
      throw new RuntimeException("Table name is required");
    }
    String tableName = configuration.getString("tableName");
    JsonObjectBuilder properties = Json.createObjectBuilder();
    Connection connection = null;
    ResultSet rs = null;
    ResultSet rsPrimaryKeys = null;
    String catalog = null;
    String schemaName = null;
    boolean isEmpty = true;
    Boolean isOracle = configuration.getString("dbEngine").equals("oracle");
    Boolean isMysql = configuration.getString("dbEngine").equals("mysql");
    try {
      connection = Utils.getConnection(configuration);
      DatabaseMetaData dbMetaData = connection.getMetaData();
      if (tableName.contains(".")) {
        schemaName = tableName.split("\\.")[0];
        tableName = tableName.split("\\.")[1];
      }
      if (isMysql) {
        catalog = configuration.getString("databaseName");
      }
      rsPrimaryKeys = dbMetaData
          .getPrimaryKeys(catalog, ((isOracle && !schemaName.isEmpty()) ? schemaName : null),
              tableName);
      rs = dbMetaData.getColumns(null, schemaName, tableName, "%");
      while (rs.next()) {
        JsonObjectBuilder field = Json.createObjectBuilder();
        String name = rs.getString("COLUMN_NAME");
        Boolean isRequired = false;
        while (rsPrimaryKeys.next()) {
          if (rsPrimaryKeys.getString("COLUMN_NAME").equals(name)) {
            isRequired = true;
            break;
          }
        }
        field.add("required", isRequired)
            .add("title", name)
            .add("type", convertType(rs.getInt("DATA_TYPE")));
        properties.add(name, field.build());
        isEmpty = false;
      }
      if (isEmpty) {
        properties.add("empty dataset", "no columns");
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close result set {}", e);
        }
      }
      if (rsPrimaryKeys != null) {
        try {
          rsPrimaryKeys.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close result set {}", e);
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close connection {}", e);
        }
      }
    }
    return properties.build();
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
