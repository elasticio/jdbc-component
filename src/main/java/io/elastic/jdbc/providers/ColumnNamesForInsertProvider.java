package io.elastic.jdbc.providers;

import io.elastic.api.DynamicMetadataProvider;
import io.elastic.jdbc.Utils.Engines;
import io.elastic.jdbc.Utils.Utils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnNamesForInsertProvider implements DynamicMetadataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnNamesForInsertProvider.class);

  /**
   * Returns Columns list as metadata
   */

  @Override
  public JsonObject getMetaModel(JsonObject configuration) {
    LOGGER.info("Getting metadata...");
    JsonObject inMetadata = getInputMetaData(configuration);
    LOGGER.info("Generated input metadata {}", inMetadata);
    JsonObject outMetadata = getOutputMetaData();
    LOGGER.info("Generated output metadata {}", outMetadata);
    return Json.createObjectBuilder()
        .add("out", outMetadata)
        .add("in", inMetadata)
        .build();
  }

  private JsonObject getOutputMetaData() {
    LOGGER.info("Getting output metadata...");
    JsonObject result = Json.createObjectBuilder()
        .add("required", true)
        .add("title", "result")
        .add("type", "boolean")
        .build();
    return Json.createObjectBuilder()
        .add("type", "object")
        .add("properties", Json.createObjectBuilder()
            .add("result", result)
            .build())
        .build();
  }

  private JsonObject getInputMetaData(JsonObject configuration) {
    LOGGER.info("Getting input metadata...");
    final String dbEngine = Utils.getDbEngine(configuration);
    final boolean isOracle = dbEngine.equals(Engines.ORACLE.name().toLowerCase());
    final String tableName = Utils.getTableName(configuration, isOracle);
    JsonObjectBuilder propertiesIn = Json.createObjectBuilder();
    boolean isEmpty = true;

    try (Connection connection = Utils.getConnection(configuration)) {
      DatabaseMetaData dbMetaData;
      try {
        LOGGER.info("Getting DatabaseMetaData for table: '{}'...", tableName);
        dbMetaData = connection.getMetaData();
      } catch (SQLException e) {
        LOGGER.error("Failed while getting DatabaseMetaData. Error: " + e.getMessage());
        throw new RuntimeException(e);
      }

      final String schemaNamePattern = Utils.getSchemaNamePattern(tableName);
      final String tableNamePattern = Utils.getTableNamePattern(tableName);

      try (ResultSet resultSet = dbMetaData
          .getColumns(null, schemaNamePattern, tableNamePattern, "%")) {
        LOGGER.info("Getting primary key names...");
        final boolean isMySql = dbEngine.equals(Engines.MYSQL.name().toLowerCase());
        final String catalog = isMySql ? configuration.getString("databaseName") : null;
        ArrayList<String> primaryKeysNames = Utils
            .getPrimaryKeyNames(catalog, schemaNamePattern, tableNamePattern, dbMetaData);
        LOGGER.info("Found primary key name(s): '{}'", primaryKeysNames);

        LOGGER.info("Starting processing columns...");
        while (resultSet.next()) {
          final String fieldName = resultSet.getString("COLUMN_NAME");
          final int sqlDataType = resultSet.getInt("DATA_TYPE");
          final String fieldType = Utils.convertType(sqlDataType);
          LOGGER.info("Found column: name={}, type={}", fieldName, fieldType);

          final boolean isPrimaryKey = Utils.isPrimaryKey(primaryKeysNames, fieldName);
          final boolean isNotNull = Utils.isNotNull(resultSet);
          final boolean isAutoincrement = Utils.isAutoincrement(resultSet, isOracle);
          final boolean isCalculated = Utils.isCalculated(resultSet, dbEngine);
          LOGGER
              .info(
                  "Field '{}': isPrimaryKey={}, isNotNull={}, isAutoincrement={}, isCalculated={}",
                  fieldName, isPrimaryKey, isNotNull, isAutoincrement,
                  isCalculated);
          final boolean isRequired = Utils.isRequired(isPrimaryKey, isNotNull, isAutoincrement,
              isCalculated);
          if (!isAutoincrement && !isCalculated) {
            JsonObject field = Json.createObjectBuilder()
                .add("required", isRequired)
                .add("title", fieldName)
                .add("type", fieldType)
                .build();
            LOGGER.debug("Field description '{}': {}", fieldName, field);
            propertiesIn.add(fieldName, field);
            isEmpty = false;
          }
        }
      } catch (SQLException e) {
        LOGGER.error("Failed while processing ResultSet. Error: " + e.getMessage());
        throw new RuntimeException(e);
      }
    } catch (SQLException e) {
      LOGGER.error("Failed while connecting. Error: " + e.getMessage());
      throw new RuntimeException(e);
    }

    if (isEmpty) {
      String errorMessage =
          "Table '" + tableName + "' doesn't contain columns for inserting values";
      LOGGER.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      return Json.createObjectBuilder()
          .add("type", "object")
          .add("properties", propertiesIn.build())
          .build();
    }
  }
}
