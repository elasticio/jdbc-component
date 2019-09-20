package io.elastic.jdbc;

import io.elastic.api.DynamicMetadataProvider;
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
    final String dbEngine = Utils.getDbEngine(configuration);
    final boolean isOracle = dbEngine.equals(Engines.ORACLE.name().toLowerCase());
    final boolean isMySql = dbEngine.equals(Engines.MYSQL.name().toLowerCase());
    final String tableName = Utils.getTableName(configuration, isOracle);
    LOGGER.info("Getting metadata for table: '{}'...", tableName);
    Connection connection = null;
    ResultSet resultSet = null;
    ResultSet resultSetPK = null;

    JsonObjectBuilder propertiesIn = Json.createObjectBuilder();
    JsonObjectBuilder propertiesOut = Json.createObjectBuilder();
    boolean isEmpty = true;
    try {
      connection = Utils.getConnection(configuration);
      DatabaseMetaData dbMetaData = connection.getMetaData();
      final String schemaNamePattern = Utils.getSchemaNamePattern(tableName);
      final String tableNamePattern = Utils.getTableNamePattern(tableName);
      final String catalog = isMySql ? configuration.getString("databaseName") : null;
      LOGGER.debug("Getting primary keys...");
      resultSetPK = dbMetaData.getPrimaryKeys(catalog, schemaNamePattern, tableNamePattern);
      ArrayList<String> primaryKeysNames = Utils.getColumnNames(resultSetPK);
      LOGGER.debug("Found primary key(s): '{}'", primaryKeysNames);
      resultSet = dbMetaData.getColumns(null, schemaNamePattern, tableNamePattern, "%");
      while (resultSet.next()) {
        final String fieldName = resultSet.getString("COLUMN_NAME");
        final String type = Utils.convertType(resultSet.getInt("DATA_TYPE"));
        final boolean isPrimaryKey = Utils.isPrimaryKey(primaryKeysNames, fieldName);
        final boolean isNotNull = Utils.isNotNull(resultSet);
        final boolean isAutoincrement = Utils.isAutoincrement(resultSet, isOracle);
        final boolean isCalculated = Utils.isCalculated(resultSet, dbEngine);
        LOGGER
            .debug("Field '{}': isPrimaryKey={}, isNotNull={}, isAutoincrement={}, isCalculated={}",
                fieldName, isPrimaryKey, isNotNull, isAutoincrement,
                isCalculated);
        final boolean isRequired = Utils.isRequired(isPrimaryKey, isNotNull, isAutoincrement,
            isCalculated);
        JsonObject field = Json.createObjectBuilder()
            .add("required", isRequired)
            .add("title", fieldName)
            .add("type", type)
            .build();
        LOGGER.debug("Description of field '{}': {}", fieldName, field);
        if (!isAutoincrement && !isCalculated) {
          propertiesIn.add(fieldName, field);
        }
        propertiesOut.add(fieldName, field);
        isEmpty = false;
      }
      if (isEmpty) {
        LOGGER.error("The table doesn't contain columns for inserting values");
        throw new RuntimeException("The table doesn't contain columns for inserting values");
      }
      JsonObject inMetadata = Json.createObjectBuilder()
          .add("type", "object")
          .add("properties", propertiesIn.build())
          .build();
      LOGGER.info("Generated input metadata {}", inMetadata);
      JsonObject outMetadata = Json.createObjectBuilder()
          .add("type", "object")
          .add("properties", propertiesOut.build())
          .build();
      LOGGER.info("Generated output metadata {}", outMetadata);
      return Json.createObjectBuilder()
          .add("out", outMetadata)
          .add("in", inMetadata)
          .build();
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
  }
}
