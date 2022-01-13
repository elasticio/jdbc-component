package io.elastic.jdbc.providers;

import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeStampColumnNamesProvider implements SelectModelProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeStampColumnNamesProvider.class);

  @Override
  public JsonObject getSelectModel(JsonObject configuration) {
    LOGGER.info("Getting select model...");
    if (configuration.getString("tableName") == null || configuration.getString("tableName")
        .isEmpty()) {
      throw new RuntimeException("Table name is required");
    }
    String tableName = configuration.getString("tableName");
    String schemaName = null;
    if (tableName.contains(".")) {
      schemaName = tableName.split("\\.")[0];
      tableName = tableName.split("\\.")[1];
    }
    LOGGER.trace("Table name and SchemaName found");
    JsonObjectBuilder columnNames = Json.createObjectBuilder();
    try (Connection connection = Utils.getConnection(configuration)) {
      DatabaseMetaData dbMetaData = connection.getMetaData();
      try (ResultSet rs = dbMetaData.getColumns(null, schemaName, tableName, "%")) {
        while (rs.next()) {
          int sqlType = rs.getInt("DATA_TYPE");
          String name = rs.getString("COLUMN_NAME");
          String typeName = rs.getString("TYPE_NAME").toUpperCase();
          LOGGER.trace("Found field with name: {}, sqlType: {}, typeName: {}", name, sqlType,
              typeName);
          if (sqlType == Types.DATE || sqlType == Types.TIMESTAMP || typeName
              .contains("TIMESTAMP")) {
            LOGGER.debug("Found similar to timestamp field: {}", name);
            columnNames.add(name, name);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    JsonObject result = columnNames.build();
    if (result.size() == 0) {
      throw new RuntimeException("Can't find fields similar to timestamp");
    }
    return result;
  }
}
