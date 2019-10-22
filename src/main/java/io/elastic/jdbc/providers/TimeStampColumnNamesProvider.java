package io.elastic.jdbc.providers;

import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.sql.*;

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
        LOGGER.info("Table name: {}, SchemaName: {}", tableName, schemaName);
        JsonObjectBuilder columnNames = Json.createObjectBuilder();
        try (Connection connection = Utils.getConnection(configuration)) {
            DatabaseMetaData dbMetaData = connection.getMetaData();
            try (ResultSet rs = dbMetaData.getColumns(null, schemaName, tableName, "%")) {
                while (rs.next()) {
                    int sqlType = rs.getInt("DATA_TYPE");
                    String name = rs.getString("COLUMN_NAME");
                    LOGGER.debug("Found field with name: {} and sqlType: {}", name, sqlType);
                    if (sqlType == Types.DATE || sqlType == Types.TIMESTAMP) {
                        LOGGER.info("Found similar to timestamp field: {}", name);
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
