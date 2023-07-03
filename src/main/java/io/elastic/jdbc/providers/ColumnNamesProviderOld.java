package io.elastic.jdbc.providers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.elastic.api.DynamicMetadataProvider;
import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.utils.SailorVersionsAdapter;
import io.elastic.jdbc.utils.UtilsOld;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Deprecated
public class ColumnNamesProviderOld implements DynamicMetadataProvider, SelectModelProvider {
    private static final Logger logger = LoggerFactory.getLogger(ColumnNamesProviderOld.class);

    public jakarta.json.JsonObject getSelectModel(jakarta.json.JsonObject configuration) {
        JsonObject result = new JsonObject();
        JsonObject properties = SailorVersionsAdapter.javaxToGson(getColumns(configuration));
        for (Map.Entry<String, JsonElement> entry : properties.entrySet()) {
            JsonObject field = entry.getValue().getAsJsonObject();
            result.addProperty(entry.getKey(), field.get("title").getAsString());
        }
        return SailorVersionsAdapter.gsonToJavax(result);
    }

    /**
     * Returns Columns list as metadata
     *
     * @param configuration
     * @return
     */

    public jakarta.json.JsonObject getMetaModel(jakarta.json.JsonObject configuration) {
        JsonObject result = new JsonObject();
        JsonObject inMetadata = new JsonObject();
        JsonObject properties = SailorVersionsAdapter.javaxToGson(getColumns(configuration));
        inMetadata.addProperty("type", "object");
        inMetadata.add("properties", properties);
        result.add("out", inMetadata);
        result.add("in", inMetadata);
        return SailorVersionsAdapter.gsonToJavax(result);
    }

    public jakarta.json.JsonObject getColumns(jakarta.json.JsonObject configuration) {
        if (SailorVersionsAdapter.javaxToGson(configuration).get("tableName") == null ||
                SailorVersionsAdapter.javaxToGson(configuration).get("tableName").getAsString().isEmpty()) {
            throw new RuntimeException("Table name is required");
        }
        String tableName = SailorVersionsAdapter.javaxToGson(configuration).get("tableName").getAsString();
        JsonObject properties = new JsonObject();
        Connection connection = null;
        ResultSet rs = null;
        String schemaName = null;
        Boolean isEmpty = true;
        try {
            connection = UtilsOld.getConnection(SailorVersionsAdapter.javaxToGson(configuration));
            DatabaseMetaData dbMetaData = connection.getMetaData();
            if (tableName.contains(".")) {
                schemaName = tableName.split("\\.")[0];
                tableName = tableName.split("\\.")[1];
            }
            rs = dbMetaData.getColumns(null, schemaName, tableName, "%");
            while (rs.next()) {
                JsonObject field = new JsonObject();
                String name = rs.getString("COLUMN_NAME");
                Boolean isRequired = rs.getInt("NULLABLE") == 0 && !rs.getString("IS_AUTOINCREMENT").equals("YES");
                field.addProperty("required", isRequired);
                field.addProperty("title", name);
                field.addProperty("type", convertType(rs.getInt("DATA_TYPE")));
                properties.add(name, field);
                isEmpty = false;
            }
            if (isEmpty) {
                properties.addProperty("", "no columns");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("Failed to close result set!");
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Failed to close connection!");
                }
            }
        }
        return SailorVersionsAdapter.gsonToJavax(properties);
    }

    /**
     * Converts JDBC column type name to js type according to http://db.apache.org/ojb/docu/guides/jdbc-types.html
     *
     * @param sqlType JDBC column type
     * @return
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
