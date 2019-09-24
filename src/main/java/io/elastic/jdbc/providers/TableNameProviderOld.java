package io.elastic.jdbc.providers;

import com.google.gson.JsonObject;
import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.utils.SailorVersionsAdapter;
import io.elastic.jdbc.utils.UtilsOld;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Deprecated
public class TableNameProviderOld implements SelectModelProvider {

    private static final Logger logger = LoggerFactory.getLogger(TableNameProviderOld.class);

    public javax.json.JsonObject getSelectModel(javax.json.JsonObject configuration) {
        logger.info("About to retrieve table name");

        JsonObject result = new JsonObject();
        Connection connection = null;
        ResultSet rs = null;

        try {
            connection = UtilsOld.getConnection(SailorVersionsAdapter.javaxToGson(configuration));
            logger.info("Successfully connected to DB");

            // get metadata
            DatabaseMetaData md = connection.getMetaData();

            // get table names
            String[] types = {"TABLE", "VIEW"};
            rs = md.getTables(null, "%", "%", types);

            // put table names to result
            String tableName;
            String schemaName;
            Boolean isEmpty = true;

            while (rs.next()) {
                tableName = rs.getString("TABLE_NAME");
                schemaName = rs.getString("TABLE_SCHEM");
                if (SailorVersionsAdapter.javaxToGson(configuration).get("dbEngine").getAsString().toLowerCase().equals("oracle")
                        && isOracleServiceSchema(schemaName)) {
                    continue;
                }
                if (schemaName != null) {
                    tableName = schemaName + "." + tableName;
                }
                result.addProperty(tableName, tableName);
                isEmpty = false;
            }
            if (isEmpty) {
                result.addProperty("", "no tables");
            }
        } catch (SQLException e) {
            logger.error("Unexpected error", e);
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error(e.toString());
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e.toString());
                }
            }
        }
        return SailorVersionsAdapter.gsonToJavax(result);
    }

    private boolean isOracleServiceSchema(String schema) {
        List<String> schemas = Arrays.asList("APPQOSSYS", "CTXSYS", "DBSNMP", "DIP", "OUTLN", "RDSADMIN", "SYS", "SYSTEM");
        return schemas.indexOf(schema) > -1;
    }
}
