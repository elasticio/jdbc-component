package io.elastic.jdbc.triggers;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Function;
import io.elastic.jdbc.query_builders.QueryOld;
import io.elastic.jdbc.utils.SailorVersionsAdapter;
import io.elastic.jdbc.utils.TriggerQueryFactory;
import io.elastic.jdbc.utils.UtilsOld;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class SelectTriggerOld implements Function {
    private static final Logger logger = LoggerFactory.getLogger(SelectTriggerOld.class);
    public static final String PROPERTY_TABLE_NAME = "tableName";
    public static final String PROPERTY_ORDER_FIELD = "orderField";

    @Override
    public final void execute(ExecutionParameters parameters) {

        logger.info("About to execute select action");
        JsonArray rows = new JsonArray();
        JsonObject config = SailorVersionsAdapter.javaxToGson(parameters.getConfiguration());
        Connection connection = UtilsOld.getConnection(config);
        checkConfig(config);

        String dbEngine = config.get("dbEngine").getAsString();
        String tableName = config.get(PROPERTY_TABLE_NAME).getAsString();
        String orderField = config.get("orderField").getAsString();

        JsonObject snapshot = SailorVersionsAdapter.javaxToGson(parameters.getSnapshot());
        Integer skipNumber = 0;
        if (snapshot.get("skipNumber") != null)
            skipNumber = snapshot.get("skipNumber").getAsInt();

        if (snapshot.get(PROPERTY_TABLE_NAME) != null && !snapshot.get(PROPERTY_TABLE_NAME).getAsString().equals(tableName)) {
            skipNumber = 0;
        }
        ResultSet rs = null;
        logger.info("Executing select action");
        try {
            TriggerQueryFactory queryFactory = new TriggerQueryFactory();
            QueryOld queryOld = queryFactory.getQuery(dbEngine);
            queryOld.from(tableName).skip(skipNumber).orderBy(orderField);
            rs = queryOld.execute(connection);
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                JsonObject row = new JsonObject();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String rsString = rs.getString(metaData.getColumnName(i));
                    if (metaData.getColumnType(i) == Types.TIMESTAMP) {
                        rsString = rs.getTimestamp(metaData.getColumnName(i)).toString();
                    } else if (metaData.getColumnType(i) == Types.DATE) {
                        rsString = rs.getDate(metaData.getColumnName(i)).toString();
                    } else if (metaData.getColumnType(i) == Types.TIME) {
                        rsString = rs.getTime(metaData.getColumnName(i)).toString();
                    }
                    row.addProperty(metaData.getColumnName(i), rsString);
                }
                rows.add(row);
                logger.info("Emitting data");
                parameters.getEventEmitter().emitData(new Message.Builder().body(SailorVersionsAdapter.gsonToJavax(row)).build());
            }
            snapshot.addProperty("skipNumber", skipNumber + rows.size());
            snapshot.addProperty(PROPERTY_TABLE_NAME, tableName);
            logger.info("Emitting new snapshot");
            parameters.getEventEmitter().emitSnapshot(SailorVersionsAdapter.gsonToJavax(snapshot));
        } catch (SQLException e) {
            logger.error("Failed to make request");
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("Failed to close result set", e.toString());
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Failed to close connection", e.toString());
                }
            }
        }
    }

    private void checkConfig(JsonObject config) {
        final JsonElement tableName = config.get(PROPERTY_TABLE_NAME);
        final JsonElement orderField = config.get(PROPERTY_ORDER_FIELD);

        if (tableName == null || tableName.isJsonNull() || tableName.getAsString().isEmpty()) {
            throw new RuntimeException("Table name is required field");
        }

        if (orderField == null || orderField.isJsonNull() || orderField.getAsString().isEmpty()) {
            throw new RuntimeException("Order column is required field");
        }
    }
}
