package io.elastic.jdbc.actions;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.utils.EnginesOld;
import io.elastic.jdbc.utils.SailorVersionsAdapter;
import io.elastic.jdbc.utils.UtilsOld;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class CreateOrUpdateRecord implements Module {
    private static final Logger logger = LoggerFactory.getLogger(CreateOrUpdateRecord.class);

    private Connection connection = null;
    private Map<String, String> columnTypes = null;
    private boolean isOracle = false;

    @Override
    public void execute(ExecutionParameters parameters) {
        final JsonObject configuration = SailorVersionsAdapter.javaxToGson(parameters.getConfiguration());
        final JsonObject body = SailorVersionsAdapter.javaxToGson(parameters.getMessage().getBody());
        if (!configuration.has("tableName") || configuration.get("tableName").isJsonNull() || configuration.get("tableName").getAsString().isEmpty()) {
            throw new RuntimeException("Table name is required field");
        }
        if (!configuration.has("idColumn") || configuration.get("idColumn").isJsonNull() || configuration.get("idColumn").getAsString().isEmpty()) {
            throw new RuntimeException("ID column is required field");
        }
        String tableName = configuration.get("tableName").getAsString();
        String idColumn = configuration.get("idColumn").getAsString();
        String idColumnValue = null;
        if (!(!body.has(idColumn) || body.get(idColumn).isJsonNull() || body.get(idColumn).getAsString().isEmpty())) {
            idColumnValue = body.get(idColumn).getAsString();
        }
        logger.trace("ID column value: {}", idColumnValue);
        String db = configuration.get(UtilsOld.CFG_DB_ENGINE).getAsString();
        isOracle = db.equals(EnginesOld.ORACLE.name().toLowerCase());
        try {
            connection = UtilsOld.getConnection(configuration);
            columnTypes = getColumnTypes(tableName);
            logger.debug("Detected column types: " + columnTypes);
            if (recordExists(tableName, idColumn, idColumnValue)) {
                makeUpdate(tableName, idColumn, idColumnValue, body);
            } else {
                makeInsert(tableName, body);
            }
            parameters.getEventEmitter().emitData(new Message.Builder().body(SailorVersionsAdapter.gsonToJavax(body)).build());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e.toString());
                }
            }
        }
    }

    private Map<String, String> getColumnTypes(String tableName) {
        DatabaseMetaData md;
        ResultSet rs = null;
        Map<String, String> columnTypes = new HashMap<String, String>();
        String schemaName = null;
        try {
            md = connection.getMetaData();
            if (tableName.contains(".")) {
                schemaName = tableName.split("\\.")[0];
                tableName = tableName.split("\\.")[1];
            }
            if (isOracle) tableName = tableName.toUpperCase();
            rs = md.getColumns(null, schemaName, tableName, "%");
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME").toLowerCase();
                String type = detectColumnType(rs.getInt("DATA_TYPE"));
                columnTypes.put(name, type);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                    logger.error(e.toString());
                }
            }
        }
        return columnTypes;
    }

    private String detectColumnType(Integer sqlType) {
        if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL || sqlType == Types.TINYINT
                || sqlType == Types.SMALLINT || sqlType == Types.INTEGER || sqlType == Types.BIGINT
                || sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
            return "number";
        }
        if (sqlType == Types.TIMESTAMP) {
            return "timestamp";
        }
        if (sqlType == Types.DATE) {
            return "date";
        }
        if (sqlType == Types.BIT || sqlType == Types.BOOLEAN) {
            return "boolean";
        }
        return "string";
    }

    private String getColumnType(String columnName) {
        return columnTypes.get(columnName.toLowerCase());
    }

    private boolean isNumeric(String columnName) {
        String type = getColumnType(columnName);
        return type != null && type.equals("number");
    }

    private boolean isTimestamp(String columnName) {
        String type = getColumnType(columnName);
        return type != null && type.equals("timestamp");
    }

    private boolean isDate(String columnName) {
        String type = getColumnType(columnName);
        return type != null && type.equals("date");
    }

    private void setStatementParam(PreparedStatement statement, int paramNumber, String colName, String colValue) throws SQLException {
        if (isNumeric(colName)) {
        	statement.setBigDecimal(paramNumber, new BigDecimal(colValue));
        } else if (isTimestamp(colName)) {
            statement.setTimestamp(paramNumber, Timestamp.valueOf(colValue));
        } else if (isDate(colName)) {
            statement.setDate(paramNumber, Date.valueOf(colValue));
        } else {
            statement.setString(paramNumber, colValue);
        }
    }

    private boolean recordExists(String tableName, String idColumn, String idValue) throws SQLException {
        String query = "SELECT COUNT(*) FROM " + tableName + " WHERE " + idColumn + " = ?";
        PreparedStatement statement = connection.prepareStatement(query);
        setStatementParam(statement, 1, idColumn, idValue);
        logger.trace("{}",statement);
        ResultSet rs = statement.executeQuery();
        rs.next();
        return rs.getInt(1) > 0;
    }

    private void makeInsert(String tableName, JsonObject body) throws SQLException {
        StringBuilder keys = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (Map.Entry<String, JsonElement> entry : body.entrySet()) {
            if (keys.length() > 0) keys.append(",");
            keys.append(entry.getKey());
            if (values.length() > 0)  values.append(",");
            values.append("?");
        }
        String sql = "INSERT INTO " + tableName + " (" + keys.toString() + ") VALUES (" + values.toString() + ")";
        PreparedStatement statement = connection.prepareStatement(sql);
        int i = 1;
        for (Map.Entry<String, JsonElement> entry : body.entrySet()) {
            setStatementParam(statement, i, entry.getKey(), entry.getValue().getAsString());
            i++;
        }
        logger.trace("{}",statement);
        statement.execute();
    }

    private void makeUpdate(String tableName, String idColumn, String idValue, JsonObject body) throws SQLException {
        StringBuilder setString = new StringBuilder();
        for (Map.Entry<String, JsonElement> entry : body.entrySet()) {
            if (setString.length() > 0) setString.append(",");
            setString.append(entry.getKey()).append(" = ?");
        }
        String sql = "UPDATE " + tableName + " SET " + setString.toString() + " WHERE " + idColumn + " = ?";
        PreparedStatement statement = connection.prepareStatement(sql);
        int i = 1;
        for (Map.Entry<String, JsonElement> entry : body.entrySet()) {
            setStatementParam(statement, i, entry.getKey(), entry.getValue().getAsString());
            i++;
        }
        setStatementParam(statement, i, idColumn, idValue);
        statement.execute();
    }
}