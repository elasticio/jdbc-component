package io.elastic.jdbc.QueryBuilders;

import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQL extends Query {
    private static final Logger logger = LoggerFactory.getLogger(MySQL.class);
    public ResultSet executeSelectQuery(Connection connection, String sqlQuery, JsonObject body) throws SQLException {
        StringBuilder sql = new StringBuilder(sqlQuery);
        PreparedStatement stmt = connection.prepareStatement(sql.toString());
        int i = 1;
        for (Entry<String, JsonValue> entry : body.entrySet()) {
            Utils.setStatementParam(stmt, i, entry.getKey(), (entry.getValue() != null) ? body : null);
            i++;
        }
        return stmt.executeQuery();
    }

    public ResultSet executeSelectTrigger(Connection connection, String sqlQuery) throws SQLException {
        StringBuilder sql = new StringBuilder(sqlQuery);
        PreparedStatement stmt = connection.prepareStatement(sql.toString());
        if(pollingValue != null) {
            stmt.setTimestamp(1, pollingValue);
        }
        return stmt.executeQuery();
    }

    public ResultSet executePolling(Connection connection) throws SQLException {
        validateQuery();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(tableName);
        sql.append(" WHERE ");
        sql.append(pollingField);
        sql.append(" > ?");
        if (orderField != null) {
            sql.append(" ORDER BY ").append(orderField);
        }
        sql.append(" ASC LIMIT ? OFFSET ?");

        PreparedStatement stmt = connection.prepareStatement(sql.toString());
        stmt.setTimestamp(1, pollingValue);
        stmt.setInt(2, countNumber);
        stmt.setInt(3, skipNumber);
        return stmt.executeQuery();
    }

    public ResultSet executeLookup(Connection connection, JsonObject body) throws SQLException {
        validateQuery();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(tableName);
        sql.append(" WHERE ");
        sql.append(lookupField);
        sql.append(" = ?");
        sql.append(" ORDER BY ").append(lookupField);
        sql.append(" ASC LIMIT ? OFFSET ?");

        PreparedStatement stmt = connection.prepareStatement(sql.toString());
        for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
            Utils.setStatementParam(stmt, 1, entry.getKey(), body);
        }
        stmt.setInt(2, countNumber);
        stmt.setInt(3, skipNumber);
        return stmt.executeQuery();
    }

    public int executeDelete(Connection connection, JsonObject body) throws SQLException {
        String sql = "DELETE" +
                " FROM " + tableName +
                " WHERE " + lookupField + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1, lookupValue);
        return stmt.executeUpdate();
    }

    public boolean executeRecordExists(Connection connection, JsonObject body) throws SQLException {
        validateQuery();
        String sql = "SELECT COUNT(*)" +
                " FROM " + tableName +
                " WHERE " + lookupField + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        Utils.setStatementParam(stmt, 1, lookupField, body);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        return rs.getInt(1) > 0;
    }

    public void executeInsert(Connection connection, String tableName, JsonObject body)
            throws SQLException {
        validateQuery();
        StringBuilder keys = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
            if (keys.length() > 0) {
                keys.append(",");
            }
            keys.append(entry.getKey());
            if (values.length() > 0) {
                values.append(",");
            }
            values.append("?");
        }
        String sql = "INSERT INTO " + tableName +
                " (" + keys.toString() + ")" +
                " VALUES (" + values.toString() + ")";
        PreparedStatement stmt = connection.prepareStatement(sql);
        int i = 1;
        for (String key : body.keySet()) {
            Utils.setStatementParam(stmt, i, key, body);
            i++;
        }
        stmt.execute();
    }

    public void executeUpdate(Connection connection, String tableName, String idColumn,
                              String idValue, JsonObject body) throws SQLException {
        validateQuery();
        StringBuilder setString = new StringBuilder();
        for (Map.Entry<String, JsonValue> entry : body.entrySet()) {
            if (setString.length() > 0) {
                setString.append(",");
            }
            setString.append(entry.getKey()).append(" = ?");
        }
        String sql = "UPDATE " + tableName +
                " SET " + setString.toString() +
                " WHERE " + idColumn + " = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        int i = 1;
        for (String key : body.keySet()) {
            Utils.setStatementParam(stmt, i, key, body);
            i++;
        }
        Utils.setStatementParam(stmt, i, idColumn, body);
        stmt.execute();
    }

}