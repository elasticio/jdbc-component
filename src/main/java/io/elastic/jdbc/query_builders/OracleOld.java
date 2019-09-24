package io.elastic.jdbc.query_builders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Deprecated
public class OracleOld extends QueryOld {
    public ResultSet execute(Connection connection) throws SQLException {
        validateQuery();
        String sql = "SELECT * FROM " +
                "(SELECT  b.*, rank() over (order by " + orderField + ") as rnk FROM " +
                tableName + " b) WHERE rnk BETWEEN ? AND ? " +
                "ORDER BY " + orderField;
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setInt(1, skipNumber);
        stmt.setInt(2, countNumber);
        return stmt.executeQuery();
    }
}
