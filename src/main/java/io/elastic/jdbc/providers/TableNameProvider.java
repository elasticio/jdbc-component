package io.elastic.jdbc.providers;

import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableNameProvider implements SelectModelProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableNameProvider.class);

  @Override
  public JsonObject getSelectModel(JsonObject configuration) {
    LOGGER.info("About to retrieve table name");

    JsonObjectBuilder result = Json.createObjectBuilder();
    Connection connection = null;
    ResultSet rs = null;

    try {
      connection = Utils.getConnection(configuration);
      LOGGER.info("Successfully connected to DB");

      // get metadata
      DatabaseMetaData md = connection.getMetaData();

      // get table names
      String[] types = {"TABLE", "VIEW"};
      rs = md.getTables(null, "%", "%", types);

      // put table names to result
      String tableName;
      String schemaName;
      boolean isEmpty = true;

      while (rs.next()) {
        tableName = rs.getString("TABLE_NAME");
        schemaName = rs.getString("TABLE_SCHEM");
        if (configuration.getString("dbEngine").toLowerCase().equals("oracle")
            && isOracleServiceSchema(schemaName)) {
          continue;
        }
        if (schemaName != null) {
          tableName = schemaName + "." + tableName;
        }
        result.add(tableName, tableName);
        isEmpty = false;
      }
      if (isEmpty) {
        result.add("empty dataset", "no tables");
      }
    } catch (SQLException e) {
      LOGGER.error("Unexpected error");
      throw new RuntimeException(e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close result set!");
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close connection!");
        }
      }
    }
    return result.build();
  }

  private boolean isOracleServiceSchema(String schema) {
    List<String> schemas = Arrays
        .asList("ANONYMOUS", "APEX_040000", "APEX_PUBLIC_USER", "MDSYS", "XDB", "XS$NULL",
            "APPQOSSYS", "CTXSYS", "DBSNMP", "DIP", "OUTLN", "RDSADMIN", "SYS", "SYSTEM");
    return schemas.indexOf(schema) > -1;
  }
}
