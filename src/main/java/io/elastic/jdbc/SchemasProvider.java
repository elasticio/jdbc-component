package io.elastic.jdbc;

import io.elastic.api.SelectModelProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemasProvider implements SelectModelProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemasProvider.class);

  @Override
  public JsonObject getSelectModel(JsonObject configuration) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    List<String> proceduresNames = getSchemasList(configuration);
    proceduresNames.forEach(procedure -> result.add(procedure, procedure));
    return result.build();
  }

  public List<String> getSchemasList(JsonObject configuration) {
    List<String> result = new LinkedList<>();

    try (Connection conn = Utils.getConnection(configuration)) {
      DatabaseMetaData meta = conn.getMetaData();
      ResultSet res = meta.getSchemas();
      System.out.println("List of schemas: ");
      while (res.next()) {
        result.add(res.getString("TABLE_SCHEM"));
      }
      res.close();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

    return result;
  }
}
