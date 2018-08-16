package io.elastic.jdbc;

import io.elastic.api.DynamicMetadataProvider;
import io.elastic.api.SelectModelProvider;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryColumnNamesProvider implements DynamicMetadataProvider, SelectModelProvider {

  private static final Logger logger = LoggerFactory.getLogger(QueryColumnNamesProvider.class);

  public JsonObject getSelectModel(JsonObject configuration) {
    JsonObject result = Json.createObjectBuilder().build();
    JsonObject properties = getColumns(configuration);
    for (Map.Entry<String, JsonValue> entry : properties.entrySet()) {
      JsonValue field = entry.getValue();
      result = Json.createObjectBuilder().add(entry.getKey(), field.toString()).build();
    }
    return result;
  }

  /**
   * Returns Columns list as metadata
   */

  public JsonObject getMetaModel(JsonObject configuration) {
    JsonObject result = Json.createObjectBuilder().build();
    JsonObject inMetadata = Json.createObjectBuilder().build();
    JsonObject properties = getColumns(configuration);
    inMetadata = Json.createObjectBuilder().add("type", "object")
        .add("properties", properties).build();
    result = Json.createObjectBuilder().add("out", inMetadata)
        .add("in", inMetadata).build();
    return result;
  }

  public JsonObject getColumns(JsonObject configuration) {
    JsonObjectBuilder properties = Json.createObjectBuilder();
    String sqlQuery = configuration.getString("sqlQuery");
    Pattern pattern = Pattern.compile(Utils.VARS_REGEXP);
    Matcher matcher = pattern.matcher(sqlQuery);
    Boolean isEmpty = true;
    if (matcher.find()) {
      do {
        logger.info("Var = {}", matcher.group());
        JsonObjectBuilder field = Json.createObjectBuilder();
        String result[] = matcher.group().split(":");
        String name = result[0].substring(1);
        String type = result[1];
        field.add("title", name)
             .add("type", type);
        properties.add(name, field);
        isEmpty = false;
      } while (matcher.find());
      if (isEmpty) {
        properties.add("empty dataset", "no columns");
      }
    }

    return properties.build();
  }
}
