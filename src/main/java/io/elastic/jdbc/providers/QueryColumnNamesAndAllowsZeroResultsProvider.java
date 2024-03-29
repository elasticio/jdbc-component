package io.elastic.jdbc.providers;

import io.elastic.api.DynamicMetadataProvider;
import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.utils.Utils;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryColumnNamesAndAllowsZeroResultsProvider implements DynamicMetadataProvider, SelectModelProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      QueryColumnNamesAndAllowsZeroResultsProvider.class);

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
    Pattern patternCheckCharacter = Pattern.compile(Utils.TEMPLATE_REGEXP);
    Matcher matcherCheckCharacter = patternCheckCharacter.matcher(sqlQuery);
    Pattern pattern = Pattern.compile(Utils.VARS_REGEXP);
    Matcher matcher = pattern.matcher(sqlQuery);
    Boolean isEmpty = true;
    if (matcher.find()) {
      do {
        matcherCheckCharacter.find();
        if (!matcher.group().equals(matcherCheckCharacter.group())){
          throw new RuntimeException(
              "Prepared statement variables name '"
              + matcherCheckCharacter.group()
              + "' contains a forbidden character. "
              + "The name could contain: any word character, a digit and a character '_'");
        }
        JsonObjectBuilder field = Json.createObjectBuilder();
        String result[] = matcher.group().split(":");
        String name;
        String type;
        if (result.length > 0 && result.length < 3){
          name = result[0].substring(1);
          if (result.length == 1){
            type = "string";
          } else {
            type = result[1];
          }
        } else {
          throw new RuntimeException("Incorrect prepared statement" + matcher.group());
        }
        field.add("title", name)
            .add("type", type);
        properties.add(name, field);
        isEmpty = false;
      } while (matcher.find());
      if (isEmpty) {
        properties.add("empty dataset", "no columns");
      }
    }
    String emitBehaviour = "emitIndividually";

    try {
      emitBehaviour = configuration.getString("emitBehaviour");
    } catch (NullPointerException e) {
      LOGGER.info("No Emit behavior is specified, the default value Emit Individually will be used");
    }
    if (emitBehaviour.equals("expectSingle")) {
      JsonObjectBuilder field = Json.createObjectBuilder();
      field.add("title", "Allow Zero Results")
          .add("type", "boolean");
      properties.add("allowZeroResults", field);
    }

    return properties.build();
  }
}
