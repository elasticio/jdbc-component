package io.elastic.jdbc;

import io.elastic.api.DynamicMetadataProvider;
import io.elastic.api.SelectModelProvider;
import io.elastic.jdbc.ProcedureParameter.Direction;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcedureFieldsNameProvider implements DynamicMetadataProvider, SelectModelProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureFieldsNameProvider.class);

  @Override
  public JsonObject getSelectModel(JsonObject configuration) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    List<String> proceduresNames = getProceduresList(configuration);
    proceduresNames.forEach(procedure -> result.add(procedure, procedure));
    return result.build();
  }

  @Override
  public JsonObject getMetaModel(JsonObject configuration) {
    JsonObjectBuilder result = Json.createObjectBuilder();
    JsonObjectBuilder inFields = Json.createObjectBuilder();
    JsonObjectBuilder outFields = Json.createObjectBuilder();

    List<ProcedureParameter> parameters = getProcedureMetadata(configuration);

    parameters.stream()
        .filter(p -> p.getDirection() == Direction.IN || p.getDirection() == Direction.INOUT)
        .forEach(p -> {
          JsonObjectBuilder valueContent = Json.createObjectBuilder()
              .add("type", Utils.detectColumnType(p.getType(), ""))
              .add("name", p.getName())
              .add("required", true);
          inFields.add(p.getName(), valueContent.build());
        });

    JsonObjectBuilder inMetadata = Json.createObjectBuilder()
        .add("type", "object")
        .add("properties", inFields.build());

    parameters.stream()
        .filter(p -> p.getDirection() == Direction.OUT || p.getDirection() == Direction.INOUT)
        .forEach(p -> {
          JsonObjectBuilder valueContent = Json.createObjectBuilder()
              .add("type", Utils.detectColumnType(p.getType(), ""))
              .add("name", p.getName())
              .add("required", true);
          outFields.add(p.getName(), valueContent.build());
        });

    JsonObjectBuilder outMetadata = Json.createObjectBuilder()
        .add("type", "object")
        .add("properties", outFields.build());

    result.add("in", inMetadata.build()).add("out", outMetadata.build());

    JsonObject metadataResponse = result.build();
    System.out.println(metadataResponse);
    return metadataResponse;
  }

  public List<String> getProceduresList(JsonObject config) {
    List<String> result = new ArrayList<>();
    try (Connection conn = Utils.getConnection(config)) {
      DatabaseMetaData meta = conn.getMetaData();
      ResultSet res = meta.getProcedures(null, config.getString("schemaName"), null);
      System.out.println("List of procedures: ");
      while (res.next()) {
        String cat = res.getString("PROCEDURE_CAT");
        String schem = res.getString("PROCEDURE_SCHEM");
        String name = res.getString("PROCEDURE_NAME");

        result.add(name);
      }
      res.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  public static List<ProcedureParameter> getProcedureMetadata(JsonObject config) {
    List<ProcedureParameter> parameters = new LinkedList<>();

    try (Connection conn = Utils.getConnection(config)) {
      Statement st = conn.createStatement();

      DatabaseMetaData dbMetaData = conn.getMetaData();
      ResultSet rs = dbMetaData.getProcedureColumns(conn.getCatalog(),
          config.getString("schemaName"),
          config.getString("procedureName"),
          null);

      while (rs.next()) {
        // get stored procedure metadata
        String procedureCatalog = rs.getString(1);
        String procedureSchema = rs.getString(2);
        String procedureName = rs.getString(3);
        String columnName = rs.getString(4);
        short columnReturn = rs.getShort(5);
        int columnDataType = rs.getInt(6);
        String columnReturnTypeName = rs.getString(7);
        int columnPrecision = rs.getInt(8);
        int columnByteLength = rs.getInt(9);
        short columnScale = rs.getShort(10);
        short columnRadix = rs.getShort(11);
        short columnNullable = rs.getShort(12);
        String columnRemarks = rs.getString(13);

        parameters.add(new ProcedureParameter(columnName, columnReturn, columnDataType));
      }

      st.close();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return parameters;
  }
}