package io.elastic.jdbc.triggers;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Function;
import io.elastic.jdbc.query_builders.Query;
import io.elastic.jdbc.utils.QueryFactory;
import io.elastic.jdbc.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GetRowsPollingTrigger implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(GetRowsPollingTrigger.class);
  private static final String PROPERTY_TABLE_NAME = "tableName";
  private static final String PROPERTY_POLLING_FIELD = "pollingField";
  private static final String PROPERTY_POLLING_VALUE = "pollingValue";
  private static final String PROPERTY_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  private static final String DATETIME_REGEX = "(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})(\\.(\\d{1,3}))?";

  @Override
  public final void execute(ExecutionParameters parameters) {
    LOGGER.info("About to execute select trigger");
    final JsonObject configuration = parameters.getConfiguration();
    JsonObject snapshot = parameters.getSnapshot();
    LOGGER.debug("Got snapshot");
    checkConfig(configuration);
    String pollingField = "";
    Calendar cDate = Calendar.getInstance();
    cDate.set(cDate.get(Calendar.YEAR), cDate.get(Calendar.MONTH), cDate.get(Calendar.DATE), 0, 0,
        0);
    String dbEngine = configuration.getString(Utils.CFG_DB_ENGINE);
    String tableName = configuration.getString(PROPERTY_TABLE_NAME);

    if (Utils.getNonNullString(configuration, PROPERTY_POLLING_FIELD).length() != 0) {
      pollingField = configuration.getString(PROPERTY_POLLING_FIELD);
    }
    Timestamp pollingValue;
    Timestamp cts = new java.sql.Timestamp(cDate.getTimeInMillis());
    String formattedDate = new SimpleDateFormat(PROPERTY_DATETIME_FORMAT).format(cts);

    if (snapshot.containsKey(PROPERTY_POLLING_VALUE) && Utils
        .getNonNullString(snapshot, PROPERTY_POLLING_VALUE).matches(DATETIME_REGEX)) {
      pollingValue = Timestamp.valueOf(snapshot.getString(PROPERTY_POLLING_VALUE));
     } else if (configuration.containsKey(PROPERTY_POLLING_VALUE) && Utils
        .getNonNullString(configuration, PROPERTY_POLLING_VALUE).matches(DATETIME_REGEX)) {
      pollingValue = Timestamp.valueOf(configuration.getString(PROPERTY_POLLING_VALUE));
    } else {
      LOGGER.info(
          "There is an empty value for Start Polling From at the config and snapshot. So, we set Current Date = "
              + formattedDate);
      pollingValue = cts;
    }

    LOGGER.info("Executing row polling trigger");
    try (Connection connection = Utils.getConnection(configuration)) {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      query.from(tableName).orderBy(pollingField)
          .rowsPolling(pollingField, pollingValue);
      query.setMaxPollingValue(cts);
      ArrayList<JsonObject> resultList = query.executePolling(connection);

      for (int i = 0; i < resultList.size(); i++) {
        LOGGER.info("Row number: {} from {}", i + 1, resultList.size());
        LOGGER.info("Emitting data");
        parameters.getEventEmitter()
            .emitData(new Message.Builder().body(resultList.get(i)).build());
      }
      if (resultList.size() > 0) {
        formattedDate = query.getMaxPollingValue().toString();
        snapshot = Json.createObjectBuilder()
            .add(PROPERTY_TABLE_NAME, tableName)
            .add(PROPERTY_POLLING_FIELD, pollingField)
            .add(PROPERTY_POLLING_VALUE, formattedDate).build();
        LOGGER.info("Emitting new snapshot");
        parameters.getEventEmitter().emitSnapshot(snapshot);
      }
    } catch (SQLException e) {
      LOGGER.error("Failed to make request");
      throw new RuntimeException(e);
    }
  }

  private void checkConfig(JsonObject config) {
    final JsonValue tableName = config.get(PROPERTY_TABLE_NAME);
    final JsonValue pollingField = config.get(PROPERTY_POLLING_FIELD);

    if (tableName == null || tableName.toString().isEmpty()) {
      throw new RuntimeException("Table name is required field");
    }

    if (pollingField == null || pollingField.toString().isEmpty()) {
      throw new RuntimeException("Timestamp column is required field");
    }
  }
}
