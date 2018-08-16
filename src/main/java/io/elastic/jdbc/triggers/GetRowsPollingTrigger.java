package io.elastic.jdbc.triggers;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.QueryBuilders.Query;
import io.elastic.jdbc.QueryFactory;
import io.elastic.jdbc.Utils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GetRowsPollingTrigger implements Module {

  private static final Logger logger = LoggerFactory.getLogger(GetRowsPollingTrigger.class);
  private static final String PROPERTY_TABLE_NAME = "tableName";
  private static final String PROPERTY_POLLING_FIELD = "pollingField";
  private static final String PROPERTY_POLLING_VALUE = "pollingValue";
  private static final String PROPERTY_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.sss";
  private static final String PROPERTY_SKIP_NUMBER = "skipNumber";
  private static final String DATETIME_REGEX = "(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})(\\.(\\d{1,3}))?";
  private static boolean isEmpty = true;
  
  @Override
  public final void execute(ExecutionParameters parameters) {
    logger.info("About to execute select trigger");
    final JsonObject configuration = parameters.getConfiguration();
    JsonObject snapshot = parameters.getSnapshot();
    JsonObjectBuilder row = Json.createObjectBuilder();
    checkConfig(configuration);
    Connection connection = Utils.getConnection(configuration);
    Integer skipNumber = 0;
    Integer rowsCount = 0;
    String pollingField = "";
    Calendar cDate = Calendar.getInstance();
    cDate.set(cDate.get(Calendar.YEAR), cDate.get(Calendar.MONTH), cDate.get(Calendar.DATE), 0, 0,
        0);

    String dbEngine = configuration.getString(Utils.CFG_DB_ENGINE);
    String tableName = configuration.getString(PROPERTY_TABLE_NAME);
    if(Utils.getNonNullString(configuration, PROPERTY_POLLING_FIELD).length() != 0) {
      pollingField = configuration.getString(PROPERTY_POLLING_FIELD);
    }
    Timestamp pollingValue;
    Timestamp cts = new java.sql.Timestamp(cDate.getTimeInMillis());
    Timestamp maxPollingValue = cts;
    String formattedDate = new SimpleDateFormat(PROPERTY_DATETIME_FORMAT).format(cts);

    if (configuration.containsKey(PROPERTY_POLLING_VALUE) && Utils.getNonNullString(configuration, PROPERTY_POLLING_VALUE).matches(DATETIME_REGEX)) {
      pollingValue = Timestamp.valueOf(configuration.getString(PROPERTY_POLLING_VALUE));
    } else if (snapshot.containsKey(PROPERTY_POLLING_VALUE) && Utils.getNonNullString(snapshot, PROPERTY_POLLING_VALUE).matches(DATETIME_REGEX)) {
      pollingValue = Timestamp.valueOf(snapshot.getString(PROPERTY_POLLING_VALUE));
    } else {
      logger.info(
          "There is an empty value for Start Polling From at the config and snapshot. So, we set Current Date = "
              + formattedDate);
      pollingValue = cts;
    }

    if (snapshot.containsKey(PROPERTY_SKIP_NUMBER))
      skipNumber = snapshot.getInt(PROPERTY_SKIP_NUMBER);

    if (snapshot.containsKey(PROPERTY_TABLE_NAME) && snapshot.get(PROPERTY_TABLE_NAME) != null && !snapshot.getString(PROPERTY_TABLE_NAME)
        .equals(tableName)) {
      skipNumber = 0;
    }

    ResultSet rs = null;
    logger.info("Executing row polling trigger");
    try {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      query.from(tableName).skip(skipNumber).orderBy(pollingField)
          .rowsPolling(pollingField, pollingValue);
      rs = query.executePolling(connection);
      ResultSetMetaData metaData = rs.getMetaData();
      while (rs.next()) {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          row = Utils.getColumnDataByType(rs, metaData, i, row);
          if (metaData.getColumnName(i).toUpperCase().equals(pollingField.toUpperCase())) {
            if (maxPollingValue.before(rs.getTimestamp(i))) {
              if (rs.getString(metaData.getColumnName(i)).length() > 10) {
                maxPollingValue = java.sql.Timestamp
                    .valueOf(rs.getString(metaData.getColumnName(i)));
              } else {
                maxPollingValue = java.sql.Timestamp
                    .valueOf(rs.getString(metaData.getColumnName(i)) + " 00:00:00");
              }
            }
          }
        }
        parameters.getEventEmitter().emitData(new Message.Builder().body(row.build()).build());
        rowsCount++;
      }

      if (rowsCount == 0) {
        row.add("empty dataset", "no data");
        logger.info("Emitting empty data");
        maxPollingValue = new java.sql.Timestamp(System.currentTimeMillis());
        parameters.getEventEmitter().emitData(new Message.Builder().body(row.build()).build());
      }

      formattedDate = new SimpleDateFormat(PROPERTY_DATETIME_FORMAT).format(maxPollingValue);

      snapshot = Json.createObjectBuilder().add(PROPERTY_SKIP_NUMBER, skipNumber + rowsCount)
                                           .add(PROPERTY_TABLE_NAME, tableName)
                                           .add(PROPERTY_POLLING_FIELD, pollingField)
                                           .add(PROPERTY_POLLING_VALUE, formattedDate).build();
      logger.info("Emitting new snapshot {}", snapshot.toString());
      parameters.getEventEmitter().emitSnapshot(snapshot);
    } catch (SQLException e) {
      logger.error("Failed to make request", e.toString());
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
