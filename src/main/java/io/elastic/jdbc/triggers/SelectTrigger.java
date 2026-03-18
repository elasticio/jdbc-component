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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectTrigger implements Function {

  private static final Logger LOGGER = LoggerFactory.getLogger(SelectTrigger.class);
  private static final String PROPERTY_DB_ENGINE = "dbEngine";
  private static final String LAST_POLL_PLACEHOLDER = "%%EIO_LAST_POLL%%";
  private static final String SQL_QUERY_VALUE = "sqlQuery";
  private static final String PROPERTY_POLLING_VALUE = "pollingValue";
  private static final String PROPERTY_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  private static final String PROPERTY_SKIP_NUMBER = "skipNumber";
  private static final String DATETIME_REGEX = "(\\d{4})-(\\d{2})-(\\d{2})( (\\d{2}):(\\d{2}):(\\d{2})(\\.(\\d{1,9}))?)?";

  @Override
  public final void execute(ExecutionParameters parameters) {
    LOGGER.info("About to execute select trigger");
    final JsonObject configuration = parameters.getConfiguration();
    checkConfig(configuration);
    String sqlQuery = configuration.getString(SQL_QUERY_VALUE);
    JsonObject snapshot = parameters.getSnapshot();
    Integer skipNumber = 0;

    Calendar cDate = Calendar.getInstance();
    cDate.set(cDate.get(Calendar.YEAR), cDate.get(Calendar.MONTH), cDate.get(Calendar.DATE), 0, 0,
        0);
    String dbEngine = configuration.getString(PROPERTY_DB_ENGINE);
    Timestamp cts = new java.sql.Timestamp(cDate.getTimeInMillis());

    Timestamp pollingValue = getPollingValue(configuration, snapshot, cts);
    LOGGER.debug("EIO_LAST_POLL = {}", pollingValue);

    if (snapshot.get(PROPERTY_SKIP_NUMBER) != null) {
      skipNumber = snapshot.getInt(PROPERTY_SKIP_NUMBER);
    }
    LOGGER.info("Executing select trigger");
    try {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      sqlQuery = Query.preProcessSelect(sqlQuery);
      String lowerCaseQuery = sqlQuery.toLowerCase();
      if (lowerCaseQuery.contains(LAST_POLL_PLACEHOLDER.toLowerCase())) {
        sqlQuery = sqlQuery.replaceAll("(?i)'" + Pattern.quote(LAST_POLL_PLACEHOLDER) + "'", "?");
        sqlQuery = sqlQuery.replaceAll("(?i)" + Pattern.quote(LAST_POLL_PLACEHOLDER), "?");
        query.selectPolling(sqlQuery, pollingValue);
      }
      Connection connection = Utils.getConnection(configuration);
      ArrayList<JsonObject> resultList = query.executeSelectTrigger(connection, sqlQuery);
      for (int i = 0; i < resultList.size(); i++) {
        LOGGER.info("Columns count: {} from {}", i + 1, resultList.size());
        LOGGER.info("Emitting data");
        parameters.getEventEmitter()
            .emitData(new Message.Builder().body(resultList.get(i)).build());
      }

      snapshot = Json.createObjectBuilder()
          .add(PROPERTY_SKIP_NUMBER, skipNumber + resultList.size())
          .add(LAST_POLL_PLACEHOLDER, pollingValue.toString())
          .add(SQL_QUERY_VALUE, sqlQuery).build();
      LOGGER.info("Emitting new snapshot");
      parameters.getEventEmitter().emitSnapshot(snapshot);
    } catch (SQLException e) {
      LOGGER.error("Failed to make request");
      throw new RuntimeException(e);
    }
  }

  public Timestamp getPollingValue(JsonObject configuration, JsonObject snapshot, Timestamp defaultTimestamp) {
    String val = null;
    if (snapshot.containsKey(LAST_POLL_PLACEHOLDER)) {
      val = snapshot.getString(LAST_POLL_PLACEHOLDER);
    } else if (snapshot.containsKey(PROPERTY_POLLING_VALUE)) {
      val = snapshot.getString(PROPERTY_POLLING_VALUE);
    } else if (configuration.containsKey(PROPERTY_POLLING_VALUE)) {
      val = configuration.getString(PROPERTY_POLLING_VALUE);
    }

    if (val != null && !val.isEmpty()) {
      val = val.trim();
      if (val.matches(DATETIME_REGEX)) {
        if (val.length() <= 10) {
          val += " 00:00:00";
        }
        try {
          return Timestamp.valueOf(val);
        } catch (IllegalArgumentException e) {
          LOGGER.warn("Failed to parse polling value '{}' with Timestamp.valueOf, falling back to default.", val);
        }
      } else {
        LOGGER.warn("Polling value '{}' does not match expected format {}, falling back to default.", val,
            DATETIME_REGEX);
      }
    }

    String formattedDate = new SimpleDateFormat(PROPERTY_DATETIME_FORMAT).format(defaultTimestamp);
    LOGGER.trace(
        "Using default polling value (Today Midnight): " + formattedDate);
    return defaultTimestamp;
  }

  private void checkConfig(JsonObject config) {
    final String sqlQuery = config.getString(SQL_QUERY_VALUE);

    if (sqlQuery == null || sqlQuery.isEmpty()) {
      throw new RuntimeException("SQL Query is required field");
    }

    Pattern pattern = Pattern.compile(Utils.TEMPLATE_REGEXP);
    Matcher matcher = pattern.matcher(sqlQuery);
    if (matcher.find()) {
      throw new RuntimeException("Use of prepared statement variables is forbidden: '"
          + matcher.group()
          + "'");
    }
  }
}
