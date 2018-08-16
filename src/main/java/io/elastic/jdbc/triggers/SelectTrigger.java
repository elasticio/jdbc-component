package io.elastic.jdbc.triggers;

import io.elastic.api.ExecutionParameters;
import io.elastic.api.Message;
import io.elastic.api.Module;
import io.elastic.jdbc.QueryBuilders.Query;
import io.elastic.jdbc.QueryFactory;
import io.elastic.jdbc.Utils;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SelectTrigger implements Module {

  private static final Logger logger = LoggerFactory.getLogger(SelectTrigger.class);
  private static final String PROPERTY_DB_ENGINE = "dbEngine";
  private static final String LAST_POLL_PLACEHOLDER = "%%EIO_LAST_POLL%%";
  private static final String SQL_QUERY_VALUE = "sqlQuery";
  private static final String PROPERTY_POLLING_VALUE = "pollingValue";
  private static final String PROPERTY_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.sss";
  private static final String PROPERTY_SKIP_NUMBER = "skipNumber";
  private static final String DATETIME_REGEX = "(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})(\\.(\\d{1,3}))?";

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

    Calendar cDate = Calendar.getInstance();
    cDate.set(cDate.get(Calendar.YEAR), cDate.get(Calendar.MONTH), cDate.get(Calendar.DATE), 0, 0,
        0);
    String dbEngine = configuration.getString(PROPERTY_DB_ENGINE);
    Timestamp pollingValue;
    Timestamp cts = new java.sql.Timestamp(cDate.getTimeInMillis());

    String formattedDate = new SimpleDateFormat(PROPERTY_DATETIME_FORMAT).format(cts);
    if (configuration.containsKey(PROPERTY_POLLING_VALUE) && Utils.getNonNullString(configuration, PROPERTY_POLLING_VALUE).matches(DATETIME_REGEX)) {
      pollingValue = Timestamp.valueOf(configuration.getString(PROPERTY_POLLING_VALUE));
    } else if (snapshot.containsKey(PROPERTY_POLLING_VALUE) && Utils.getNonNullString(snapshot, LAST_POLL_PLACEHOLDER).matches(DATETIME_REGEX)) {
      pollingValue = Timestamp.valueOf(snapshot.getString(LAST_POLL_PLACEHOLDER));
    } else {
      logger.info(
          "There is an empty value for Start Polling From at the config and snapshot. So, we set Current Date = "
              + formattedDate);
      pollingValue = cts;
    }
    logger.info("EIO_LAST_POLL = {}", pollingValue);
    String sqlQuery = configuration.getString(SQL_QUERY_VALUE);
    if (snapshot.get(PROPERTY_SKIP_NUMBER) != null)
      skipNumber = snapshot.getInt(PROPERTY_SKIP_NUMBER);
    logger.info("SQL QUERY {} : ", sqlQuery);
    ResultSet rs = null;
    logger.info("Executing select trigger");
    try {
      QueryFactory queryFactory = new QueryFactory();
      Query query = queryFactory.getQuery(dbEngine);
      sqlQuery = Query.preProcessSelect(sqlQuery);
      if(sqlQuery.contains(LAST_POLL_PLACEHOLDER)) {
        sqlQuery = sqlQuery.replace(LAST_POLL_PLACEHOLDER, "?");
        query.selectPolling(sqlQuery, pollingValue);
      }
      logger.info("SQL Query: {}", sqlQuery);
      rs = query.executeSelectTrigger(connection, sqlQuery);
      ResultSetMetaData metaData = rs.getMetaData();
      while (rs.next()) {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          row = Utils.getColumnDataByType(rs, metaData, i, row);
        }
        rowsCount++;
        logger.info("Emitting data");
        logger.info(row.toString());
        parameters.getEventEmitter().emitData(new Message.Builder().body(row.build()).build());
      }

      if (rowsCount == 0) {
        row.add("empty dataset", "no data");
        logger.info("Emitting data");
        logger.info(row.toString());
        parameters.getEventEmitter().emitData(new Message.Builder().body(row.build()).build());
      }

      snapshot = Json.createObjectBuilder().add(PROPERTY_SKIP_NUMBER, skipNumber + rowsCount)
                                           .add(LAST_POLL_PLACEHOLDER, pollingValue.toString())
                                           .add(SQL_QUERY_VALUE, sqlQuery).build();
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
    final JsonString sqlQuery = config.getJsonString(SQL_QUERY_VALUE);

    if (sqlQuery == null || sqlQuery.toString().isEmpty()) {
      throw new RuntimeException("SQL Query is required field");
    }
  }

}
