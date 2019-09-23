package io.elastic.jdbc;

import io.elastic.api.CredentialsVerifier;
import io.elastic.api.InvalidCredentialsException;
import io.elastic.jdbc.Utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import javax.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcCredentialsVerifier implements CredentialsVerifier {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcCredentialsVerifier.class);

  @Override
  public void verify(JsonObject configuration) throws InvalidCredentialsException {

    LOGGER.info("About to connect to database using given credentials");

    Connection connection = null;

    try {
      connection = Utils.getConnection(configuration);
      LOGGER.info("Successfully connected to database. Credentials verified.");
    } catch (Exception e) {
      throw new InvalidCredentialsException("Failed to connect to database", e);
    } finally {
      if (connection != null) {
        LOGGER.info("Closing database connection");
        try {
          connection.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to closed database connection", e);
        }
      }
    }
  }
}
