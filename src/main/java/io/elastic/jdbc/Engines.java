package io.elastic.jdbc;

public enum Engines {
  MYSQL("com.mysql.cj.jdbc.Driver", 3306) {
    @Override
    protected String getSubprotocol(String host, Integer port, String db) {
      return "mysql";
    }

    @Override
    protected String getSubname(String host, Integer port, String db) {
      return String.format("//%s:%s/%s", host, port, db);
    }
  },

  HSQLDB("org.hsqldb.jdbcDriver", null) {
    @Override
    protected String getSubprotocol(String host, Integer port, String db) {
      return "hsqldb";
    }

    @Override
    protected String getSubname(String host, Integer port, String db) {
      return db;
    }
  },

  POSTGRESQL("org.postgresql.Driver", 5432) {
    @Override
    protected String getSubprotocol(String host, Integer port, String db) {
      return "postgresql";
    }
  },

  ORACLE("oracle.jdbc.driver.OracleDriver", 1521) {
    @Override
    protected String getSubprotocol(String host, Integer port, String db) {
      return "oracle:thin";
    }

    @Override
    protected String getSubname(String host, Integer port, String db) {
      return String.format("@//%s:%s", host, port);
    }
  },

  MSSQL("com.microsoft.sqlserver.jdbc.SQLServerDriver", 1433) {
    @Override
    protected String getSubprotocol(String host, Integer port, String db) {
      return "sqlserver";
    }

    @Override
    protected String getSubname(String host, Integer port, String db) {
      return String.format("//%s:%s;databaseName=%s", host, port, db);
    }
  };

  private final String driverClassName;
  private Integer defaultPort;

  Engines(final String driverClassName, final Integer defaultPort) {
    this.driverClassName = driverClassName;
    this.defaultPort = defaultPort;
  }

  protected abstract String getSubprotocol(String host, Integer port, String db);

  protected String getSubname(String host, Integer port, String db) {
    return String.format("//%s:%s/%s", host, port, db);
  }

  public String getConnectionString(String host, Integer port, String db) {
    return String.format("jdbc:%s:%s",
        getSubprotocol(host, port, db),
        getSubname(host, port, db));
  }

  public void loadDriverClass() {
    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Integer defaultPort() {
    return defaultPort;
  }
}
