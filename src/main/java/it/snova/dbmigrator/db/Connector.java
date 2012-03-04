package it.snova.dbmigrator.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connector
{
  ConnectorOptions options;
  
  public Connector(ConnectorOptions options)
  {
    this.options = options;
  }
  
  public Connect createConnect() throws SQLException
  {
    return new Connect(getConnection());
  }
  
  public String getDBName()
  {
    return options.dbname();
  }
  
  private Connection getConnection() throws SQLException
  {
    try {
      Class.forName(options.driver());
      return DriverManager.getConnection(getUrl(), options.username(), options.password());
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  private String getUrl()
  {
    return "jdbc:mysql://" + options.host() + ":" + options.port() + "/" + options.dbname();
  }

}
