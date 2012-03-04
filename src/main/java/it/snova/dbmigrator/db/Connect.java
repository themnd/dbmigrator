package it.snova.dbmigrator.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Connect
{
  Connection connection;

  protected Connect(Connection connection)
  {
    this.connection = connection;
  }
  
  public Connection getConnection()
  {
    return connection;
  }
  
  public Statement createStatement() throws SQLException
  {
    return connection.createStatement();
  }
  
  public boolean doesTableExists(String name)
  {
    try {
      DatabaseMetaData dbm = connection.getMetaData();
      ResultSet tables = dbm.getTables(null, null, name, null);
      return tables.next();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }
  
}
