package it.snova.dbmigrator.db;

public class ConnectorOptions
{
  String username;
  String password;
  String host;
  Integer port;
  String driver = "com.mysql.jdbc.Driver";
  String dbname;

  public ConnectorOptions driver(String driver)
  {
    this.driver = driver;
    return this;
  }
  
  public ConnectorOptions username(String username)
  {
    this.username = username;
    return this;
  }
  
  public ConnectorOptions password(String password)
  {
    this.password = password;
    return this;
  }
  
  public ConnectorOptions host(String host)
  {
    this.host = host;
    return this;
  }
  
  public ConnectorOptions dbname(String dbname)
  {
    this.dbname = dbname;
    return this;
  }
  
  public ConnectorOptions port(Integer port)
  {
    this.port = port;
    return this;
  }
  
  public String driver()
  {
    return driver;
  }

  public String username()
  {
    return username;
  }
  
  public String password()
  {
    return password;
  }
  
  public String host()
  {
    return host;
  }
  
  public String dbname()
  {
    return dbname;
  }
  
  public Integer port()
  {
    return port;
  }
  
}
