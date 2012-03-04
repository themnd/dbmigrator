package it.snova.dbmigrator.schema;

import it.snova.dbmigrator.db.Connect;
import it.snova.dbmigrator.db.Connector;
import it.snova.dbmigrator.db.ScriptRunner;
import it.snova.dbmigrator.schema.ScriptsCache.SchemaCache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class SchemaMigrator
{
  Connector connector;
  String encoding;
  boolean dropdb;
  
  public SchemaMigrator(Connector connector)
  {
    this.connector = connector;
    this.encoding = "UTF-8";
    this.dropdb = false;
  }
  
  public SchemaMigrator encoding(String encoding)
  {
    this.encoding = encoding;
    return this;
  }
  
  public SchemaMigrator dropdb(boolean dropdb)
  {
    this.dropdb = dropdb;
    return this;
  }
  
  public int migrate(ScriptsCache scriptsCache)
  {
    List<SchemaCache> schemas = scriptsCache.init();
    
    try {
      if (dropdb) {
        recreateDB();
      }
      
      SchemaVersion schema = getSchemaVersion();
      for (SchemaCache s: schemas) {
        if (s.version().compareTo(schema) <= 0) {
          continue;
        }
        applySchema(s);
      }
      return 0;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return 1;
  }
  
  private void applySchema(SchemaCache schema) throws IOException, SQLException
  {
    System.out.println("Applying schema major: " + schema.version().getMajor() + " minor: " + schema.version().getMinor());
    
    Connect connect = connector.createConnect();
    
    for (String script: schema.scripts()) {
      System.out.println("Script: " + script);

      boolean versionExists = connect.doesTableExists("schemaversion");
      if (versionExists) {
        insertSchemaVersion(connect, schema.version(), script);
      }
      
      ScriptRunner runner = new ScriptRunner(connect.getConnection(), false, true);
      runner.setLogWriter(null);
      runner.runScript(new FileReader(script));
      
      if (!versionExists) {
        insertSchemaVersion(connect, schema.version(), script);
      }
    }
  }
  
  private void insertSchemaVersion(Connect connect, SchemaVersion schema, String script) throws SQLException, IOException
  {
    File f = new File(script);
    FileInputStream in = new FileInputStream(f);
    String content = IOUtils.toString(in, encoding);
    
    String description = getDescription(content);
    System.out.println("Description: " + description);

    FileInputStream fis = new FileInputStream(f);
    String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);
    
    Statement stmt = connect.createStatement();
    String query = "insert into schemaversion "
      + "(major, minor, date, script, description, md5) values "
      + "(" + schema.getMajor()
      + "," + schema.getMinor()
      + ", NOW()"
      + ", \"" + f.getName() + "\""
      + ", \"" + description + "\""
      + ", \"" + md5 + "\""
      + ")";
    stmt.executeUpdate(query);
    stmt.close();    
  }
  
  private String getDescription(String content)
  {
    StringBuilder description = new StringBuilder();
    for (String line: content.split("\n")) {
      if (line.startsWith("--")) {
        description.append(line.replaceFirst("--\\s*", ""));
        description.append("\n");
        continue;
      }
      break;
    }
    
    return description.toString();
  }

  private SchemaVersion getSchemaVersion()
  {
    SchemaVersion schema = new SchemaVersion(0, 0);
    try {
      Connect conn = connector.createConnect();
      if (conn.doesTableExists("schemaversion")) {
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select id, major, minor from schemaversion");
        while (resultSet.next()) {
          int id = resultSet.getInt(0);
          int major = resultSet.getInt(1);
          int minor = resultSet.getInt(2);
          if (major > schema.getMajor()) {
            schema.setMajor(major);
            schema.setMinor(minor);
          } else if (major == schema.getMajor() && minor > schema.getMinor()) {
            schema.setMinor(minor);
          }
          System.out.println("version : " + id + " major: " + major + " minor: " + minor);
        }
        statement.close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return schema;
  }
  
  private void recreateDB() throws SQLException
  {
    Connect conn = connector.createConnect();
    Statement statement = conn.createStatement();
    statement.executeUpdate("drop schema " + connector.getDBName());
    statement.executeUpdate("create schema " + connector.getDBName());
    statement.close();
  }
  
}
