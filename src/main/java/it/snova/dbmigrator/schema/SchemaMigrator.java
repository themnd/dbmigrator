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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class SchemaMigrator
{
  Connector connector;
  String encoding;
  boolean dropdb;
  boolean checkscripts;
  boolean noexecute;
  
  public SchemaMigrator(Connector connector)
  {
    this.connector = connector;
    this.encoding = "UTF-8";
    this.dropdb = false;
    this.checkscripts = false;
    this.noexecute = false;
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

  public SchemaMigrator check(boolean check)
  {
    this.checkscripts = check;
    return this;
  }

  public SchemaMigrator noexecute(boolean noexecute)
  {
    this.noexecute = noexecute;
    return this;
  }
  
  public int migrate(ScriptsCache scriptsCache)
  {
    if (checkscripts) {
      return doCheckScripts(scriptsCache);
    }
    return doUpgrade(scriptsCache);
  }

  private int doCheckScripts(ScriptsCache scriptsCache)
  {
    try {
      List<SchemaCache> schemas = scriptsCache.init();

      int retvalue = 0;
      
      List<SchemaVersion> versions = getAllSchemaVersions();
      for (SchemaVersion v: versions) {
        System.out.println("check version " + v.toString());
        
        for (SchemaCache s: schemas) {
          if (s.version().compareToIgnoreScript(v) == 0) {
            for (String script: s.scripts()) {
              File f = new File(script);
              if (f.getName().equals(v.getScript())) {
                String md5 = getMD5(f);
                if (!md5.equals(v.getMD5())) {
                  System.err.println("md5 of  " + script + " differs from applied!");
                  retvalue = 1;
                }
                s.scripts().remove(script);
                break;
              }
            }
          }
        }
      }
      for (SchemaCache s: schemas) {
        List<String> scripts = s.scripts();
        if (scripts.size() > 0) {
          SchemaVersion v = s.version();
          System.err.println("Missing " + scripts.size() + " script from version " + v.getMajor() + "." + v.getMinor() + "!");
          for (String script: scripts) {            
            System.err.println("Script: " + script);
          }
          retvalue = 1;
        }
      }
      return retvalue;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 1;
  }
  
  private int doUpgrade(ScriptsCache scriptsCache)
  {
    List<SchemaCache> schemas = scriptsCache.init();
    
    try {
      if (dropdb) {
        recreateDB();
      }
      
      List<SchemaVersion> versions = getAllSchemaVersions();

      SchemaVersion schema = getSchemaVersion();
      for (SchemaCache s: schemas) {
        if (s.version().compareTo(schema) < 0) {
          continue;
        }
        applySchema(s, versions);
      }
      return 0;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return 1;
  }

  private void applySchema(SchemaCache schema, List<SchemaVersion> versions) throws IOException, SQLException
  {
    System.out.println("Applying schema major: " + schema.version().getMajor() + " minor: " + schema.version().getMinor() + "\n");
    
    Connect connect = connector.createConnect();
    
    for (String script: schema.scripts()) {
      
      File f = new File(script);
      
      boolean skip = false;
      for (SchemaVersion v: versions) {
        if (schema.version().compareToIgnoreScript(v) == 0) {
          if (v.getScript().equals(f.getName())) {
            String md5 = getMD5(f);
            if (!md5.equals(v.getMD5())) {
              System.err.println("the script " + script + " has already been imported but the md5 hash is not the same!");
            }
            skip = true;
            break;
          }
        }
      }
      if (skip) {
        continue;
      }
      System.out.println("* apply " + script + "\n");

      boolean versionExists = connect.doesTableExists("schemaversion");
      if (versionExists) {
        insertSchemaVersion(connect, schema.version(), script);
      }
      
      if (!noexecute) {
        ScriptRunner runner = new ScriptRunner(connect.getConnection(), false, true);
        runner.setLogWriter(null);
        runner.runScript(new FileReader(script));        
      }
      
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
    System.out.println(description);

    String md5 = getMD5(f);
    
    if (!noexecute) {
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
  }
  
  private String getMD5(File f) throws IOException
  {
    FileInputStream fis = new FileInputStream(f);
    return org.apache.commons.codec.digest.DigestUtils.md5Hex(fis);    
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

  private List<SchemaVersion> getAllSchemaVersions()
  {
    List<SchemaVersion> schemas = new ArrayList<SchemaVersion>();
    
    try {
      Connect conn = connector.createConnect();
      if (conn.doesTableExists("schemaversion")) {
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("select id, major, minor, script, description, md5 from schemaversion order by id");
        while (resultSet.next()) {
          int id = resultSet.getInt(1);
          int major = resultSet.getInt(2);
          int minor = resultSet.getInt(3);
          String script = resultSet.getString(4);
          String description = resultSet.getString(5);
          String md5 = resultSet.getString(6);
          
          SchemaVersion schema = new SchemaVersion(major, minor);
          schema.setScript(script);
          schema.setDescription(description);
          schema.setMD5(md5);
          
          schemas.add(schema);

          System.out.println("version : " + id + " major: " + major + " minor: " + minor);
        }
        statement.close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return schemas;
  }

  private SchemaVersion getSchemaVersion()
  {
    List<SchemaVersion> schema = getAllSchemaVersions();
    if (schema.size() > 0) {
      return schema.get(schema.size() - 1);
    }
    return new SchemaVersion(0, 0);
  }

  private void recreateDB() throws SQLException
  {
    if (!noexecute) {
      Connect conn = connector.createConnect();
      Statement statement = conn.createStatement();
      statement.executeUpdate("drop schema " + connector.getDBName());
      statement.executeUpdate("create schema " + connector.getDBName());
      statement.close();
    }
  }
  
}
