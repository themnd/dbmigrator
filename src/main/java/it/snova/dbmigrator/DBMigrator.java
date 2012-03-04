package it.snova.dbmigrator;

import it.snova.dbmigrator.db.Connector;
import it.snova.dbmigrator.db.ConnectorOptions;
import it.snova.dbmigrator.schema.SchemaMigrator;
import it.snova.dbmigrator.schema.ScriptsCache;
import joptsimple.OptionParser;
import joptsimple.OptionSet;


public class DBMigrator
{
  private static final String USERNAME_OPT = "u";
  private static final String PASSWORD_OPT = "p";
  private static final String SOURCE_OPT = "src";
  private static final String HOSTNAME_OPT = "h";
  private static final String PORT_OPT = "P";
  private static final String DATABASE_OPT = "d";
  private static final String DROPDB_OPT = "dropdb";

  public static void main(String[] args)
  {
    DBMigrator migrator = new DBMigrator();
    migrator.setArguments(args);
    System.exit(migrator.execute());
  }
  
  String[] args;
  
  public void setArguments(String[] args)
  {
    this.args = args;
  }
  
  public int execute()
  {
    OptionSet options = parseOptions(args);
    
    String user = (String) options.valueOf(USERNAME_OPT);
    String pwd = (String) options.valueOf(PASSWORD_OPT);
    String source = (String) options.valueOf(SOURCE_OPT);
    
    System.out.println("user: " + user);
    System.out.println("pwd: " + pwd);
    System.out.println("source: " + source);
    
    ScriptsCache scriptsCache = new ScriptsCache(source);
    SchemaMigrator migrator = new SchemaMigrator(
      new Connector(
        new ConnectorOptions()
          .username(user)
          .password(pwd)
          .host((String)options.valueOf(HOSTNAME_OPT))
          .port((Integer)options.valueOf(PORT_OPT))
          .dbname((String)options.valueOf(DATABASE_OPT))
    ));
    
    if (options.has(DROPDB_OPT)) {
      migrator.dropdb(true);
    }
    
    return migrator.migrate(scriptsCache);
  }
  
  private OptionSet parseOptions(String[] args)
  {
    OptionParser parser = initParser();
    return parser.parse(args);
  }
  
  private OptionParser initParser()
  {
    OptionParser parser = new OptionParser();
    parser
      .accepts(USERNAME_OPT)
      .withRequiredArg()
      .ofType(String.class)
      .describedAs("username")
      .defaultsTo(new String[] { "root" });
    parser
      .accepts(PASSWORD_OPT)
      .withRequiredArg()
      .ofType(String.class)
      .describedAs("password")
      .defaultsTo(new String[] { "" });
    parser
      .accepts(HOSTNAME_OPT)
      .withRequiredArg()
      .ofType(String.class)
      .describedAs("hostname")
      .defaultsTo(new String[] { "localhost" });
    parser
      .accepts(PORT_OPT)
      .withRequiredArg()
      .ofType(Integer.class)
      .describedAs("port")
      .defaultsTo(new Integer[] { new Integer(3306) });
    parser
      .accepts(DATABASE_OPT)
      .withRequiredArg()
      .ofType(String.class)
      .describedAs("database")
      .required();
    parser
      .accepts(SOURCE_OPT)
      .withRequiredArg()
      .ofType(String.class)
      .required()
      .describedAs("db scripts sources");
    parser
      .accepts(DROPDB_OPT);
    return parser;
  }

}
