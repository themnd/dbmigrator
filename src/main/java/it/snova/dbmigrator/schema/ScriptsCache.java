package it.snova.dbmigrator.schema;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ScriptsCache
{
  String folder;
  
  public ScriptsCache(String folder)
  {
    this.folder = folder;
  }
  
  public List<SchemaCache> init()
  {
    List<SchemaCache> schemas = new ArrayList<SchemaCache>();
    
    File source = new File(folder);
    final File[] children = source.listFiles();
    for (File child: children) {
      String name = child.getName();
      SchemaVersion version = getSchemaVersion(name);
      if (version == null) {
        continue;
      }
      List<String> scripts = new ArrayList<String>();
      for (File script: child.listFiles()) {
        if (!script.getName().endsWith(".sql")) {
          continue;
        }
        scripts.add(script.getAbsolutePath());
      }
      Collections.sort(scripts);
      
      schemas.add(new SchemaCache(version, scripts));
    }
    
    return schemas;
  }
  
  private SchemaVersion getSchemaVersion(String name)
  {
    if (name.length() > 0) {
      String[] fields = name.split("\\.");
      if (fields.length == 2) {
        if (isNumeric(fields[0]) && isNumeric(fields[1])) {
          return new SchemaVersion(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
        }
      }
    }
    return null;
  }
  
  private boolean isNumeric(String s)
  {
    try {
      Integer.parseInt(s);
      return true;
    } catch (NumberFormatException e) {
    }
    return false;
  }
  
  public class SchemaCache
  {
    SchemaVersion version;
    List<String> scripts;
    
    public SchemaCache(SchemaVersion version, List<String> scripts)
    {
      this.version = version;
      this.scripts = scripts;
    }
    
    public SchemaVersion version()
    {
      return version;
    }
    
    public List<String> scripts()
    {
      return scripts;
    }
  }
}
