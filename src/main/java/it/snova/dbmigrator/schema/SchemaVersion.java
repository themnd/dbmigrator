package it.snova.dbmigrator.schema;

public class SchemaVersion implements Comparable<SchemaVersion>
{
  int major;
  int minor;
  String script;
  String md5;
  String description;
  
  public SchemaVersion(int major, int minor)
  {
    this.major = major;
    this.minor = minor;
  }
  
  public int getMajor()
  {
    return major;
  }
  
  public int getMinor()
  {
    return minor;
  }
  
  public String getScript()
  {
    return this.script;
  }
  
  public String getMD5()
  {
    return this.md5;
  }
  
  public String getDescription()
  {
    return this.description;
  }
  
  public void setMajor(int major)
  {
    this.major = major;
  }
  
  public void setMinor(int minor)
  {
    this.minor = minor;
  }

  public void setScript(String script)
  {
    this.script = script;
  }
  
  public void setMD5(String md5)
  {
    this.md5 = md5;
  }
  
  public void setDescription(String description)
  {
    this.description = description;
  }
  
  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + major;
    result = prime * result + ((script == null) ? 0 : script.hashCode());
    result = prime * result + minor;
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SchemaVersion other = (SchemaVersion) obj;
    return compareTo(other) == 0;
  }
  
  public int compareTo(SchemaVersion other)
  {
    if (getMajor() == other.getMajor() && getMinor() == other.getMinor()) {
      return 0;
    }
    
    if (getMajor() == other.getMajor()) {
      if (getMinor() == other.getMinor()) {
        if (getScript() == null && other.getScript() == null) {
          return 0;
        }
        if (getScript() != null & other.getScript() == null) {
          return 1;
        }
        if (getScript() == null && other.getScript() != null) {
          return -1;
        }
        return getScript().compareTo(other.getScript());
      }
      return getMinor() - other.getMinor();
    }

    return getMajor() - other.getMajor();
  }

  public int compareToIgnoreScript(SchemaVersion other)
  {
    if (getMajor() == other.getMajor() && getMinor() == other.getMinor()) {
      return 0;
    }
    
    if (getMajor() == other.getMajor()) {
      return getMinor() - other.getMinor();
    }

    return getMajor() - other.getMajor();
  }

  @Override
  public String toString()
  {
    return "SchemaVersion [major=" + major + ", minor=" + minor + ", " + (script != null ? "script=" + script : "") + "]";
  }

}
