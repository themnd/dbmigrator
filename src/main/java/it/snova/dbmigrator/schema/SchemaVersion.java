package it.snova.dbmigrator.schema;

public class SchemaVersion implements Comparable<SchemaVersion>
{
  int major;
  int minor;
  
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
  
  public void setMajor(int major)
  {
    this.major = major;
  }
  
  public void setMinor(int minor)
  {
    this.minor = minor;
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + major;
    result = prime * result + minor;
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SchemaVersion other = (SchemaVersion) obj;
    if (major != other.major) {
      return false;
    }
    if (minor != other.minor) {
      return false;
    }
    return true;
  }
  
  public int compareTo(SchemaVersion other)
  {
    if (getMajor() == other.getMajor() && getMinor() == other.getMinor()) {
      return 0;
    }
    
    if (getMajor() == other.getMajor()) {
      return getMinor() - other.getMinor();
    }

    return getMajor() - other.getMajor();
  }

}
