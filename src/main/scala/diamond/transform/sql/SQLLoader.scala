package diamond.transform.sql

import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by markmo on 16/12/2015.
  */
object SQLLoader {

  private val dotXml = ".+\\.[xX][mM][lL]".r

  private val sql = mutable.Map[String, mutable.Map[String, String]]()

  /**
    * Loads a set of named queries into a Map object. This implementation
    * reads a properties file at the given path. The properties file can be
    * in either line-oriented or XML format. XML formatted properties files
    * must use a <code>.xml</code> file extension.
    *
    * @param path The path that the ClassLoader will use to find the file
    * @return Map of query names to SQL values
    * @see java.util.Properties
    */
  def load(path: String) = {
    this.synchronized {
      var sqlMap = sql.getOrElse(path, null)
      if (sqlMap == null) {
        sqlMap = loadQueries(path)
        sql.put(path, sqlMap)
      }
      sqlMap
    }
  }

  def loadQueries(path: String) = {
    val in = getClass.getResourceAsStream(path)

    if (in == null) {
      throw new IllegalArgumentException(path + " not found")
    }

    val props = new Properties()
    try {
      if (dotXml.pattern.matcher(path).matches()) {
        props.loadFromXML(in)
      } else {
        props.load(in)
      }
    } finally {
      in.close()
    }
    props
  }

  def unload(path: String) {
    sql.remove(path)
  }

}
