package laoflch.debezium.connector.informix

/**
 *
 * @author Jiri Pechanec
 *
 */
trait Nullable {
  /**
   * @return true if this object has real value, false if it is NULL object
   */
  def isAvailable: Boolean
}