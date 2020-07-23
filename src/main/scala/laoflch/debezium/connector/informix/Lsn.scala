
package laoflch.debezium.connector.informix.integrtest

import java.math.BigInteger
import java.util
import java.util.Arrays

import io.debezium.util.Strings


/**
 * A logical representation of Informix LSN (log sequence number) position. When LSN is not available
 * it is replaced with {@link } constant.
 *
 * @author laoflch Luo
 *
 */
object Lsn {
  private val NULL_STRING = "NULL"
  val NULL = new Lsn(null)

  /**
   * @param lsnString - textual representation of Lsn
   * @return LSN converted from its textual representation
   */
  def valueOf(lsnString: String): Lsn = if (lsnString == null || NULL_STRING == lsnString) NULL
  else new Lsn(Strings.hexStringToByteArray(lsnString.replace(":", "")))

  /**
   * @param lsnBinary - binary representation of Lsn
   * @return LSN converted from its binary representation
   */
  def valueOf(lsnBinary: Array[Byte]): Lsn = if (lsnBinary == null) NULL
  else new Lsn(lsnBinary)
}

class Lsn private(val binary: Array[Byte]) extends Comparable[Lsn] with Nullable {
  private var unsignedBinary: Array[Int] = null
  private var string: String = null

  /**
   * @return binary representation of the stored LSN
   */
  def getBinary: Array[Byte] = binary

  /**
   * @return true if this is a real LSN or false it it is { @code NULL}
   */
  override def isAvailable: Boolean = binary != null

  private def getUnsignedBinary: Array[Int] = {

    if (unsignedBinary != null || binary == null) return unsignedBinary
    unsignedBinary = new Array[Int](binary.length)
    for (i <- 0 until binary.length) {
      unsignedBinary(i) = java.lang.Byte.toUnsignedInt(binary(i))
    }
    unsignedBinary
  }

  /**
   * @return textual representation of the stored LSN
   */
  override def toString: String = {
    if (string != null) return string
    val sb = new StringBuilder
    if (binary == null) return Lsn.NULL_STRING
    val unsigned = getUnsignedBinary
    for (i <- 0 until unsigned.length) {
      val byteStr = Integer.toHexString(unsigned(i))
      if (byteStr.length == 1) sb.append('0')
      sb.append(byteStr)
      if (i == 3 || i == 7) sb.append(':')
    }
    string = sb.toString
    string
  }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + util.Arrays.hashCode(binary)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass ne obj.getClass) return false
    val other = obj.asInstanceOf[Lsn]
    if (!Arrays.equals(binary, other.binary)) return false
    true
  }

  /**
   * Enables ordering of LSNs. The {@code NULL} LSN is always the smallest one.
   */
  override def compareTo(o: Lsn): Int = {
    if (this eq o) return 0
    if (!this.isAvailable) {
      if (!o.isAvailable) return 0
      return -1
    }
    if (!o.isAvailable) return 1
    val thisU = getUnsignedBinary
    val thatU = o.getUnsignedBinary
    for (i <- 0 until thisU.length) {
      val diff = thisU(i) - thatU(i)
      if (diff != 0) return diff
    }
    0
  }

  /**
   * Verifies whether the LSN falls into a LSN interval
   *
   * @param from start of the interval (included)
   * @param to   end of the interval (excluded)
   * @return true if the LSN falls into the interval
   */
  def isBetween(from: Lsn, to: Lsn): Boolean = this.compareTo(from) >= 0 && this.compareTo(to) < 0

  /**
   * Return the next LSN in sequence
   */
  def increment: Lsn = {
    val bi = new BigInteger(this.toString.replace(":", ""), 16).add(BigInteger.ONE)
    val biByteArray = bi.toByteArray
    val lsnByteArray = new Array[Byte](16)
    for (i <- 0 until biByteArray.length) {
      lsnByteArray(i + 16 - biByteArray.length) = biByteArray(i)
    }
    Lsn.valueOf(lsnByteArray)
  }
}
