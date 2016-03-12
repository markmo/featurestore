package diamond.utility

import java.math.BigInteger
import java.security.MessageDigest

import net.openhft.hashing.LongHashFunction

/**
  * Created by markmo on 27/02/2016.
  */
object hashFunctions {

  /**
    * Hashes a string key using MD5.
    *
    * Used to hash entity keys, which may be composite.
    *
    * MD5 is widely supported.
    *
    * The number of inserts to get a 50% chance of a hash collision for a hash
    * of N hex characters is approximately
    *
    *   `0.5 + sqrt(0.25 - (2 * log(0.5) * 16 ** N))`
    *
    * http://danlinstedt.com/allposts/datavaultcat/datavault-2-0-hashes-versus-natural-keys/
    *
    * @param key String
    * @return String hashed key
    */
  def hashKey(key: String) = {
    val md = MessageDigest.getInstance("MD5")
    md.update(key.getBytes("UTF-8"))
    val digest = md.digest()
    String.format("%032x", new BigInteger(1, digest))
  }

  /**
    * Hashes a string key using SHA-256.
    *
    * Used to hash entity keys, which may be composite.
    *
    * @param key String
    * @return String hashed key
    */
  def sha256HashKey(key: String) = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(key.getBytes("UTF-8"))
    val digest = md.digest()
    String.format("%064x", new BigInteger(1, digest))
  }

  /**
    * Fast hashing for change data capture.
    * Uses the xxHash algorithm.
    *
    * @see https://github.com/OpenHFT/Zero-Allocation-Hashing
    * @param str String
    * @return String hashed
    */
  def fastHash(str: String) =
    LongHashFunction.xx_r39().hashChars(str).toString

}
