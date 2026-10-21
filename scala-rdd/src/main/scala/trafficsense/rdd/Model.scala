package trafficsense.rdd

import java.time.LocalDate

/** One 5-minute speed reading of one sensor, parsed from the wide PEMS-BAY CSV.
  * Compact primitive fields keep the Kryo-serialized cache small (~16 bytes per reading).
  *
  * @param day    local date as yyyymmdd
  * @param dow    ISO day of week, Monday = 1 ... Sunday = 7
  * @param speed  mph; 0.0 encodes a missing reading in the DCRNN export
  */
final case class Obs(sensor: Int, day: Int, hour: Byte, minute: Byte, dow: Byte, speed: Float) {
  def isValid: Boolean = speed > 0f
  def isWeekend: Boolean = dow >= 6
  def state: String = TrafficState.of(speed)
}

/** Traffic states shared with the PySpark code (src/preprocessing/congestion.py). */
object TrafficState {
  val CongestedMph = 35f   // Caltrans congestion threshold
  val FreeFlowMph  = 60f   // PeMS free-flow / delay threshold

  val Free = "free"
  val Slowed = "slowed"
  val Congested = "congested"
  val Missing = "missing"
  val All: Seq[String] = Seq(Free, Slowed, Congested, Missing)

  def of(speed: Float): String =
    if (!(speed > 0f)) Missing
    else if (speed < CongestedMph) Congested
    else if (speed < FreeFlowMph) Slowed
    else Free
}

object Parsing {

  /** Sensor ids from the header line ",400001,400017,...". */
  def sensorIds(header: String): Array[Int] = header.split(",", -1).drop(1).map(_.trim.toInt)

  /** Parse one wide CSV line into one Obs per sensor (this is what flatMap emits).
    * Returns None for a malformed line (wrong field count or unparseable values).
    */
  def parseLine(line: String, sensors: Array[Int]): Option[Array[Obs]] = {
    val f = line.split(",", -1)
    if (f.length != sensors.length + 1 || f(0).length < 16) return None
    try {
      val ts = f(0)
      val date = LocalDate.parse(ts.substring(0, 10))
      val day = date.getYear * 10000 + date.getMonthValue * 100 + date.getDayOfMonth
      val hour = ts.substring(11, 13).toByte
      val minute = ts.substring(14, 16).toByte
      val dow = date.getDayOfWeek.getValue.toByte
      val out = new Array[Obs](sensors.length)
      var i = 0
      while (i < sensors.length) {
        val raw = f(i + 1).trim
        out(i) = Obs(sensors(i), day, hour, minute, dow, if (raw.isEmpty) 0f else raw.toFloat)
        i += 1
      }
      Some(out)
    } catch {
      case _: Exception => None
    }
  }

  /** "400001,37.364085,-121.901149" -> (400001, (lat, lon)). */
  def parseLocation(line: String): Option[(Int, (Double, Double))] = {
    val f = line.split(",", -1).map(_.trim)
    if (f.length < 3) None
    else scala.util.Try((f(0).toInt, (f(1).toDouble, f(2).toDouble))).toOption
  }
}
