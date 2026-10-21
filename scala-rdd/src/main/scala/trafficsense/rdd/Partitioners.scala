package trafficsense.rdd

import org.apache.spark.Partitioner

/** Routes every observation of a traffic state to its own partition (free | slowed | congested | missing),
  * so state-specific work (e.g. statistics of congested speeds) runs per partition without another shuffle.
  * Key: the state name.
  */
class TrafficStatePartitioner extends Partitioner {
  private val index: Map[String, Int] = TrafficState.All.zipWithIndex.toMap

  override def numPartitions: Int = TrafficState.All.size

  override def getPartition(key: Any): Int = key match {
    case s: String => index.getOrElse(s, index(TrafficState.Missing))
    case other     => throw new IllegalArgumentException(s"TrafficStatePartitioner expects a state name, got $other")
  }

  override def equals(other: Any): Boolean = other.isInstanceOf[TrafficStatePartitioner]
  override def hashCode: Int = numPartitions
}

/** Assigns sensors to partitions so that the WORKLOAD (observations per sensor) is balanced.
  *
  * Observation frequencies are skewed - e.g. congested readings range from 0 to >10,000 per sensor - so
  * HashPartitioner(sensorId) can put several heavy sensors in one partition and leave others nearly empty.
  * This partitioner uses the classic greedy "longest processing time first" bin packing: sort sensors by
  * weight descending and always give the next sensor to the currently lightest partition.
  * Key: the sensor id (Int). Unknown sensors fall back to hashing.
  */
class ObservationFrequencyPartitioner(weights: Map[Int, Long], partitions: Int) extends Partitioner {
  require(partitions > 0, "partitions must be positive")

  val assignment: Map[Int, Int] = {
    val load = Array.fill(partitions)(0L)
    weights.toSeq.sortBy { case (sensor, w) => (-w, sensor) }.map { case (sensor, w) =>
      val target = load.indices.minBy(i => (load(i), i))
      load(target) += math.max(w, 1L)
      sensor -> target
    }.toMap
  }

  /** Expected records per partition, from the weights used to build the assignment. */
  def plannedLoad: Array[Long] = {
    val load = Array.fill(partitions)(0L)
    weights.foreach { case (s, w) => load(assignment(s)) += w }
    load
  }

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case s: Int => assignment.getOrElse(s, Math.floorMod(s.hashCode, partitions))
    case other  => throw new IllegalArgumentException(s"ObservationFrequencyPartitioner expects a sensor id, got $other")
  }

  override def equals(other: Any): Boolean = other match {
    case p: ObservationFrequencyPartitioner => p.numPartitions == numPartitions && p.assignment == assignment
    case _                                  => false
  }
  override def hashCode: Int = 31 * partitions + assignment.hashCode
}
