package trafficsense.rdd

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter
import org.apache.spark.{HashPartitioner, Partitioner, ShuffleDependency, SparkConf, SparkContext}

/** Spark Core (RDD API) demonstration on the raw PEMS-BAY wide CSV.
  *
  *  1. RDD creation           textFile, parallelize, header handling with mapPartitionsWithIndex
  *  2. Transformations        flatMap (1 wide line -> 325 readings), map, filter; accumulators; broadcast
  *  3. Pair RDDs              reduceByKey, mapValues, join, sortByKey, takeOrdered
  *  4. Distributed aggregates aggregateByKey(StatCounter), combineByKey, stats(), histogram()
  *  5. Frequency profiles     countByValue, per-sensor traffic-state profiles, co-congestion frequency
  *  6. Custom partitioners    TrafficStatePartitioner, ObservationFrequencyPartitioner vs HashPartitioner (timed)
  *  7. Persistence & lineage  timed cache vs recompute, toDebugString, checkpoint()
  *  8. Output                 saveAsTextFile
  *
  * Usage: spark-submit --class trafficsense.rdd.TrafficRddDemo <jar> <readings.csv> <locations.csv> <report-dir> <scratch-dir> [partitions]
  */
object TrafficRddDemo {
  import TrafficState._

  private def timed[T](body: => T): (T, Double) = {
    val t0 = System.nanoTime()
    val result = body
    (result, (System.nanoTime() - t0) / 1e9)
  }

  /** Records per partition (computed by an action over mapPartitionsWithIndex). */
  def partitionSizes[T](rdd: RDD[T]): Array[Long] =
    rdd.mapPartitionsWithIndex((i, it) => Iterator((i, it.size.toLong))).collect().sortBy(_._1).map(_._2)

  def maxOverMean(sizes: Array[Long]): Double = {
    val mean = sizes.sum.toDouble / sizes.length
    if (mean > 0) sizes.max / mean else 0.0
  }

  private def isShuffle(rdd: RDD[_]): Boolean = rdd.dependencies.exists(_.isInstanceOf[ShuffleDependency[_, _, _]])

  private def minuteIndex(o: Obs): Long =
    LocalDate.of(o.day / 10000, (o.day / 100) % 100, o.day % 100).toEpochDay * 1440L + o.hour * 60L + o.minute

  /** Per-partition episode detection: group a partition's congested readings by sensor, sort by time,
    * count runs of consecutive 5-minute slots. Returns (sensors, episodes, longest episode in minutes). */
  def episodesInPartition(it: Iterator[(Int, Obs)]): Iterator[(Int, Long, Long)] = {
    val bySensor = it.toSeq.groupBy(_._1)
    var episodes = 0L
    var longest = 0L
    bySensor.values.foreach { readings =>
      val times = readings.map(r => minuteIndex(r._2)).sorted
      var runStart = times.head
      var prev = times.head
      episodes += 1
      times.tail.foreach { t =>
        if (t - prev != 5) { longest = math.max(longest, prev - runStart + 5); runStart = t; episodes += 1 }
        prev = t
      }
      longest = math.max(longest, prev - runStart + 5)
    }
    Iterator((bySensor.size, episodes, longest))
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("usage: TrafficRddDemo <readings.csv> <locations.csv> <report-dir> <scratch-dir> [partitions]")
      sys.exit(2)
    }
    val Array(readingsPath, locationsPath, reportDir, scratchDir) = args.take(4)
    val conf = new SparkConf()
      .setAppName("TrafficSense-RDD")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Obs], classOf[Array[Obs]]))
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val numParts = if (args.length > 4) args(4).toInt else sc.defaultParallelism
    sc.setCheckpointDir(Paths.get(scratchDir, "checkpoints").toString)
    try run(sc, readingsPath, locationsPath, reportDir, scratchDir, numParts)
    finally sc.stop()
  }

  def run(sc: SparkContext, readingsPath: String, locationsPath: String, reportDir: String, scratchDir: String,
          numParts: Int): Unit = {
    val r = new Report("Scala RDD demonstration — PEMS-BAY (raw wide CSV)")
    val started = System.nanoTime()
    r.p(s"Spark ${sc.version} · Scala ${scala.util.Properties.versionNumberString} · master `${sc.master}` · " +
      s"default parallelism ${sc.defaultParallelism} · partitions used $numParts · Kryo serializer · input `$readingsPath`")

    // ------------------------------------------------------------------ 1. RDD creation
    val lines = sc.textFile(readingsPath, numParts)
    val header = lines.first()
    val sensors = Parsing.sensorIds(header)
    val bSensors = sc.broadcast(sensors)
    val body = lines.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
    val locations = sc.textFile(locationsPath).flatMap(Parsing.parseLocation)
    val hoursRdd = sc.parallelize(0 until 24, 4)
    val nLines = body.count()
    val nLocations = locations.count()
    r.h2("1. RDD creation")
    r.table(Seq("RDD", "Created with", "Partitions", "Records"), Seq(
      Seq("raw lines", "sc.textFile(csv, minPartitions)", lines.getNumPartitions, nLines + 1),
      Seq("data lines", "mapPartitionsWithIndex (drop header in partition 0)", body.getNumPartitions, nLines),
      Seq("sensor locations (pair RDD)", "sc.textFile + flatMap(parseLocation)", locations.getNumPartitions, nLocations),
      Seq("hours 0-23", "sc.parallelize(0 until 24, 4)", hoursRdd.getNumPartitions, hoursRdd.count())))
    r.p(s"The header lists **${sensors.length} sensors**. Their id array is shared with tasks as a **broadcast variable** " +
      "(sent once per executor instead of once per task).")
    r.metric("lines", nLines); r.metric("sensors", sensors.length); r.metric("locations", nLocations)

    // ------------------------------------------------------------------ 2. flatMap / map / filter + accumulators
    val malformed = sc.longAccumulator("malformed lines")
    val obs: RDD[Obs] = body.flatMap { line =>
      Parsing.parseLine(line, bSensors.value) match {
        case Some(readings) => readings
        case None           => malformed.add(1); Array.empty[Obs]
      }
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val (nObs, tMaterialise) = timed(obs.count())
    val zeroAcc = sc.longAccumulator("zero/missing readings")
    obs.foreach(o => if (!o.isValid) zeroAcc.add(1))
    val valid = obs.filter(_.isValid)
    val nValid = valid.count()
    val speeds = valid.map(_.speed.toDouble)
    r.h2("2. Transformations: flatMap, map, filter (+ accumulators)")
    r.table(Seq("Step", "Operation", "Records"), Seq(
      Seq("wide lines", "textFile", nLines),
      Seq("readings", s"flatMap: 1 line → ${sensors.length} Obs", nObs),
      Seq("valid readings", "filter(speed > 0)", nValid),
      Seq("zero readings", "accumulator inside foreach (action)", zeroAcc.value),
      Seq("malformed lines", "accumulator inside flatMap", malformed.value)))
    r.p(f"Materialising and caching the readings (`persist(MEMORY_AND_DISK_SER)`, Kryo) took **$tMaterialise%.1f s**. " +
      "Accumulators updated inside an *action* (`foreach`) are counted exactly once. Inside a *transformation* they can " +
      "over-count if a partition is recomputed; here the readings are cached, so each partition is computed once.")
    r.metric("observations", nObs); r.metric("valid", nValid); r.metric("zero_readings", zeroAcc.value)
    r.metric("malformed_lines", malformed.value); r.metric("materialise_seconds", tMaterialise)

    // ------------------------------------------------------------------ 3. Pair RDDs: reduceByKey, join, sort
    val hourly = valid
      .map(o => ((o.isWeekend, o.hour.toInt), (if (o.speed < CongestedMph) 1L else 0L, 1L)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (c, n) => c.toDouble / n }
    val hourlyMap = hourly.collectAsMap()
    val (peakHour, peakRate) = (0 until 24).map(h => (h, hourlyMap((false, h)))).maxBy(_._2)
    val congestedPerSensor = valid.map(o => (o.sensor, if (o.speed < CongestedMph) 1L else 0L)).reduceByKey(_ + _)
    val readingsPerSensor = valid.map(o => (o.sensor, 1L)).reduceByKey(_ + _)
    val ratePerSensor = congestedPerSensor.join(readingsPerSensor).mapValues { case (c, n) => c.toDouble / n }
    val top = ratePerSensor.join(locations).takeOrdered(10)(Ordering.by((x: (Int, (Double, (Double, Double)))) => (-x._2._1, x._1)))
    val bLoc = sc.broadcast(locations.collectAsMap())
    val topBroadcast = ratePerSensor.map { case (s, rate) => (s, (rate, bLoc.value.get(s))) }
      .takeOrdered(10)(Ordering.by((x: (Int, (Double, Option[(Double, Double)]))) => (-x._2._1, x._1)))
    val joinsAgree = top.map(_._1).sameElements(topBroadcast.map(_._1))
    r.h2("3. Pair RDDs: reduceByKey, mapValues, join, takeOrdered")
    r.p("Congestion rate (share of valid readings < 35 mph) by hour, from `map → reduceByKey → mapValues`:")
    r.table(Seq("Hour") ++ (0 until 24 by 3).map(h => f"$h%02d"), Seq(
      Seq("weekday") ++ (0 until 24 by 3).map(h => f"${100 * hourlyMap((false, h))}%.1f%%"),
      Seq("weekend") ++ (0 until 24 by 3).map(h => f"${100 * hourlyMap((true, h))}%.1f%%")))
    r.p(f"Weekday peak hour: **$peakHour%02d:00** with **${100 * peakRate}%.1f%%** of readings congested.")
    r.p("Top 10 most congested sensors (`reduceByKey` counts, `join` with readings and with the locations pair RDD, `takeOrdered`):")
    r.table(Seq("Sensor", "Congested share", "Latitude", "Longitude"),
      top.toSeq.map { case (s, (rate, (lat, lon))) => Seq(s.toString, f"${100 * rate}%.2f%%", lat, lon) })
    r.p(s"Map-side lookup with a broadcast location map gives the same top 10 without a shuffle join: **$joinsAgree**.")
    r.metric("weekday_peak_hour", peakHour); r.metric("weekday_peak_rate", peakRate)
    r.metric("top_sensor", top.head._1); r.metric("broadcast_join_agrees", joinsAgree)

    // ------------------------------------------------------------------ 4. Distributed aggregations
    val pairs = valid.map(o => (o.sensor, o.speed.toDouble))
    val statsBySensor = pairs.aggregateByKey(new StatCounter())((s, v) => s.merge(v), (a, b) => a.merge(b))
    val meanByCombine = pairs.combineByKey(
      (v: Double) => (v, 1L),
      (acc: (Double, Long), v: Double) => (acc._1 + v, acc._2 + 1L),
      (a: (Double, Long), b: (Double, Long)) => (a._1 + b._1, a._2 + b._2)).mapValues { case (s, n) => s / n }
    val maxDiff = statsBySensor.mapValues(_.mean).join(meanByCombine).map { case (_, (a, b)) => math.abs(a - b) }.max()
    val global = speeds.stats()
    val buckets = (0 to 90 by 10).map(_.toDouble).toArray
    val histogram = speeds.histogram(buckets)
    val slowest = statsBySensor.takeOrdered(5)(Ordering.by((x: (Int, StatCounter)) => (x._2.mean, x._1)))
    r.h2("4. Distributed aggregations: aggregateByKey(StatCounter), combineByKey, stats(), histogram()")
    r.p(f"Network: n = ${global.count}%,d, mean ${global.mean}%.2f mph, stdev ${global.stdev}%.2f, min ${global.min}%.1f, max ${global.max}%.1f.")
    r.table(Seq("Speed bucket (mph)", "Readings"), buckets.sliding(2).toSeq.zip(histogram).map { case (b, n) =>
      Seq(f"${b(0)}%.0f–${b(1)}%.0f", n) })
    r.p("Five slowest sensors by mean speed (`aggregateByKey` with a `StatCounter` per key, one pass, map-side combined):")
    r.table(Seq("Sensor", "Mean", "Stdev", "Min", "Max", "Readings"),
      slowest.toSeq.map { case (s, st) => Seq(s.toString, st.mean, st.stdev, st.min, st.max, st.count) })
    r.p(f"`combineByKey` (sum, count) means agree with `aggregateByKey` StatCounter means: max |difference| = $maxDiff%.2e mph.")
    r.metric("global_mean_speed", global.mean); r.metric("aggregate_vs_combine_max_diff", maxDiff)

    // ------------------------------------------------------------------ 5. Frequency profiles
    val stateFreq = obs.map(_.state).countByValue()
    val profiles = obs.map(o => ((o.sensor, o.state), 1L)).reduceByKey(_ + _)
      .map { case ((s, st), n) => (s, Map(st -> n)) }
      .reduceByKey(_ ++ _)
    val congestedShare = profiles.mapValues(m => m.getOrElse(Congested, 0L).toDouble / m.values.sum).values.collect()
    def bucketOf(x: Double): String =
      if (x == 0) "0%" else if (x <= 0.01) "(0, 1%]" else if (x <= 0.05) "(1, 5%]" else if (x <= 0.10) "(5, 10%]" else "> 10%"
    val shareHist = congestedShare.groupBy(bucketOf).mapValues(_.length)
    val validPerSensor = readingsPerSensor.values.collect()
    val hourState = valid.map(o => ((o.hour.toInt, o.state), 1L)).reduceByKey(_ + _).collectAsMap()
    val coCongestion = valid.filter(_.speed < CongestedMph)
      .map(o => ((o.day, o.hour, o.minute), 1)).reduceByKey(_ + _).values
    val coStats = coCongestion.stats()
    val nTimestamps = nLines
    r.h2("5. Frequency profiles")
    r.table(Seq("Traffic state", "Readings", "Share"), All.map { st =>
      val n = stateFreq.getOrElse(st, 0L); Seq(st, n, f"${100.0 * n / nObs}%.2f%%") })
    r.p("Per-sensor traffic-state profile (`((sensor, state), 1) → reduceByKey → per-sensor Map`): sensors by share of congested readings:")
    r.table(Seq("Congested share", "Sensors"), Seq("0%", "(0, 1%]", "(1, 5%]", "(5, 10%]", "> 10%").map(b => Seq(b, shareHist.getOrElse(b, 0))))
    r.p(f"Observation frequency: valid readings per sensor range ${validPerSensor.min}%,d – ${validPerSensor.max}%,d " +
      f"(of $nTimestamps%,d timestamps).")
    r.table(Seq("Hour") ++ Seq(Free, Slowed, Congested), (0 until 24 by 3).map { h =>
      val tot = Seq(Free, Slowed, Congested).map(st => hourState.getOrElse((h, st), 0L)).sum.toDouble
      Seq(f"$h%02d:00") ++ Seq(Free, Slowed, Congested).map(st => f"${100 * hourState.getOrElse((h, st), 0L) / tot}%.1f%%")
    })
    r.p(f"Co-congestion: in ${coStats.count}%,d of $nTimestamps%,d timestamps (${100.0 * coStats.count / nTimestamps}%.1f%%) " +
      f"at least one sensor is congested; on those, on average ${coStats.mean}%.1f sensors at once (max ${coStats.max}%.0f).")
    r.metric("state_frequency", All.map(st => st -> stateFreq.getOrElse(st, 0L)).toMap)
    r.metric("co_congestion_max_sensors", coStats.max)

    // ------------------------------------------------------------------ 6. Custom partitioners
    val stateP = new TrafficStatePartitioner
    val byState = obs.map(o => (o.state, o.speed)).partitionBy(stateP).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val stateSizes = partitionSizes(byState)
    val purity = byState.mapPartitionsWithIndex((i, it) => Iterator((i, it.map(_._1).toSet))).collect().sortBy(_._1)
    val pure = purity.forall(_._2.size <= 1)
    val perStateStats = byState.mapPartitions { it =>
      val st = new StatCounter(); var name = ""
      it.foreach { case (k, v) => name = k; st.merge(v.toDouble) }
      if (st.count > 0) Iterator((name, st)) else Iterator.empty
    }.collect().toMap
    val narrowCount = byState.mapValues(_ => 1L).reduceByKey(_ + _)
    val shuffledCount = obs.map(o => (o.state, 1L)).reduceByKey(_ + _)
    val (_, tNarrow) = timed(narrowCount.collect())
    val (_, tShuffled) = timed(shuffledCount.collect())
    r.h2("6. Custom partitioners")
    r.p("**TrafficStatePartitioner**: one partition per traffic state, via `partitionBy` on `(state, speed)`.")
    r.table(Seq("Partition", "State", "Records", "Mean speed (computed per partition, no shuffle)"),
      All.indices.map { i =>
        val st = All(i); Seq(i, st, stateSizes(i), perStateStats.get(st).map(_.mean).getOrElse(Double.NaN)) })
    r.p(s"Every partition holds exactly one state: **$pure**. Because `mapValues` keeps the partitioner, a later " +
      s"`reduceByKey` is a **narrow** dependency (ShuffleDependency: ${isShuffle(narrowCount)}, ${f"$tNarrow%.2f"} s) " +
      s"instead of a new shuffle on unpartitioned data (ShuffleDependency: ${isShuffle(shuffledCount)}, ${f"$tShuffled%.2f"} s).")
    val stateShare = stateSizes.max.toDouble / stateSizes.sum
    r.p(if (tNarrow > tShuffled)
      f"**Avoiding the shuffle did not make it faster here.** The state partitioner creates only ${stateSizes.length} " +
      f"partitions and the largest holds ${100 * stateShare}%.0f%% of all readings, so one task does most of the work while " +
      f"the other cores idle. The shuffled version runs ${obs.getNumPartitions} map tasks with map-side combining. " +
      "A partitioner must also **balance** the data, which is what the frequency-aware partitioner below does."
    else
      f"The co-partitioned reduce avoids the shuffle and is faster, although the largest state partition holds " +
      f"${100 * stateShare}%.0f%% of the readings, so balance still limits parallelism.")
    r.metric("state_partition_largest_share", stateShare)
    r.metric("narrow_reduce_seconds", tNarrow); r.metric("shuffled_reduce_seconds", tShuffled)
    r.metric("state_partition_pure", pure); r.metric("state_partition_sizes", stateSizes.toSeq)
    r.metric("partitioned_reduce_is_shuffle", isShuffle(narrowCount)); r.metric("plain_reduce_is_shuffle", isShuffle(shuffledCount))
    byState.unpersist()

    val weights = congestedPerSensor.collectAsMap().toMap
    val congestedObs = valid.filter(_.speed < CongestedMph).map(o => (o.sensor, o)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    congestedObs.count()
    val freqP = new ObservationFrequencyPartitioner(weights, numParts)
    val hashP = new HashPartitioner(numParts)
    def runWith(p: Partitioner): (Array[Long], Double, (Long, Long, Long)) = {
      val parted = congestedObs.partitionBy(p).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val sizes = partitionSizes(parted)                            // also materialises the shuffle
      val (res, secs) = timed(parted.mapPartitions(episodesInPartition).collect())
      parted.unpersist()
      (sizes, secs, res.foldLeft((0L, 0L, 0L)) { case ((a, b, c), (x, y, z)) => (a + x, b + y, math.max(c, z)) })
    }
    val (hashSizes, hashSecs, hashRes) = runWith(hashP)
    val (freqSizes, freqSecs, freqRes) = runWith(freqP)
    congestedObs.unpersist()
    r.p(s"**ObservationFrequencyPartitioner**: the workload is the **${weights.values.sum}** congested readings, keyed by " +
      s"sensor and very skewed (per sensor: min ${weights.values.min}, max ${weights.values.max}). Greedy bin packing on " +
      "those frequencies vs `HashPartitioner(sensorId)`, followed by per-partition episode detection (group by sensor, sort by time):")
    r.table(Seq("Partitioner", "Partitions", "Min records", "Max records", "Max / mean", "Episode pass (s)", "Episodes", "Longest (min)"), Seq(
      Seq("HashPartitioner", numParts, hashSizes.min, hashSizes.max, maxOverMean(hashSizes), hashSecs, hashRes._2, hashRes._3),
      Seq("ObservationFrequencyPartitioner", numParts, freqSizes.min, freqSizes.max, maxOverMean(freqSizes), freqSecs, freqRes._2, freqRes._3)))
    r.p("A stage finishes when its slowest task does, so the largest partition sets the runtime. Both partitioners find the " +
      s"same episodes: **${hashRes == freqRes}**.")
    r.metric("hash_max_over_mean", maxOverMean(hashSizes)); r.metric("freq_max_over_mean", maxOverMean(freqSizes))
    r.metric("hash_episode_seconds", hashSecs); r.metric("freq_episode_seconds", freqSecs)
    r.metric("episodes", freqRes._2); r.metric("partitioners_agree", hashRes == freqRes)

    // ------------------------------------------------------------------ 7. Persistence, lineage, checkpoint
    def hourlyCongestion(rdd: RDD[Obs]): Array[(Int, Double)] = rdd.filter(_.isValid)
      .map(o => (o.hour.toInt, (if (o.speed < CongestedMph) 1L else 0L, 1L)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues { case (c, n) => c.toDouble / n }.collect()
    val parse = (line: String) => Parsing.parseLine(line, bSensors.value).getOrElse(Array.empty[Obs])
    def threeRuns(rdd: RDD[Obs]): Seq[Double] = (1 to 3).map(_ => timed(hourlyCongestion(rdd))._2)
    val strategies = Seq("none (re-parse CSV)" -> None, "MEMORY_ONLY (deserialized objects)" -> Some(StorageLevel.MEMORY_ONLY),
                         "MEMORY_AND_DISK_SER (Kryo bytes)" -> Some(StorageLevel.MEMORY_AND_DISK_SER))
    val cacheRows = strategies.map { case (label, level) =>
      val rdd = body.flatMap(line => parse(line))
      level.foreach(l => rdd.persist(l))
      val times = threeRuns(rdd)
      val size = sc.getRDDStorageInfo.find(_.id == rdd.id).map(i => (i.memSize + i.diskSize) / 1048576.0).getOrElse(0.0)
      if (level.isDefined) rdd.unpersist(blocking = true)
      (label, times, size)
    }
    val reuseNone = cacheRows.head._2.drop(1).sum / 2
    val (bestLabel, bestTimes, _) = cacheRows.tail.minBy(_._2.drop(1).sum)
    val reuseBest = bestTimes.drop(1).sum / 2
    val dailyByHour = valid.map(o => ((o.sensor, o.day), if (o.speed < CongestedMph) 1L else 0L)).reduceByKey(_ + _)
    val lineageBefore = dailyByHour.toDebugString
    dailyByHour.persist(StorageLevel.MEMORY_AND_DISK_SER)
    dailyByHour.checkpoint()
    val (nDaily, tCheckpoint) = timed(dailyByHour.count())
    val lineageAfter = dailyByHour.toDebugString
    r.h2("7. Persistence, lineage and checkpointing (timed)")
    r.p("Workload: hourly congestion rate over all 16.9M readings, run 3 times on a freshly built RDD per strategy. " +
      "Run 1 includes parsing (and storing, when cached); runs 2–3 measure reuse.")
    r.table(Seq("Strategy", "Run 1 (s)", "Run 2 (s)", "Run 3 (s)", "Cached size (MB)"),
      cacheRows.map { case (label, t, size) => Seq(label, t(0), t(1), t(2), if (size > 0) size else Double.NaN) })
    r.p(if (reuseBest < reuseNone)
      f"Reuse with **$bestLabel** takes ${reuseBest}%.2f s vs ${reuseNone}%.2f s re-parsing (**${reuseNone / reuseBest}%.1f× faster**). " +
      "Deserialized objects are fastest to reuse but largest. Kryo-serialized storage is about 2–3× smaller but must be " +
      "deserialized on every access."
    else
      f"Re-parsing (${reuseNone}%.2f s) is as fast as reading the best cache ($bestLabel, ${reuseBest}%.2f s). The 85 MB CSV " +
      "sits in the OS page cache and parsing it is cheap, so caching pays off only for more expensive lineages (shuffles, joins).")
    r.p("Lineage (`toDebugString`) of per-sensor-day congestion counts **before** checkpointing:")
    r.code(lineageBefore)
    r.p(f"After `checkpoint()` (reliable, written to `${Paths.get(scratchDir, "checkpoints")}`), the first action computed and " +
      f"saved $nDaily%,d records in $tCheckpoint%.2f s. isCheckpointed = **${dailyByHour.isCheckpointed}**. The lineage is now " +
      "cut at the checkpoint, so recovery after a failure re-reads the checkpoint instead of re-parsing the CSV:")
    r.code(lineageAfter)
    r.metric("cache_strategies", cacheRows.map { case (l, t, size) => Map("strategy" -> l, "seconds" -> t, "cached_mb" -> size) })
    r.metric("reuse_seconds_no_cache", reuseNone); r.metric("reuse_seconds_best_cache", reuseBest)
    r.metric("checkpointed", dailyByHour.isCheckpointed); r.metric("checkpoint_seconds", tCheckpoint)

    // ------------------------------------------------------------------ 8. Output with saveAsTextFile
    val outDir = Paths.get(reportDir, "sensor_profiles_csv").toString
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new HPath(outDir), true)
    statsBySensor.join(profiles)
      .map { case (s, (st, prof)) =>
        (s, f"$s,${st.count},${st.mean}%.3f,${st.stdev}%.3f,${st.min}%.1f,${st.max}%.1f," +
          All.map(x => prof.getOrElse(x, 0L)).mkString(",")) }
      .sortByKey().values.coalesce(1)
      .saveAsTextFile(outDir)
    r.h2("8. Output")
    r.p(s"Per-sensor statistics and traffic-state profiles were written with `sortByKey → coalesce(1) → saveAsTextFile` to " +
      s"`$outDir/part-00000`. Columns: `sensor,valid,mean,stdev,min,max,${All.mkString(",")}`.")
    dailyByHour.unpersist()
    obs.unpersist()
    val total = (System.nanoTime() - started) / 1e9
    r.p(f"Total runtime: $total%.1f s.")
    r.metric("total_seconds", total)
    val (md, json) = r.write(Paths.get(reportDir), "scala_rdd_report")
    println(s"[TrafficRddDemo] report: $md")
    println(s"[TrafficRddDemo] metrics: $json")
  }
}
