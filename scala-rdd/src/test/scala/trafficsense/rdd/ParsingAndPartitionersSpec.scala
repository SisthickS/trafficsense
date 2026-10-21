package trafficsense.rdd

import org.scalatest.funsuite.AnyFunSuite

class ParsingAndPartitionersSpec extends AnyFunSuite {

  private val sensors = Array(400001, 400017, 400030)

  test("parseLine emits one Obs per sensor with calendar fields") {
    val out = Parsing.parseLine("2017-03-13 17:05:00,71.4,0.0,30.5", sensors).get
    assert(out.length == 3)
    assert(out.map(_.sensor).toSeq == sensors.toSeq)
    val o = out(2)
    assert(o.day == 20170313 && o.hour == 17 && o.minute == 5 && o.dow == 1)   // Monday
    assert(o.speed == 30.5f && o.state == TrafficState.Congested)
    assert(!out(1).isValid && out(1).state == TrafficState.Missing)
    assert(out(0).state == TrafficState.Free)
  }

  test("parseLine rejects malformed lines") {
    assert(Parsing.parseLine("2017-03-13 17:05:00,71.4,0.0", sensors).isEmpty)       // missing field
    assert(Parsing.parseLine("not-a-date,1,2,3", sensors).isEmpty)
    assert(Parsing.parseLine("2017-03-13 17:05:00,x,1,2", sensors).isEmpty)
  }

  test("traffic state thresholds match the PySpark definition") {
    assert(TrafficState.of(34.9f) == TrafficState.Congested)
    assert(TrafficState.of(35f) == TrafficState.Slowed)
    assert(TrafficState.of(59.9f) == TrafficState.Slowed)
    assert(TrafficState.of(60f) == TrafficState.Free)
    assert(TrafficState.of(0f) == TrafficState.Missing)
    assert(TrafficState.of(Float.NaN) == TrafficState.Missing)
  }

  test("TrafficStatePartitioner gives each state its own partition") {
    val p = new TrafficStatePartitioner
    assert(p.numPartitions == 4)
    assert(TrafficState.All.map(p.getPartition).distinct.size == 4)
    assert(p == new TrafficStatePartitioner)
  }

  test("ObservationFrequencyPartitioner balances skewed weights better than hashing") {
    val weights: Map[Int, Long] = (1 to 40).map(s => s -> (if (s % 10 == 0) 1000L else 10L)).toMap
    val p = new ObservationFrequencyPartitioner(weights, 4)
    val load = p.plannedLoad
    assert(load.sum == weights.values.sum)
    val hashLoad = Array.fill(4)(0L)
    weights.foreach { case (s, w) => hashLoad(Math.floorMod(s.hashCode, 4)) += w }
    assert(load.max <= hashLoad.max)
    assert(load.max - load.min <= weights.values.max)                        // LPT guarantee
    assert(weights.keys.forall(s => p.getPartition(s) == p.assignment(s)))
    assert(p.getPartition(999999) >= 0 && p.getPartition(999999) < 4)       // unknown sensor -> hash fallback
  }

  test("episode detection counts runs of consecutive 5-minute slots") {
    def obs(sensor: Int, hour: Int, minute: Int) = (sensor, Obs(sensor, 20170313, hour.toByte, minute.toByte, 1, 20f))
    val readings = Seq(obs(1, 8, 0), obs(1, 8, 5), obs(1, 8, 10), obs(1, 9, 0), obs(2, 7, 55), obs(2, 8, 0))
    val Seq((sensorsSeen, episodes, longest)) = TrafficRddDemo.episodesInPartition(readings.iterator).toSeq
    assert(sensorsSeen == 2 && episodes == 3 && longest == 15)
  }
}
