package trafficsense.rdd

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.collection.mutable.ArrayBuffer

/** Collects sections, tables and key/value results; writes a Markdown report and a flat JSON file. */
class Report(title: String) {
  private val md = ArrayBuffer[String](s"# $title", "")
  private val json = ArrayBuffer[(String, String)]()

  def h2(text: String): Unit = md ++= Seq(s"## $text", "")
  def p(text: String): Unit = md ++= Seq(text, "")
  def bullet(items: Seq[String]): Unit = { md ++= items.map("- " + _); md += "" }
  def code(text: String): Unit = md ++= Seq("```", text.trim, "```", "")

  def table(headers: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    md += headers.mkString("| ", " | ", " |")
    md += headers.map(_ => "---").mkString("|", "|", "|")
    rows.foreach(r => md += r.map(Report.fmt).mkString("| ", " | ", " |"))
    md += ""
  }

  def metric(key: String, value: Any): Unit = json += key -> Report.jsonValue(value)

  def write(dir: Path, stem: String): (Path, Path) = {
    Files.createDirectories(dir)
    val mdPath = dir.resolve(s"$stem.md")
    val jsonPath = dir.resolve(s"$stem.json")
    Files.write(mdPath, md.mkString("\n").getBytes(StandardCharsets.UTF_8))
    val body = json.map { case (k, v) => s"""  "${Report.escape(k)}": $v""" }.mkString("{\n", ",\n", "\n}\n")
    Files.write(jsonPath, body.getBytes(StandardCharsets.UTF_8))
    (mdPath, jsonPath)
  }
}

object Report {
  def fmt(v: Any): String = v match {
    case d: Double if d.isNaN => "-"
    case d: Double            => if (math.abs(d) >= 1000) f"$d%,.0f" else f"$d%.3f"
    case f: Float             => fmt(f.toDouble)
    case l: Long              => f"$l%,d"
    case i: Int               => f"$i%,d"
    case other                => String.valueOf(other)
  }

  def escape(s: String): String = s.flatMap {
    case '"'  => "\\\""
    case '\\' => "\\\\"
    case '\n' => "\\n"
    case c    => c.toString
  }

  def jsonValue(v: Any): String = v match {
    case null                       => "null"
    case d: Double if d.isNaN       => "null"
    case n @ (_: Int | _: Long | _: Double | _: Float | _: Short | _: Byte) => n.toString
    case b: Boolean                 => b.toString
    case m: Map[_, _]               => m.map { case (k, x) => s""""${escape(k.toString)}": ${jsonValue(x)}""" }.mkString("{", ", ", "}")
    case s: Seq[_]                  => s.map(jsonValue).mkString("[", ", ", "]")
    case a: Array[_]                => jsonValue(a.toSeq)
    case other                      => s""""${escape(other.toString)}""""
  }
}
