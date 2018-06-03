package com.kuzdowicz.spark.examples

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MostPopularSuperheroes extends App {

  private val config: Config = ConfigFactory.load()

  private val inputPath = config.getString("app.inputPath")
  private val appName = config.getString("app.name")

  val spark = SparkSession.builder()
    .master("local")
    .appName(appName)
    .getOrCreate()

  spark.sparkContext.setLogLevel(Level.ERROR.toString)

  import spark.implicits._

  val heroesNames: RDD[(Int, String)] = spark.read.textFile(s"$inputPath/Marvel-names.txt")
    .flatMap(el => {
      val fields = el.split("\"")
      if (fields.length > 1) {
        Option((fields(0).trim.toInt, fields(1)))
      } else {
        None
      }
    }).rdd

  val heroesGraph = spark.read.textFile(s"$inputPath/Marvel-graph.txt")

  val heroesOccurrences = heroesGraph.map(line => {
    val ids = line.split(" ")
    (ids(0).toInt, ids.length - 1)
  }).rdd.reduceByKey((acc, v) => acc + v)

  val flipped = heroesOccurrences.map(t => (t._2, t._1))
  val mostPopular = flipped.max()

  println(s"---------------------------------------------------------")
  println(s"starting $appName")

  val result = s"the most popular hero is [${heroesNames.lookup(mostPopular._2).head}]" +
    s" with ${mostPopular._1} co-appearances"

  println(result)

}
