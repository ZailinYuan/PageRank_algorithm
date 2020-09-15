import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object pageRank {
  def main(args: Array[String]): Unit = {

    // Constants:
    val path = "C:\\Users\\zailinyuan\\IdeaProjects\\pageRank\\file\\airlines.csv"
    val a = 0.15
    val rounds = 10

    // Environment:
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Page Rank")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // Loading data:
    val airlines: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    // Convert to RDD:
    val lines: RDD[(String, String)] =
      airlines.rdd.map(row => (row.getAs("ORIGIN").toString, row.getAs("DEST").toString))

    // Get adjacent list:
    val links: RDD[(String, Iterable[String])] = lines.groupByKey().cache()

    // Get total number of airports:
    val N: Int = lines.count().toInt

    // Get initial page rank for each airport:
    var ranks: RDD[(String, Double)] = links.distinct.mapValues(v => 10.0)

    for (i <- 1 to rounds) {
      // (Destinations, rank) for each airport:
      val contribs: RDD[(String, Double)] = links.join(ranks).values.flatMap {
        case (dests, rank) =>
          val size: Int = dests.size
          dests.map(dest => (dest, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 / N + 0.85 * _)
    }

    ranks.sortBy(-_._2).collect().foreach(println(_))
    spark.stop
  }
}
