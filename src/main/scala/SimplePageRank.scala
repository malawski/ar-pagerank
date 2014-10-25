
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimplePageRank {
  def main(args: Array[String]) {
    val ITERATIONS = 10

    val conf = new SparkConf().setAppName("Simple PageRank")
    val sc = new SparkContext(conf)


    //  Prepare data
    val linksData = Array(("a", "c"), ("b", "a"), ("c", "a"), ("c", "d"), ("d", "a"), ("d", "b"))

    //  RDD of (url, url) pairs
    //  RDD[(String, String)]
    val linksRDD = sc.parallelize(linksData)

    //  RDD of (url, neighbors) pairs
    //  RDD[(String, Iterable[String])]
    val links = linksRDD.distinct().groupByKey().cache()

    // RDD of (url, rank) pairs
    // RDD[(String, Double)]
    // Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDD's partitioning.
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to ITERATIONS) {
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    // Return an array that contains all of the elements in this RDD.
    val output = ranks.collect()

    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    sc.stop()
  }
}
