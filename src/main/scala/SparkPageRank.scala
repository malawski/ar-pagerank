/**
 * Created by malawski on 10/25/14.
 */

  import org.apache.spark.SparkContext._
  import org.apache.spark.{SparkConf, SparkContext}

  /**
   * Computes the PageRank of URLs from an input file. Input file should
   * be in format of:
   * URL         neighbor URL
   * URL         neighbor URL
   * URL         neighbor URL
   * ...
   * where URL and their neighbors are separated by space(s).
   */
  object SparkPageRank {
    def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: SparkPageRank <file> <iter>")
        System.exit(1)
      }
      val sparkConf = new SparkConf().setAppName("PageRank")
      val iters = if (args.length > 0) args(1).toInt else 10
      val ctx = new SparkContext(sparkConf)
      val lines = ctx.textFile(args(0), 1)
      val links = lines.map{ s =>
        val parts = s.split("\\s+")
        (parts(0), parts(1))
      }.distinct().groupByKey().cache()
      var ranks = links.mapValues(v => 1.0)

      for (i <- 1 to iters) {
        val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
        }
        ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      }

      val output = ranks.collect()
      output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

      ctx.stop()
    }
  }


