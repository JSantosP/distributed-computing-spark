package course.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object MostRetweeted extends App {

  /*
   * At first, we should define our Spark context.
   * It will be the nexus to comunicate 
   * the driver (our main program) with the spark
   * cluster (in this example, a local embeded one).
   */

  /*
   * We will first define its configuration
   */

  val sparkConf = new SparkConf()
    .setAppName("MostRetweeted")
    .setMaster("local[4]")

  /*
   * And then we initialize our spark context (the driver)
   */

  val sparkContext = new SparkContext(sparkConf)

  /*
   * Let's initialize our RDD reading from tweets file source.
   */
  val lines: RDD[String] = sparkContext.textFile("src/main/resources/tweets.txt")

  /*
   * Let's convert our Strings into Tweets.
   * This is called an RDD transformation.
   * In transformations, RDD elements are not evaluated yet.
   */
  val tweets: RDD[Tweet] =
    lines.map(Tweet.apply) //equals to 'lines.map(s => Tweet.apply(s))'

  /*
   * Supposing we want to print all tweets...
   * By iterating on the RDD, we're evaluating its values.
   * This is called an RDD action.
   */
  tweets.foreach(println)

  /*
   * For testing which is the most retweeted tweet,
   * we will check that at least some tweets have non-zero amounts
   * of retweet.
   */

  println(s"Retweeted tweets : ${tweets.filter(_.retweets > 0).count}")

  /*
   * So we could get max retweeted value with a reduce function (RDD action).
   */
  val mostRetweeted: Tweet =
    tweets.reduce((t1: Tweet, t2: Tweet) =>
      if (t1.retweets > t2.retweets) t1 else t2)

  println(s"Most retweeted : $mostRetweeted")

  /*
   * Let's cleanup and free busy resources...
   */
  sparkContext.stop()

}

