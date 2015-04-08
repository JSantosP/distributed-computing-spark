package course.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

object MostRetweeted1 extends App {

  println("MostRetweeted - Sample1")

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
    .setAppName("MostRetweeted1")
    .setMaster("local[4]")

  /*
   * And then we initialize our spark context (the driver)
   */

  implicit val sparkContext = new SparkContext(sparkConf)

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
   * we will assign a random retweets amount to each
   * tweet, beacuse right now all tweet have 0 retweets
   */

  println(s"Retweeted tweets : ${tweets.filter(_.retweets > 0).count}")

  val simulatedRetweets: RDD[Tweet] = tweets.map(
  	_.copy(retweets = math.abs(scala.util.Random.nextInt().toLong)))

  /*
   * Now there should exist different retweet amounts...
   */
  simulatedRetweets.foreach(println)
  println(s"Retweeted tweets : ${simulatedRetweets.filter(_.retweets > 0).count}")

  /*
   * So we could get max retweeted value with a reduce function (RDD action).
   */
  val mostRetweeted: Tweet = 
  	simulatedRetweets.reduce((t1: Tweet,t2: Tweet) => 
  		if (t1.retweets > t2.retweets) t1 else t2)
  println(s"Most retweeted : $mostRetweeted")

  /*
   * Let's cleanup and free busy resources...
   */
  sparkContext.stop()

}

