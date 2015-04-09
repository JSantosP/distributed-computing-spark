package course.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext,DataFrame}

object MostRetweetedSparkSQL extends App {

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
    .setAppName("MostRetweetedSparkSQL")
    .setMaster("local[4]")

  /*
   * And then we initialize our spark context (the driver)
   */

  val sparkContext = new SparkContext(sparkConf)

  /*
   * For using SparkSQL, we will need to define an SQLContext,
   * based on our SparkContext
   */

  val sqlContext = new SQLContext(sparkContext)

  /*
   * We can define a new temporary table in SparkSQL with
   * the following syntax, setting where's the file to read
   * and the sampling ratio used to infer the schema.
   */

  val tweets: DataFrame = 
  	sqlContext.jsonFile("src/main/resources/tweets.txt",1.0)
  tweets.registerTempTable("tweets")
  /*
   * Equals to:
	 * sqlContext.sql(s"""
	 *   |CREATE TEMPORARY TABLE tweets
	 *   |USING org.apache.spark.sql.json
	 *   |OPTIONS (
	 *   |  path 'src/main/resources/tweets.txt',
	 *   |  samplingRatio '1'
	 *   |)
	 * """.stripMargin)
   */

  /*
   * A DataFrame represents a particular RDD of Row type,
   * which also contains an schema of contained data.
   */

  /*
   * We could see which is the inferred schema by writing:
   */
  tweets.printSchema

  /*
   * And we could also print all records...
   */
   
  //tweets.foreach(println)

  /*
   * That's the awkward moment when you realize there's too much
   * useless stuff and you only want to select some fields. So...
   */

   val shortTweets = 
   	sqlContext.sql(s"""
   		|SELECT id, text, retweet_count, user.screen_name 
   		|FROM tweets
   	""".stripMargin)

  /*
   * And voilà!
   */

   shortTweets.printSchema

   //shortTweets.foreach(println)

	/*
	 * So let's calculate, using 'SQL' statements, which is the
	 * most retweeted tweet
	 */

	 val mostRetweeted =
	 	sqlContext.sql(s"""
	 		|SELECT id, text, retweet_count, user.screen_name 
	 		|FROM tweets
	 		|ORDER BY retweet_count DESC LIMIT 1
	  """.stripMargin)

	mostRetweeted.foreach(println)

	/*
	 * That simple!
	 */

  /*
   * Let's cleanup and free busy resources...
   */
  sparkContext.stop()

}