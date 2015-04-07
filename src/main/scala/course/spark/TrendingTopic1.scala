package course.spark

import org.apache.spark.SparkConf

object TrendingTopic1 extends App with TweetDataSource {

	println("TrendingTopic - Sample1")

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

}

