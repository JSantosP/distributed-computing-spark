package course.spark

case class Tweet(
	id: Long,
	content: String,
	retweets: Long,
	favs: Long,
	user: String)