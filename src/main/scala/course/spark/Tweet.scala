package course.spark

import spray.json._
import DefaultJsonProtocol._

//  Simplified representation of a tweet

case class Tweet(
  id: Long,
  content: String,
  retweets: Long,
  user: String)

object Tweet {

  //  Constructor from json value

  def apply(json: String): Tweet = {

    val Seq(
      JsNumber(id),
      user: JsObject,
      JsString(content),
      JsNumber(retweet)) = json.parseJson.asJsObject().getFields(
      "id",
      "user",
      "text",
      "retweet_count")

    val Seq(JsString(userName)) = user.getFields("screen_name")

    Tweet(id.toLong, content, retweet.toLong, userName)

  }

}