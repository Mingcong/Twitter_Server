package common

import spray.json.DefaultJsonProtocol

case class Tweet(user_id: Int, text: String, time_stamp: String, var ref_id: String)
case class DirectMessage(sender_id: Int, receiver_id: Int, text: String, time_stamp: String, var ref_id: String)
case class followerNum(var userID: Int, var numFollowers: Int)

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val followerNumFormat = jsonFormat2(followerNum)
  implicit val tweetFormat = jsonFormat4(Tweet)
  implicit val messageFormat = jsonFormat5(DirectMessage)
}