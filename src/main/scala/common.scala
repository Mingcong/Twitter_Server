package common
//import java.util.Date
//import scala.collection.mutable.ArrayBuffer
//
//sealed trait Message
//case object GetNumofServerWorkers extends Message
//case class numOfServerWorkers(num: Int)
//case object ClientBossReady extends Message
//case object ServerActorWantYouWork extends Message
//
//case object IsServerBossReady extends Message
//case object ServerReady extends Message
//
//case class numFollowers(user_id: Int)
//case class followers_num(num: Int)
//
//case class getTweet(t: Tweet)

case class Tweet(user_id: Int, text: String, time_stamp: String, var ref_id: String)
case class DirectMessage(sender_id: Int, receiver_id: Double, text: String, time_stamp: String, var ref_id: String)


