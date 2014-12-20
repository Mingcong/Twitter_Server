import spray.routing._
import spray.http.MediaTypes
import spray.json._

import akka.actor.{ActorSystem, Actor, Props, ActorRef}
import scala.collection.mutable.ArrayBuffer
import akka.util.Timeout
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.text.SimpleDateFormat
import scala.util.Random
import scala.util.control.Breaks._
import common._
import common.MyJsonProtocol._

import java.util.Calendar
import java.util.Date


object project4_server extends App with SimpleRoutingApp {

  sealed trait Message
  case object InitJob extends Message
  case object IsReady extends Message
  case object IsBuildFinish extends Message
  case object GetSum extends Message

  case class buildFollowers(user_id: Int)
  case class getNumFollowers(user_id: Int)
  case class addFollowings(user_id: Int, id: Int)

  case class processWorkload(user_id: Int, ref_id: String, time_stamp: String, mentionID: Int)
  case class processTweet(user_id: Int, time_stamp: Date, ref_id: String, mentionID: Int)

  case class postMessage(user_id: Int, ref_id: String, time_stamp: String, receiveID: Int)
  case class processMessage(user_id: Int, time_stamp: Date, ref_id: String, receiveID: Int)
  case class updateMessageTimeline(user_id: Int, ref_id: String, time_stamp: Date)
  case class viewSendMessageTimeline(user_id: Int)
  case class viewReceiveMessageTimeline(user_id: Int)
  case class destroyMessage(user_id: Int, del_ID: Double)

  case class addMentionTimeline(mentionID: Int, time_stamp: Date, ref_id: String)
  case class getFollowers(user_id: Int, time_stamp: Date, ref_id: String, followers: ArrayBuffer[Int])
  case class updateHomeTimeline(user_id: Int, time_stamp: Date, ref_id: String)
  case class viewUserTimeline(user_id: Int)
  case class viewMentionTimeline(user_id: Int)
  case class viewHomeTimeline(user_id: Int)
  case class clientGetFollowers(user_id: Int)
  case class clientGetFriends(user_id: Int)
  case class createFriendship(user_id: Int, newFriend: Double)
  case class destroyFriendship(user_id: Int, oldFriend: Double)
  case class destroyTweet(user_id: Int, del_ID: Double)
  case class clientGetTweet(user_id: Int, numTweet: Double)

  case class sumRequest(i: Int)

  val prob: ArrayBuffer[Double] = ArrayBuffer(0.06, 0.811, 0.874, 0.966, 0.9825, 0.9999, 0.99999, 1.000)

  val cycle = if (args.length >0) args(0) toInt else 3600
  val numWorkers = if (args.length > 1) args(1) toInt else 200  // the number of workers in server
  val numPerWorker = if (args.length > 2) args(2) toInt else 500  // the number of workers in server

  var tweetStorage: Map[String, Tweet] = Map()
  var messageStorage: Map[String, DirectMessage] = Map()

  var workerArray = ArrayBuffer[ActorRef]()
  implicit val actorSystem = ActorSystem()
  implicit val timeout = Timeout(10.second)
  val sumNode = actorSystem.actorOf(Props(classOf[sumActor]), "sumNode")
  actorSystem.scheduler.schedule(cycle seconds, cycle seconds, sumNode, GetSum )

  val numUsers = numWorkers * numPerWorker
  var counter: Int =0
  while(counter < numWorkers){
    val worker = actorSystem.actorOf(Props(classOf[workerActor]), counter.toString)
    workerArray.append(worker)
    counter += 1
  }

  for (node <- workerArray)
    node ! InitJob
  for (node <- workerArray) {
    implicit val timeout = Timeout(20 seconds)
    var ready: Boolean = false
    while (!ready) {
      val future = node ? IsReady
      ready = Await.result(future.mapTo[Boolean], timeout.duration)
    }
  }

  for(i<-0 until numWorkers*numPerWorker) {
    workerArray(i%numWorkers) ! buildFollowers(i)
  }
  for (node <- workerArray) {
    implicit val timeout = Timeout(600 seconds)
    var ready: Boolean = false
    while (!ready) {
      val future = node ? IsBuildFinish
      ready = Await.result(future.mapTo[Boolean], timeout.duration)
    }
  }

  println("hello smc")
  var count = 0
  var tweet_count = 0
  var message_count = 0

  def getJson(route: Route) = {
    respondWithMediaType(MediaTypes.`application/json`) { route }
  }

//10.244.33.189 192.168.1.5
//  startServer(interface = "10.227.56.44", port = 8080) {
    startServer(interface = "192.168.1.5", port = 9056) {
      getJson {
        path("hello") {
          complete {
            "Welcome to Spray!\n"
          }
        }
      } ~
        getJson {
          path("getFollowerNum" / IntNumber) { index =>
            //          println(index + " " + count)
            count = count + 1

            complete {
              (workerArray(index % numWorkers) ? getNumFollowers(index)).mapTo[followerNum].map(s => s.toJson.prettyPrint)

            }
          }
        } ~
        getJson {
          path("viewUserTimeline" / IntNumber) { index =>
            println("user" + index + " view UserTimeline ")
            complete {
              (workerArray(index % numWorkers) ? viewUserTimeline(index)).mapTo[List[Tweet]].map(s => s.toJson.prettyPrint)

            }
          }
        } ~
        getJson {
          path("viewMentionTimeline" / IntNumber) { index =>
            println("user" + index + " view MentionTimeline ")
            complete {
              (workerArray(index % numWorkers) ? viewMentionTimeline(index)).mapTo[List[Tweet]].map(s => s.toJson.prettyPrint)

            }
          }
        } ~
        getJson {
          path("viewHomeTimeline" / IntNumber) { index =>
            println("user" + index + " view HomeTimeline ")
            complete {
              (workerArray(index % numWorkers) ? viewHomeTimeline(index)).mapTo[List[Tweet]].map(s => s.toJson.prettyPrint)


            }
          }
        } ~
        getJson {
          path("viewSendMessage" / IntNumber) { index =>
            println("view SendMessage " + index)
            complete {
              (workerArray(index % numWorkers) ? viewSendMessageTimeline(index)).mapTo[List[DirectMessage]].map(s => s.toJson.prettyPrint)

            }
          }
        } ~
        getJson {
          path("viewReceiveMessage" / IntNumber) { index =>
            println("view ReceiveMessage " + index)
            complete {
              (workerArray(index % numWorkers) ? viewReceiveMessageTimeline(index)).mapTo[List[DirectMessage]].map(s => s.toJson.prettyPrint)

            }
          }
        } ~
        get {
          path("getNum") {
            //          println(count)
            complete {
              count.toString
            }
          }
        } ~
        getJson {
          path("getTweet" / Rest) { index =>
            complete {
              tweetStorage(index).toJson.prettyPrint
            }
          }
        } ~
        getJson {
          path("getFollowers" / IntNumber) { index =>
            println("user" + index + " get followers ")
            complete {
              (workerArray(index % numWorkers) ? clientGetFollowers(index)).mapTo[Array[Int]].map(s => s.toJson.prettyPrint)
            }
          }
        } ~
        getJson {
          path("getFriends" / IntNumber) { index =>
            println("user" + index + " get friends ")
            complete {
              (workerArray(index % numWorkers) ? clientGetFriends(index)).mapTo[Array[Int]].map(s => s.toJson.prettyPrint)
            }
          }
        } ~
        getJson {
          path("showTweet" / IntNumber / DoubleNumber) { (user_id, numTweet) =>
            println("user" + user_id + " want to get Tweet ")
            complete {
              (workerArray(user_id % numWorkers) ? clientGetTweet(user_id, numTweet)).mapTo[Tweet].map(s => s.toJson.prettyPrint)
            }
          }
        } ~
        post {
          path("postTweet") {
            parameters("userID".as[Int], "mentionID".as[Int], "text".as[String], "timeStamp".as[String], "refID".as[String]) { (userID, mentionID, text, timeStamp, refID) =>
              val t = Tweet(userID, text, timeStamp, refID)
              //            println("Tweet: " + t.user_id + " " + t.text + " " + t.time_stamp)
              tweetStorage += t.ref_id -> t
              workerArray(tweet_count % numWorkers) ! processWorkload(t.user_id, t.ref_id, t.time_stamp, mentionID)
              tweet_count = tweet_count + 1
              sumNode ! sumRequest(1)
              complete {
                "ok"
              }
            }
          }
        } ~
        post {
          path("postMessage") {
            parameters("userID".as[Int], "receiveID".as[Int], "text".as[String], "timeStamp".as[String], "refID".as[String]) {
              (userID, receiveID, text, timeStamp, refID) =>
                val t = DirectMessage(userID, receiveID, text, timeStamp, refID)
                println(t.sender_id + " send a message: " + t.text + " to " + receiveID + " at " + t.time_stamp)
                messageStorage += t.ref_id -> t
                workerArray(message_count % numWorkers) ! postMessage(t.sender_id, t.ref_id, t.time_stamp, t.receiver_id)
                message_count = message_count + 1

                complete {
                  "ok"
                }
            }
          }
        } ~
        post {
          path("destroyMessage") {
            parameters("user_ID".as[Int], "del_ID".as[Double]) { (user_ID, del_ID) =>
              workerArray(user_ID % numWorkers) ! destroyMessage(user_ID, del_ID)
              println("user" + user_ID + " destroy a message ")
              complete {
                "ok"
              }
            }
          }
        } ~
        post {
          path("createFriendship") {
            parameters("user_ID".as[Int], "newFriend".as[Double]) { (user_ID, newFriend) =>
              workerArray(user_ID % numWorkers) ! createFriendship(user_ID, newFriend)
              println("user" + user_ID + " follow a friend ")
              complete {
                "ok"
              }
            }
          }
        } ~
        post {
          path("destroyFriendship") {
            parameters("user_ID".as[Int], "oldFriend".as[Double]) { (user_ID, oldFriend) =>
              workerArray(user_ID % numWorkers) ! destroyFriendship(user_ID, oldFriend)
              println("user" + user_ID + " unfollow a friend ")
              complete {
                "ok"
              }
            }
          }
        } ~
        post {
          path("destroyTweet") {
            parameters("user_ID".as[Int], "del_ID".as[Double]) { (user_ID, del_ID) =>
              workerArray(user_ID % numWorkers) ! destroyTweet(user_ID, del_ID)
              println("user" + user_ID + " destroy a Tweet ")
              complete {
                "ok"
              }
            }
          }
        }
  }

  case class TimeElement(ref_id: String, time_stamp: Date)

  def insertIntoArray(timeLine: ArrayBuffer[TimeElement], ref_id: String, time_stamp: Date) {
    if (0 == timeLine.size) {
      timeLine.append(TimeElement(ref_id, time_stamp))
    } else {
      var index = 0
      while (index < timeLine.size && time_stamp.compareTo(timeLine(index).time_stamp) < 0)
        index += 1
      timeLine.insert(index, TimeElement(ref_id, time_stamp))
    }
  }


class sumActor() extends Actor {
  var count: Int = 0
  var temp: Int = 0
  def receive = {
    case sumRequest(i) => {
      count = count + i
    }
    case GetSum => {
      println("count = " + count +  " " + Calendar.getInstance().getTime + " ThroughPut: " + (count-temp)/cycle)
      temp = count
    }
  }
}

  class workerActor() extends Actor {
    var userTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var homeTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var mentionTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)

    var sendMessageTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var receiveMessageTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)

    var followers = new Array[ArrayBuffer[Int]](numPerWorker)
    var followings = new Array[ArrayBuffer[Int]](numPerWorker)

    var ready: Boolean = false
    var count: Int = 0


    def receive = {

      case postMessage(user_id, ref_id, time_stamp, receiveID) => {
        val workerId = user_id%numWorkers
        workerArray(workerId) ! processMessage(user_id, stringToDate(time_stamp), ref_id, receiveID)
      }
      case processMessage(user_id, time_stamp, ref_id, receiveID) => {
        insertIntoArray(sendMessageTimeline(user_id/numWorkers), ref_id, time_stamp)
//        println(user_id + " send Message to " + friend + " : " + messageStorage(ref_id).text)
        workerArray(receiveID%numWorkers) ! updateMessageTimeline(receiveID, ref_id, time_stamp)
      }
      case updateMessageTimeline(user_id, ref_id, time_stamp) => {
        insertIntoArray(receiveMessageTimeline(user_id/numWorkers), ref_id, time_stamp)
      }
      case viewSendMessageTimeline(i) => {
        var sendMessageLine: List[DirectMessage] = List()
        val line = sendMessageTimeline(i/numWorkers)
        val userLine = line.dropRight(line.size - 25)
        //val userLine = line
        var t = DirectMessage(-1,-1, "not find this message", "", "")
        for (ele <- userLine) {
          if(messageStorage.contains(ele.ref_id)) {
            t = messageStorage(ele.ref_id)
          }
          sendMessageLine = sendMessageLine :+ t
        }
        sender ! sendMessageLine
      }
      case viewReceiveMessageTimeline(i) => {
        var receiveMessageLine: List[DirectMessage] = List()
        val line = receiveMessageTimeline(i/numWorkers)
        val userLine = line.dropRight(line.size - 25)
        //val userLine = line
        var t = DirectMessage(-1, -1, "not find this message", "", "")
        for (ele <- userLine) {
          if(messageStorage.contains(ele.ref_id)) {
            t = messageStorage(ele.ref_id)
          }
          receiveMessageLine = receiveMessageLine :+ t
        }
        sender ! receiveMessageLine
      }
      case destroyMessage(user_id, del_ID) => {
        val line = receiveMessageTimeline(user_id/numWorkers)
        if(line.isEmpty) {
          println(" message is null")
        } else {
          val t = line((line.size*del_ID).toInt)
          messageStorage = messageStorage - t.ref_id
//          println("delete message " + t.ref_id)
        }

      }


      case processWorkload(user_id, ref_id, time_stamp, mentionID) => {
        val workerId = user_id%numWorkers
        workerArray(workerId) ! processTweet(user_id, stringToDate(time_stamp), ref_id, mentionID)
      }

      case processTweet(user_id, time_stamp, ref_id, mentionID) => {
        insertIntoArray(userTimeline(user_id/numWorkers), ref_id, time_stamp)
        sender ! getFollowers(user_id, time_stamp, ref_id, followers(user_id/numWorkers))
//        println(user_id + " send tweet: " + tweetStorage(ref_id).text  )
        if(followers(user_id/numWorkers).contains(mentionID)) {
          workerArray(mentionID%numWorkers) ! addMentionTimeline(mentionID,time_stamp, ref_id)
        }
      }
      case addMentionTimeline(mentionID, time_stamp, ref_id) => {
        insertIntoArray(mentionTimeline(mentionID/numWorkers), ref_id, time_stamp)
      }

      case getFollowers(user_id, time_stamp, ref_id, followers) => {
        for (node <- followers) {
          workerArray(node % numWorkers) ! updateHomeTimeline(node, time_stamp, ref_id)
        }
//        workloadPerWorker(self.path.name.toInt) = workloadPerWorker(self.path.name.toInt) + 1
      }
      case updateHomeTimeline(user_id, time_stamp, ref_id) => {
        insertIntoArray(homeTimeline(user_id/numWorkers), ref_id, time_stamp)
        //        println(user_id + " following: " + tweetStorage(ref_id).text  )

      }

      case viewUserTimeline(i) => {
        var timeLine: List[Tweet] = List()
        val line = userTimeline(i/numWorkers)
        val userLine = line.dropRight(line.size - 25)
        var t = Tweet(-1, "not find this tweet", "", "")
        //println(sender.path.name + " userTimeline " + line.size )
        for (ele <- userLine) {
          if(tweetStorage.contains(ele.ref_id)) {
            t = tweetStorage(ele.ref_id)
          }
          timeLine = timeLine :+ t
        }
        sender ! timeLine
        sumNode ! sumRequest(timeLine.size)
//        requestPerWorker(self.path.name.toInt) = requestPerWorker(self.path.name.toInt) +1
      }

      case viewMentionTimeline(i) => {
        var mentionLine: List[Tweet] = List()
        val line = mentionTimeline(i/numWorkers)
        val userLine = line.dropRight(line.size - 25)
        //val userLine = line
        var t = Tweet(-1, "not find this mention", "", "")
        for (ele <- userLine) {
          if(tweetStorage.contains(ele.ref_id)) {
            t = tweetStorage(ele.ref_id)
          }
          mentionLine = mentionLine :+ t
        }
        sender ! mentionLine


      }


      case viewHomeTimeline(i) => {
        var timeLine: List[Tweet] = List()
        val line = mentionTimeline(i/numWorkers)
        val mentionLine = line.dropRight(line.size - 20)
        val line1 = userTimeline(i/numWorkers)
        val userLine = line1.dropRight(line1.size - 20)
        val line2 = homeTimeline(i/numWorkers)
        val homeLine = line2.dropRight(line2.size - 20)
        var t = Tweet(-1, "not find this tweet", "", "")
        userLine.appendAll(mentionLine)
        userLine.appendAll(homeLine)
        //println(sender.path.name + " userTimeline " + line.size )
        for (ele <- userLine) {
          if(tweetStorage.contains(ele.ref_id)) {
            t = tweetStorage(ele.ref_id)
          }
          timeLine = timeLine :+ t
        }
        sender ! timeLine
        sumNode ! sumRequest(timeLine.size)
      }


      case InitJob => {
        for(i <- 0 until numPerWorker){
          userTimeline(i) = new ArrayBuffer()
          homeTimeline(i) = new ArrayBuffer()
          mentionTimeline(i) = new ArrayBuffer()
          followers(i) = new ArrayBuffer()
          followings(i) = new ArrayBuffer()
          sendMessageTimeline(i) = new ArrayBuffer()
          receiveMessageTimeline(i) = new ArrayBuffer()
        }
        ready = true
      }

      case IsReady => {
        sender ! ready
      }

      case buildFollowers(user_id) => {
        var randomFollowers = genRandFollowerCount()
        if (randomFollowers >= numWorkers * numPerWorker)
          randomFollowers = numWorkers * numPerWorker - 1

        followers(user_id/numWorkers) = genRandExceptCur(0, numWorkers * numPerWorker, user_id, randomFollowers)
        count = count +1
        println(user_id)
      }
      case IsBuildFinish => {
        if(count >= numPerWorker) {
          sender ! true
//          println(self.path.name + " finish")
        }
        else
          sender ! false
      }
      case getNumFollowers(index) => {
        val out = followerNum(index,0)
        out.numFollowers = followers(index/numWorkers).size
        sender ! out

        //println(user_id + " numOFfollowers  "   + self + " " + followers(user_id).size + "sender:" + sender)
      }

      case addFollowings(user_id, id) => {
        followings(id/numWorkers).append(user_id)
      }

      case clientGetFollowers(user_id) => {
        sender ! followers(user_id/numWorkers).toArray
      }

      case clientGetFriends(user_id) => {
        sender ! followings(user_id/numWorkers).toArray
      }
      case clientGetTweet(user_id, numTweet) => {
        val line = userTimeline(user_id/numWorkers).clone()
        val mentionLine = mentionTimeline(user_id/numWorkers).clone()
        val homeLine = homeTimeline(user_id/numWorkers).clone()

        line.appendAll(mentionLine)
        line.appendAll(homeLine)
        if(!line.isEmpty)
          sender ! tweetStorage(line((numTweet*line.size).toInt).ref_id)
        else
          sender ! Tweet(-1,"","","")
      }

      case createFriendship(user_id, newFriend) => {
        var friend = ((self.path.name.toInt + newFriend)*numPerWorker).toInt
        while(followings(user_id/numWorkers).contains(friend)) {
          friend = friend + 1
        }
        if(friend < (self.path.name.toInt + 1)*numPerWorker) {
          followings(user_id / numWorkers).append(friend)
//          println("add friends " + friend)
        }
      }

      case destroyFriendship(user_id, oldFriend) => {
        val friends = followings(user_id/numWorkers)
        val friend = friends((oldFriend*friends.size).toInt)
//        println("delete friends " + friend)
        followings(user_id/numWorkers) -= friend
      }

      case destroyTweet(user_id, del_ID) => {
        val line = userTimeline(user_id/numWorkers)
        val t = line((line.size*del_ID).toInt)
        tweetStorage = tweetStorage - t.ref_id
//        println("delete tweet " + t.ref_id)
      }

    }

  }



  def genRandFollowerCount(): Int = {
    val randProb = Random.nextDouble()
    var index = prob.size
      breakable {
      for (i <- 0 until prob.size) {
        if (prob(i) >= randProb) {
          index = i
          break
        }
      }
    }
    if(0 == index) {
      return 0
    }
    else if (1 == index) {
      return genRandNumber(0, 50)
    }else if(2 == index) {
      return  genRandNumber(51, 100)
    }else if(3 == index) {
      return genRandNumber(101, 500)
    }else if(4 == index) {
      return genRandNumber(501, 1000)
    }else if(5 == index) {
      return genRandNumber(1001, 5000)
    }else if(6 == index) {
//      return genRandNumber(5001, 10000)
      return 5000
    }else if(7 == index) {
      //      return genRandNumber(10001, 100000)
      return 5000
    }else
    //      return 100001
      return 5000
  }


  def genRandNumber(first: Int, last: Int): Int = {
    first + Random.nextInt(last-first)
  }

  def genRandExceptCur(first: Int, last: Int, current: Int, randomNumber: Int): ArrayBuffer[Int] = {
    val followers: ArrayBuffer[Int] = new ArrayBuffer()
    followers.append(current)

    var randomFollower: Int = 0
    var counter: Int = 0
    while (counter < randomNumber) {
      //      randomFollower = genRandNumber(first.toInt, last.toInt).toLong
      randomFollower = Random.nextInt(last)
      while (followers.contains(randomFollower))
      //        randomFollower = genRandNumber(first.toInt, last.toInt).toLong
        randomFollower = Random.nextInt(last)
      followers.append(randomFollower)
      workerArray(randomFollower%numWorkers) ! addFollowings(current, randomFollower)
      counter += 1
    }
    followers.remove(0)
    followers

  }

  def dateToString(current: Date): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS")
    val s: String = formatter.format(current)
    return s
  }

  def stringToDate(current: String): Date = {
    val format = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS")
    val s: Date = format.parse(current)
    return s
  }
}