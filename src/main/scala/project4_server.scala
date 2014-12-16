import spray.routing._
import spray.http.MediaTypes
import spray.json._
import DefaultJsonProtocol._
import spray.json.{JsonFormat}

import akka.actor.{ActorSystem, Actor, Props, ActorRef}
import scala.collection.mutable.ArrayBuffer
import java.security.MessageDigest
import akka.util.Timeout
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.Date
import java.util.Formatter
import java.util.Calendar
import java.text.SimpleDateFormat
import com.typesafe.config.ConfigFactory
import Array._
import scala.util.Random
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import common._



object project4_server extends App with SimpleRoutingApp {

  sealed trait Message
  case object InitJob extends Message
  case object IsReady extends Message
  case object IsBuildFinish extends Message

  case class buildFollowers(user_id: Int)
  case class numFollowers(user_id: Int)
  case class addFollowings(user_id: Int, id: Int)

  case class processWorkload(user_id: Int, ref_id: String, time_stamp: String, mentionID: Int)
  case class processTweet(user_id: Int, time_stamp: Date, ref_id: String)
  case class getFollowers(user_id: Int, time_stamp: Date, ref_id: String, followers: ArrayBuffer[Int])
  case class updateHomeTimeline(user_id: Int, time_stamp: Date, ref_id: String)
  case class viewUserTimeline(user_id: Int)
  case class clientGetFollowers(user_id: Int)
  case class clientGetFriends(user_id: Int)
  case class createFriendship(user_id: Int, newFriend: Double)
  case class destroyFriendship(user_id: Int, oldFriend: Double)
  case class destroyTweet(user_id: Int, del_ID: Double)
  case class clientGetTweet(user_id: Int, numTweet: Double)



  val prob: ArrayBuffer[Double] = ArrayBuffer(0.06, 0.811, 0.874, 0.966, 0.9825, 0.9999, 0.99999, 1.000)

  val numWorkers = if (args.length > 0) args(0) toInt else 20  // the number of workers in server
  val numPerWorker = if (args.length > 1) args(1) toInt else 500  // the number of workers in server

  var tweetStorage: Map[String, Tweet] = Map()

  var workerArray = ArrayBuffer[ActorRef]()
  implicit val actorSystem = ActorSystem()
  implicit val timeout = Timeout(1.second)

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
  case class followerNum(var userID: Int, var numFollowers: Int)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val followerNumFormat = jsonFormat2(followerNum)
    implicit val tweetFormat = jsonFormat4(Tweet)
  }
  import MyJsonProtocol._


  println("hello smc")
  var count = 0
  var tweet_count = 0

  def getJson(route: Route) = {
    respondWithMediaType(MediaTypes.`application/json`) { route }
  }


  startServer(interface = "10.227.56.44", port = 8080) {
    getJson {
      path("hello") {
        complete {
          "Welcome to Spray!\n"
        }
      }
    } ~
      getJson {
        path("getFollowerNum" / IntNumber) { index =>
          println(index + " " + count)
          count = count + 1

          complete {
            (workerArray(index % numWorkers) ? numFollowers(index)).mapTo[followerNum].map(s => s.toJson.prettyPrint)

          }
        }
      } ~
      getJson {
        path("viewUserTimeline" / IntNumber) { index =>
          println("view UserTimeline" + index)
          complete {
            (workerArray(index % numWorkers) ? viewUserTimeline(index)).mapTo[List[Tweet]].map(s => s.toJson.prettyPrint)

          }
        }
      } ~
      getJson {
        path("viewHomeTimeline" / IntNumber) { index =>
          println("view HomeTimeline" + index)
          complete {
            (workerArray(index % numWorkers) ? viewHomeTimeline(index)).mapTo[List[Tweet]].map(s => s.toJson.prettyPrint)

          }
        }
      } ~
      get {
        path("getNum") {
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
          complete {
            (workerArray(index % numWorkers) ? clientGetFollowers(index)).mapTo[Array[Int]].map(s => s.toJson.prettyPrint)
          }
        }
      } ~
      getJson {
        path("getFriends" / IntNumber) { index =>
          complete {
            (workerArray(index % numWorkers) ? clientGetFriends(index)).mapTo[Array[Int]].map(s => s.toJson.prettyPrint)
          }
        }
      } ~
      getJson {
        path("showTweet" / IntNumber / DoubleNumber) { (user_id, numTweet) =>
          complete {
            (workerArray(user_id % numWorkers) ? clientGetTweet(user_id, numTweet)).mapTo[Tweet].map(s => s.toJson.prettyPrint)
          }
        }
      } ~
      post {
        path("postTweet") {
          parameters("userID".as[Int], "mentionID".as[Int], "text".as[String], "timeStamp".as[String], "refID".as[String]) { (userID, mentionID, text, timeStamp, refID) =>
            val t = Tweet(userID, text, timeStamp, refID)
            println(t.user_id + " " + t.text + " " + t.time_stamp)
            tweetStorage += t.ref_id -> t
            workerArray(tweet_count % numWorkers) ! processWorkload(t.user_id, t.ref_id, t.time_stamp, mentionID)
            tweet_count = tweet_count + 1

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
            complete {
              "ok"
            }
          }
        }
      }
  }

  case class TimeElement(ref_id: String, time_stamp: Date)

  def insertIntoArray(userTimeline: ArrayBuffer[TimeElement], ref_id: String, time_stamp: Date) {
    if (0 == userTimeline.size) {
      userTimeline.append(TimeElement(ref_id, time_stamp))
    } else {
      var index = 0
      while (index < userTimeline.size && time_stamp.compareTo(userTimeline(index).time_stamp) < 0)
        index += 1
      userTimeline.insert(index, TimeElement(ref_id, time_stamp))
    }
  }



  class workerActor() extends Actor {
    var userTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var homeTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var mentionTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var followers = new Array[ArrayBuffer[Int]](numPerWorker)
    var followings = new Array[ArrayBuffer[Int]](numPerWorker)

    var ready: Boolean = false
    var count: Int = 0


    def receive = {

      case processWorkload(user_id, ref_id, time_stamp, mentionID) => {
        val workerId = user_id%numWorkers
        workerArray(workerId) ! processTweet(user_id, stringToDate(time_stamp), ref_id)
      }

      case processTweet(user_id, time_stamp, ref_id) => {
        insertIntoArray(userTimeline(user_id/numWorkers), ref_id, time_stamp)
        println(user_id + " send tweet: " + tweetStorage(ref_id).text  )
        sender ! getFollowers(user_id, time_stamp, ref_id, followers(user_id/numWorkers))
      }

      case getFollowers(user_id, time_stamp, ref_id, followers) => {
        for (node <- followers) {
          workerArray(node % numWorkers) ! updateHomeTimeline(node, time_stamp, ref_id)
        }
//        workloadPerWorker(self.path.name.toInt) = workloadPerWorker(self.path.name.toInt) + 1
      }
      case updateHomeTimeline(user_id, time_stamp, ref_id) => {
        insertIntoArray(homeTimeline((user_id/numWorkers).toInt), ref_id, time_stamp)
        //        println(user_id + " following: " + tweetStorage(ref_id).text  )

      }

      case viewUserTimeline(i) => {
        var timeLine: List[Tweet] = List()
        val line = userTimeline(i/numWorkers)
        val userLine = line.dropRight(line.size - 25)
        var t = Tweet(-1, "", "", "")
        //println(sender.path.name + " userTimeline " + line.size )
        for (ele <- userLine) {
          if(tweetStorage.contains(ele.ref_id)) {
            t = tweetStorage(ele.ref_id)
          }
          timeLine = timeLine :+ t
        }
        sender ! timeLine
//        requestPerWorker(self.path.name.toInt) = requestPerWorker(self.path.name.toInt) +1
      }

      case viewHomeTimeline(i) => {
        var timeLine: List[Tweet] = List()
        val line1 = userTimeline(i/numWorkers)
        val userLine = line1.dropRight(line1.size - 25)
        val line2 = homeTimeline(i/numWorkers)
        val homeLine = line2.dropRight(line2.size - 25)
        var t = Tweet(-1, "", "", "")
        userLine.appendAll(homeLine)
        //println(sender.path.name + " userTimeline " + line.size )
        for (ele <- userLine) {
          if(tweetStorage.contains(ele.ref_id)) {
            t = tweetStorage(ele.ref_id)
          }
          timeLine = timeLine :+ t
        }
        sender ! timeLine
      }


      case InitJob => {
        for(i <- 0 until numPerWorker){
          userTimeline(i) = new ArrayBuffer()
          homeTimeline(i) = new ArrayBuffer()
          followers(i) = new ArrayBuffer()
          followings(i) = new ArrayBuffer()
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
      case numFollowers(index) => {
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
        val line = homeTimeline(user_id/numWorkers)
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
          println("add friends " + friend)
        }
      }

      case destroyFriendship(user_id, oldFriend) => {
        val friends = followings(user_id/numWorkers)
        val friend = friends((oldFriend*friends.size).toInt)
        println("delete friends " + friend)
        followings(user_id/numWorkers) -= friend
      }

      case destroyTweet(user_id, del_ID) => {
        val line = userTimeline(user_id/numWorkers)
        val t = line((line.size*del_ID).toInt)
        tweetStorage = tweetStorage - t.ref_id
        println("delete tweet " + t.ref_id)
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
      return genRandNumber(5001, 10000)
    }else if(7 == index) {
      //      return genRandNumber(10001, 100000)
      return 10001
    }else
    //      return 100001
      return 10001
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