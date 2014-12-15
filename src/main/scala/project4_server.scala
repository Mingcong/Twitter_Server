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

  val prob: ArrayBuffer[Double] = ArrayBuffer(0.06, 0.811, 0.874, 0.966, 0.9825, 0.9999, 0.99999, 1.000)

  val numWorkers = if (args.length > 0) args(0) toInt else 20  // the number of workers in server
  val numPerWorker = if (args.length > 1) args(1) toInt else 5000  // the number of workers in server


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
  }
  import MyJsonProtocol._


  println("hello smc")
  var count = 0

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
    get {
      path("getNum") {
        complete {
          count.toString
        }
      }
    }


  }

  case class TimeElement(ref_id: String, time_stamp: Date)


  class workerActor() extends Actor {
    var userTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var homeTimeline = new Array[ArrayBuffer[TimeElement]](numPerWorker)
    var followers = new Array[ArrayBuffer[Int]](numPerWorker)

    var ready: Boolean = false
    var count: Int = 0


    def receive = {

      case InitJob => {
        for(i <- 0 until numPerWorker){
          userTimeline(i) = new ArrayBuffer()
          homeTimeline(i) = new ArrayBuffer()
          followers(i) = new ArrayBuffer()
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
      counter += 1
    }
    followers.remove(0)
    followers

  }


}