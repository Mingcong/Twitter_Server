import spray.routing._
import spray.http.MediaTypes

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


  val numWorkers = if (args.length > 0) args(0) toInt else 100  // the number of workers in server
  val numPerWorker = if (args.length > 1) args(1) toInt else 5000  // the number of workers in server


  var workerArray = ArrayBuffer[ActorRef]()
  implicit val actorSystem = ActorSystem()

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
  println("hello smc")


  startServer(interface = "10.227.56.44", port = 8080) {
    get {
      path("hello") {
        complete {
          "Welcome to Spray!\n"
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

    }

  }


}