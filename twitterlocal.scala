import akka.actor.{ Actor, ActorRef, Props, ActorSystem }
import akka.actor.actorRef2Scala
import akka.dispatch.ExecutionContexts.global
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration.DurationInt
import scala.io.Source.fromFile
import java.security.MessageDigest
import akka.routing.RoundRobinRouter
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import akka.dispatch.ExecutionContexts._
import scala.collection.mutable._
import scala.util.Random
import java.io._

case class msg(ID:Int, tweet: String)
case class PrintList(i: Int)
case class usertable(n: Int)
case class UserTweet(n: Int)
case class TweetCount()
case class GetCount()

object twitterlocal 
{ 
  var TweetCount: Int =0
  
  def Increment()
  {
    TweetCount = TweetCount +1
  }
    def main(args: Array[String]) 
  {
    val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
        """ 
        akka
        { 
          actor
          { 
            provider = "akka.remote.RemoteActorRefProvider" 
          } 
          remote
          { 
                  enabled-transports = ["akka.remote.netty.tcp"] 
            netty.tcp
            { 
              hostname = "192.168.0.18" 
              port = 5150 
            } 
          }      
        }""")
    implicit val system = ActorSystem("LocalSystem", ConfigFactory.load(config))
    val cores: Int = (Runtime.getRuntime().availableProcessors())*2
    var TweetList = new ListBuffer[Queue[String]] ()
    var FolTab = new ArrayBuffer[ArrayBuffer[Int]] ()

    println("Starting Twitter Server....\n")
    println ("Locating Tweets.......\n")
    println("Following List:\n")
    val ActorRouter = system.actorOf(Props(new ServerActor(TweetList, FolTab)).withRouter(RoundRobinRouter(cores)), name = "ServerActor")
  } 
  
}

class ServerActor(TweetList: ListBuffer[Queue[String]],FolTab : ArrayBuffer[ArrayBuffer[Int]]) extends Actor 
{
  
  def receive = 
  {  
    case usertable(n) =>
    {      
      val numusers = n
      for(i <- 0 to numusers-1)
      {
        val Q = new Queue[String]
        TweetList+= Q
      }
      for(k<-0 to numusers-1)
      {
        val F = new ArrayBuffer[Int]         
        FolTab+= F
      }
      for(i <- 0 to numusers-1)
      {
        var fol= Random.nextInt(numusers-1)
        for(j<- 0 to fol-1)
        {
          var ran=Random.nextInt(numusers-1)
          while(ran==i || FolTab(i).contains(ran))
          {
            ran=Random.nextInt(numusers-1)
          }
          FolTab(i)+= ran 
        }
      }
      println(FolTab)
      sender ! START()
    }
    
    case msg(i, tweet) => 
    {
      TweetList(i)+= tweet
      twitterlocal.Increment()
      if(TweetList(i).length==101)
        TweetList(i).dequeue
      //println(twitterlocal.TweetCount)
    }
    
    case GetCount() =>
      {
        println("Final Tweet Count: "+twitterlocal.TweetCount)
      }
    
    case UserTweet(n) =>
    {
          for(k<-0 to (FolTab(n).size)-1)
          {
            
          if(!TweetList(FolTab(n)(k)).isEmpty)
            sender ! PrintTweet(n,TweetList(FolTab(n)(k)))
          } 
      }
  }
}
