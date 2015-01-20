import akka.actor.{ Actor, ActorRef, Props, ActorSystem }
import akka.actor.actorRef2Scala
import akka.dispatch.ExecutionContexts.global
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration.DurationInt
import scala.io.Source.fromFile
import java.security.MessageDigest
import java.util.UUID
import akka.actor.{ Address, AddressFromURIString }
import akka.routing.RoundRobinRouter
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.mutable._
import java.io._

case class Inilialize()
case class START()
case class Tweet(i:Int)
case class Stop(start: Long, i: Int)
case class users(num:Int)
case class Retrieve(num:Int)
case class PrintTweet(n : Int, TweetList: Queue[String])
case class Final (count: Int)

object twitterremote 
{
  //System.setOut(new PrintStream(new FileOutputStream("output.doc")));
  def main(args: Array[String])
  {
    val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
        """akka
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
              hostname = "127.0.0.1"
              port = 0
            }
          }     
        }""")
    val system = ActorSystem("HelloRemoteSystem", ConfigFactory.load(config))
    val Master =system.actorOf(Props(new Master((args(0).toInt),args(1).toString(),system)),name="Master")
    
    Master! Inilialize()
    
  }
}

class Master(UserCount : Int, ServerAddress : String, system: ActorSystem) extends Actor 
{
  var ClientActor= new ArrayBuffer[ActorRef]() 
  var remoteMaster = context.actorFor("akka.tcp://LocalSystem@"+ServerAddress+":5150/user/ServerActor")
  def receive = 
    {
      case Inilialize() => 
      {
        remoteMaster ! usertable(UserCount)
      }
      case START() =>
      {
        val start: Long = System.currentTimeMillis
        val cores: Int = (Runtime.getRuntime().availableProcessors())*2
        for(i <-0 until cores) 
        {
          ClientActor += context.actorOf(Props(new Twitter(ServerAddress)),name="Twitter"+i)       
        }
        for(i <-0 until cores) 
        {
          import system.dispatcher
          system.scheduler.schedule(0 milliseconds,100 milliseconds, ClientActor(i), Stop(start,UserCount))
        }
         for (i <- 0 to UserCount - 1 )
        {
          import system.dispatcher
          system.scheduler.schedule(0 milliseconds,10 milliseconds,ClientActor(i%cores),Tweet(i))
        }
         
        /*import system.dispatcher
          system.scheduler.schedule(0 milliseconds,30 milliseconds, ClientActor(Random.nextInt(cores-1)), Retrieve(UserCount))*/
          
       
        }
      
    }
} 

class Twitter(ServerAddress :String) extends Actor 
{
    var remote = context.actorFor("akka.tcp://LocalSystem@"+ServerAddress+":5150/user/ServerActor")
    def receive = 
    {
      case Tweet(i) => 
      {
        val TwitterString = i+" :"+ Random.alphanumeric.take(140).mkString
        remote ! msg(i,TwitterString )
      }

      case Retrieve(num) => 
      {
        val UserID = Random.nextInt(num-1)
        remote ! UserTweet(UserID)
      }
      case PrintTweet (n,t) =>
      {
        println(n+" : "+t)
      }
      
      case Stop(start,i) => 
      {
        val duration: Duration = (System.currentTimeMillis - start).millis
        if (duration > 300.second)
        {
          remote ! GetCount()
          context.stop(self)
          //System.exit(0)
        }
      }
      
    }
}