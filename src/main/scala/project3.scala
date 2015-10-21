import java.nio.ByteBuffer

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Await}
import scala.math.Ordering.CharOrdering
import scala.util.Random
import scala.concurrent.duration._

/**
 * Created by sarathfrancis on 10/16/15.
 */
object project3 {


  sealed  trait chord
  case class MasterInit(noOfNodes: Int, noOfRequests: Int) extends chord
  case class Join(Node:Long) extends  chord
  case class Init_Finger_Table_request (NodeID: Long) extends  chord
  case class Init_Finger_Table_response (NodeID: Long) extends  chord
  case class Init_Finger_Table_subActor_Init(NodeID : Long) extends  chord
  case class Closest_preceding_finger_request(NodeID : Long) extends  chord
  case class Closest_preceding_finger_response(NodeID : Long) extends  chord
  case class Closest_preceding_Finger_SubActor_Init(NodeID : Long) extends chord
  case class Find_predecessor_request(nodeID: Long) extends chord
  case class Find_predecessor_response(nodeID: Long) extends  chord
  case class Find_predessor_subActor_Init(nodeID: Long) extends chord
  case class Find_successor_request(nodeID: Long) extends chord
  case class Find_successor_response(nodeID: Long) extends chord
  case class Find_successor_subActor_Init(nodeID: Long) extends chord
  case class Update_others_request(nodeID: Long)  extends chord
  case class Update_others_response(nodeID: Long)  extends chord
  case class Update_Finger_table_request(nodeID: Long, position: Long) extends chord
  case class Update_Finger_table_response(nodeID: Long, position: Long) extends chord
  case object GetsuccessorReq extends chord
  case class GetSuccesssorRsp(nodeID: Long) extends chord
  case object GetpredecessorReq extends  chord
  case class GetpredecessorRsp(nodeID: Long) extends chord
  case object Kill  extends  chord






  var Nodes :ListBuffer[(Long,ActorRef)] = new ListBuffer[(Long,ActorRef)]()
  val m: Int  = 8
  val max_number_of_nodes:Long = math.pow(2,m).toLong
  var MyActorSystem : ActorSystem = _

  def main (args: Array[String])
  {

    if(args.length != 2) {
      println("Enter the arguments in the following order <No of Nodes>, ")
      sys.exit()
    }
    else {
      val NoOfNodes: Int = args(0).toInt
      val NoOfRequests: Int = args(1).toInt


      //Creating Actor System
      MyActorSystem = ActorSystem ("Chord_Protocol")

      //Creating Master
      val master = MyActorSystem.actorOf(Props(new Master), name = "Master")

      //Initiating Master
      master ! MasterInit(NoOfNodes,NoOfRequests)

      MyActorSystem.awaitTermination()

    }

  }
  def SHA1(s: Int, m:Int): Long = {
    val hash_bits = java.security.MessageDigest.getInstance("SHA-1").digest(s.toString.getBytes("UTF-8"))
    val bytes:ByteBuffer = ByteBuffer.wrap(hash_bits)
    val bytesToIntLong: Long = bytes.getLong(0) >>> (64-m)
    bytesToIntLong
  }

  class FingerTableEntry {
    var start: Long =_
    var interval: (Long,Long) = (0,0)
    var nodeId : Long =_
  }


  def BelongsTo(id:Long, interval:(Long,Long)):Boolean = {

    var check : Boolean = _
    if(interval._1 < interval._2) {

      if(id > interval._1 && id <= max_number_of_nodes)
        check = true
      else if (id >= 0 && id < interval._2)
        check = true
      else
        check = false
    }
    else {

      if( id > interval._1 && id < interval._2)
        check = true
      else
        check = false
    }
    check
  }

  def BelongsTo(id:Long, nDashID : Long , successorId_of_nDash: Long):Boolean = {

    var check : Boolean = _
    if(nDashID < successorId_of_nDash) {

      if(id > nDashID && id <= max_number_of_nodes)
        check = true
      else if (id >= 0 && id < successorId_of_nDash)
        check = true
      else
        check = false
    }
    else {

      if( id > nDashID && id < successorId_of_nDash)
        check = true
      else
        check = false
    }
    check
  }
  def finger_start(nodeID:Long,i:Long):Long ={

    val twotothepoweriminus1 : Long = 1 << (i-1)
    val twotothepoweerm: Long = 1 << m
    val start : Long = (nodeID + twotothepoweriminus1) % twotothepoweerm
    start
  }
  class Node extends Actor with ActorLogging {

    var identifier : Long = -1
    var successorID : Long =_
    var predecessorID : Long =_
//    var FingerTable = new Array[FingerTableEntry](m)
    var finger_table : Array[(Long, (Long, Long), Long)] = new Array[(Long, (Long, Long), Long)](m)
    var Keys_List : ListBuffer[Long] = new ListBuffer[Long]()
    var requestorMap : ListBuffer[(ActorRef,ActorRef)] =  new ListBuffer[(ActorRef, ActorRef)]
    var SubActorCount: Int =0;

    def receive = {

      case GetsuccessorReq =>

        sender ! GetSuccesssorRsp(successorID)

      case GetsuccessorReq =>
        sender ! GetpredecessorRsp(predecessorID)

      case Join(nodeID) =>
        if(nodeID != -1)  {


        }
        else {
          identifier = nodeID
          for(i <-1 to m) {
//            val fin
            predecessorID =  Nodes.find(node => {node._2 == self}).get._1
          }
        }
      case Init_Finger_Table_request(nodeID) =>

      case Find_successor_request(id) =>

        SubActorCount+=1
        val find_successor_subActorRef = MyActorSystem.actorOf(Props(new Find_successor_subActor),name = identifier.toString + " SubActor Number " + SubActorCount.toString)

        val requestMap_Pair = (sender(),find_successor_subActorRef)
        requestorMap += requestMap_Pair
        find_successor_subActorRef ! Find_successor_subActor_Init(identifier)
        find_successor_subActorRef ! Find_successor_request(id)

      case Find_successor_response(id) =>

        requestorMap.find(node => { node._2 == sender()}).get._1 ! Find_successor_response(id)

        sender ! Kill

      case Find_predecessor_request(id) =>

        SubActorCount += 1
        val find_predecessor_subActorRef = MyActorSystem.actorOf(Props(new Find_predecessor_subActor), name = identifier.toString + " SubActor Number " + SubActorCount.toString)
        val requestMap_Pair = (sender(),find_predecessor_subActorRef)
        requestorMap += requestMap_Pair
        find_predecessor_subActorRef ! Find_successor_subActor_Init(identifier)
        find_predecessor_subActorRef ! Find_successor_request(id)

      case Find_predecessor_response(id) =>

        requestorMap.find(node => { node._2 == sender()}).get._1 ! Find_predecessor_response(id)

        sender ! Kill

      case Closest_preceding_finger_request(id) =>

        var success : Boolean = false
        var position :Long = -1

        for ( i <- m to 1 by -1) {

          if(BelongsTo(finger_table(i)._3,identifier,id))  {

            success = true
            position = i
          }
        }
        if(success == true)
          sender ! Closest_preceding_finger_response(finger_table(position.toInt)._3)
        else
          sender ! Closest_preceding_finger_response(identifier)




      println("Node Receive")
    }
  }

  class Init_fingertable_subActor extends Actor with ActorLogging {

    var MyparentID :Long =_
    implicit val TimeoutDuration = Timeout(5 seconds)

    def receive = {

      case Init_Finger_Table_subActor_Init(nodeID) =>
        MyparentID = nodeID

      case Init_Finger_Table_request(nodeID) =>

        var finger_table_inside_subActor : Array[(Long, (Long, Long), Long)] = new Array[(Long, (Long, Long), Long)](m)



//        val init_fingertable_future : Future[Init_Finger_Table_response] = (Nodes.find(nodeid => {nodeid._1 == MyparentID}).get._2 ? Find_predecessor_request(nodeID)).mapTo[Init_Finger_Table_response]
//        val init_fingertable_result = Await.result(init_fingertable_future,TimeoutDuration.duration)
//
//        Nodes.find(nodeid => {nodeid._1 == MyparentID}).get._2 ! Init_Finger_Table_response(init_fingertable_result.NodeID)

    }

  }
  class Find_successor_subActor extends  Actor with ActorLogging {

    var MyparentID : Long =_
    implicit val TimeoutDuration = Timeout(5 seconds)

    def receive = {

      case Find_successor_subActor_Init(nodeID) =>
        MyparentID = nodeID

      case Find_successor_request(id) =>

        val find_predessor_future : Future[Find_predecessor_response] = (Nodes.find(nodeid => {nodeid._1 == MyparentID}).get._2 ? Find_predecessor_request(id)).mapTo[Find_predecessor_response]
        val find_predessor_result = Await.result(find_predessor_future,TimeoutDuration.duration)

        val ndashID : Long = find_predessor_result.nodeID

        val get_successor_of_ndash_future : Future[GetSuccesssorRsp] = (Nodes.find(nodeid => {nodeid._1 == ndashID}).get._2 ? Find_predecessor_request(id)).mapTo[GetSuccesssorRsp]
        val get_successor_of_ndash_result = Await.result(get_successor_of_ndash_future,TimeoutDuration.duration)

        val successorID_of_ndash : Long = get_successor_of_ndash_result.nodeID

        Nodes.find(nodeid => {nodeid._1 == MyparentID}).get._2 ! Find_successor_response(successorID_of_ndash)

      case Kill =>
        context.stop(self)

    }
  }

  class Find_predecessor_subActor extends  Actor with ActorLogging {

    var MyparentID : Long =_
    implicit val TimeoutDuration = Timeout(5 seconds)

    def receive = {

      case Find_predessor_subActor_Init(nodeID) =>
        MyparentID = nodeID

      case Find_predecessor_request(id) =>
        var ndashId :Long = MyparentID
        val getsuccessor_future : Future[GetSuccesssorRsp] = (Nodes.find(nodeid => {nodeid._1 == ndashId}).get._2 ? GetsuccessorReq).mapTo[GetSuccesssorRsp]
        val getSuccessor_result = Await.result(getsuccessor_future,TimeoutDuration.duration)

        var successorID_of_nDash : Long = getSuccessor_result.nodeID


        while(! BelongsTo(id,ndashId,successorID_of_nDash)) {

         val closest_preceding_finger_future : Future[Closest_preceding_finger_response] = (Nodes.find(nodeid => {nodeid._1 == ndashId}).get._2 ? Closest_preceding_finger_request(id)).mapTo[Closest_preceding_finger_response]
         val closest_preceding_finger_result = Await.result(closest_preceding_finger_future,TimeoutDuration.duration)
          ndashId = closest_preceding_finger_result.NodeID
          successorID_of_nDash = getSuccessor_result.nodeID

        }

        Nodes.find(nodeid => {nodeid._1 == MyparentID}).get._2 ! Find_predecessor_response(ndashId)

      case Kill =>
        context.stop(self)

    }
  }


//  class Closest_preseding_Finger_SubActor extends Actor with ActorLogging {
//
//    implicit val TimeoutDuration = Timeout(5 seconds)
//    var MyparentID : Long = _
//
//    def receive = {
//
//      case Closest_preceding_Finger_SubActor_Init(nodeID) =>
//
//        MyparentID = nodeID
//
//      case Closest_preceding_finger_request(id) =>
//
//
//        val future : Future[Closest_preceding_finger_response] = (Nodes.find(node => {node._1 == MyparentID}).get._2 ? Closest_preceding_finger_request(id)).mapTo[Closest_preceding_finger_response]
//        val closest_preceding_finger_result = Await.result(future, TimeoutDuration.duration)
//        //        log.info("got function2 result")
//
//        Nodes.find(node => {node._1 == MyparentID}).get._2 ! Closest_preceding_finger_response(closest_preceding_finger_result.NodeID)
//
//
//
//    }
//  }

  class Master extends Actor {

    var no_of_nodes: Int = _
    var no_of_requests: Int =_
    var identifier: Long =_

    def receive = {

      case MasterInit(noOfNodes,noOfRequests) =>

        no_of_nodes = noOfNodes
        no_of_requests = noOfRequests

        for(i <-0 until no_of_nodes) {
          val aRandomNumber : Int = Random.nextInt(no_of_nodes*100)
           identifier=SHA1(aRandomNumber,m)
          val NodeRef = MyActorSystem.actorOf(Props(new Node), name = identifier.toString)

          val Node_ID_pair = (identifier,NodeRef)

          Nodes+= Node_ID_pair

          if(i==0)
            Nodes(i)._2 ! Join(-1)
          else
            Nodes(i)._2 ! Join(Nodes(i-1)._1)

//          println(identifier)
        }





    }
  }

}
