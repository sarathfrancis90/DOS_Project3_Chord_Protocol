import java.nio.ByteBuffer
import sun.swing.SwingUtilities2.AATextInfo

import scala.language.postfixOps
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Await}
import scala.util.Random
import scala.concurrent.duration._

/**
 * Created by sarathfrancis on 10/16/15.
 */
object project3 {


  sealed  trait chord
  case class MasterInit(noOfNodes: Int, noOfRequests: Int) extends chord
  case class NodeInit (Identifier :Long) extends chord
  case class Join(Node:Long) extends  chord
  case class Join_SubActor_Init(NodeID:Long) extends  chord
  case class Join_response(success:Boolean) extends chord
  case class Init_Finger_Table_request (NodeID: Long) extends  chord
  case class Init_Finger_Table_response (Finger_table : Array[(Long,Long)]) extends  chord
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
  case class Update_others_request()  extends chord
  case class Update_others_response(sucess: Boolean)  extends chord
  case class Update_others_subActor_Init (nodeID : Long) extends chord
  case class Update_Finger_table_request(nodeID: Long, position: Int) extends chord
  case class Update_Finger_table_response(success: Boolean) extends chord
  case class Update_Finger_table_subActor_Init(nodeID: Long) extends chord
  case object GetsuccessorReq extends chord
  case class GetSuccesssorRsp(nodeID: Long) extends chord
  case object GetpredecessorReq extends  chord
  case class GetpredecessorRsp(nodeID: Long) extends chord
  case class SetsuccessorReq(nodeID:Long) extends chord
  case class SetSuccesssorRsp(success : Boolean) extends chord
  case class SetpredecessorReq(NodeID: Long) extends  chord
  case class SetpredecessorRsp(success: Boolean) extends chord
  case object GetFinger_tableReq extends  chord
  case class SetFinger_tableReq(finger_table: Array[(Long,Long)]) extends chord
  case class SetFinger_tableRsp(success: Boolean) extends chord
  case class GetFinger_tableRsp(Finger_Table: Array[(Long,Long)]) extends chord
  case object Kill  extends  chord
  case object Printrequest extends  chord
  case class Printresponse(success : Boolean) extends chord
  case class TestInit(successor: Long, predecessor: Long, finger_table: Array[(Long,Long)],id : Long) extends chord
  case class Find_avg_msg_count_request() extends chord
  case class Find_avg_msg_count_response(messagecount : Int) extends chord




  var Nodes :ListBuffer[(Long,ActorRef)] = new ListBuffer[(Long,ActorRef)]()
  val m: Int  =  20
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
  def SHA1(s: Long, m:Int): Long = {
    val hash_bits = java.security.MessageDigest.getInstance("SHA-1").digest(s.toString.getBytes("UTF-8"))
    val bytes:ByteBuffer = ByteBuffer.wrap(hash_bits)
    val bytesToIntLong: Long = bytes.getLong() >>> (64-m)
    bytesToIntLong
  }

  def NodeIDtoActorRef (nodeID : Long ) : ActorRef = {

    val NodeActorRef: ActorRef = Nodes.find(nodeid => {nodeid._1 == nodeID}).get._2
    NodeActorRef

  }

  def ActorReftoNodeID ( NodeActorRef: ActorRef) : Long = {

    val NodeID :Long = Nodes.find(nodeid => {nodeid._2 == NodeActorRef}).get._1
    NodeID
  }

  def BelongsTo(id:Long, start : Long , end: Long, left_closed : Boolean, right_closed: Boolean):Boolean = {

    var check : Boolean =  true
    if(start < end) {
       check = false
      if (left_closed == true && right_closed == true) {
        if (id >= start && id <= end) {
           check = true
        }
      }
      if (left_closed == true && right_closed == false) {
        if (id >= start && id < end) {
          check =  true
        }
      }
      if (left_closed == false && right_closed == true) {
        if (id > start && id <= end) {
          check =  true
        }
      }
      if (left_closed == false && right_closed == false) {
        if (id > start && id < end) {
          check =  true
        }
      }
    }
    else if (end < start) {

      val flipped_start: Long = end
      val flipped_end : Long =  start
      val flipped_leftclosed : Boolean = ! right_closed
      val flipped_rightclosed: Boolean = ! left_closed

      check  = true
      if (flipped_leftclosed == true && flipped_rightclosed == true) {
        if (id >= flipped_start && id <= flipped_end) {
          check = false
        }
      }
      if (flipped_leftclosed == true && flipped_rightclosed == false) {
        if (id >= flipped_start && id < flipped_end) {
          check =  false
        }
      }
      if (flipped_leftclosed == false && flipped_rightclosed == true) {
        if (id > flipped_start && id <= flipped_end) {
          check =  false
        }
      }
      if (flipped_leftclosed == false && flipped_rightclosed == false) {
        if (id > flipped_start && id < flipped_end) {
          check = false
        }
      }

    }
    else {
      check = false
      if(id == start && (left_closed == true || right_closed == true)) {
        check = true
      }
      else if(left_closed == true && right_closed == false) {
        check = true
      }
    }
     return check
  }
  def finger_start(nodeID:Long,i:Long):Long ={


    val twotothepoweriminus1 : Long = 1.toLong << (i-1)
    val twotothepoweerm: Long = 1.toLong << m
    val start : Long = (nodeID + twotothepoweriminus1) % twotothepoweerm


    return start
  }

  def print(id:Long, successor: Long, predecessor : Long , finger_table : Array[(Long,Long)]): Boolean = {

    println("The details of this Node with the id : " + id)
    println("Successor : " + successor)
    println("Predecessor : " + predecessor)
    println("Finger Table: ")
    println ("start   Node")

    for(i<-0 until m) {
      println(finger_table(i)._1 + "     " + finger_table(i)._2)
    }

    true
  }
  class Node extends Actor with ActorLogging {

    var identifier : Long = -1
    var successorID : Long =_
    var predecessorID : Long =_
    var finger_table : Array[(Long, Long)] = new Array[(Long, Long)](m)
    var Keys_List : ListBuffer[Long] = new ListBuffer[Long]()
    var requestorMap : ListBuffer[(ActorRef,ActorRef)] =  new ListBuffer[(ActorRef, ActorRef)]()
    var SubActorCount: Int =0;
    val msgreceived : Boolean = true
    var joining : Boolean = false
    var msgcount : Int = 0
    def receive = {

      case NodeInit(id) =>
        identifier = id

      case TestInit(successor,predecessor,fingertable,id) =>
        successorID =successor
        predecessorID= predecessor
        finger_table=fingertable
        identifier = id

      case GetsuccessorReq =>

        sender ! GetSuccesssorRsp(successorID)

      case GetpredecessorReq =>
        sender ! GetpredecessorRsp(predecessorID)

      case SetsuccessorReq(nodeID) =>
        successorID = nodeID
        sender ! SetSuccesssorRsp(msgreceived)

      case SetpredecessorReq(nodeID) =>
        predecessorID = nodeID
        sender ! SetpredecessorRsp(msgreceived)

      case GetFinger_tableReq =>
        sender ! GetFinger_tableRsp(finger_table)

      case SetFinger_tableReq(new_finger_table) =>
        finger_table = new_finger_table

        sender ! SetFinger_tableRsp(msgreceived)

      case Find_avg_msg_count_request () =>
        sender ! Find_avg_msg_count_response(msgcount)

      case Join(nodeID) =>

        joining = true

          SubActorCount +=1
          val join_subActor =MyActorSystem.actorOf(Props(new Join_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)

          val requestMap_Pair = (sender(),join_subActor)
          requestorMap += requestMap_Pair

          join_subActor ! Join_SubActor_Init(identifier)
          join_subActor ! Join(nodeID)

      case Join_response(success: Boolean) =>

        joining =false
        requestorMap.find(node => { node._2 == sender}).get._1 ! Join_response(success)


        sender ! Kill

      case Printrequest =>

        val print_success = print(identifier,successorID,predecessorID,finger_table)

        sender ! Printresponse(print_success)


      case Init_Finger_Table_request(nodeID) =>
        SubActorCount +=1
        val init_finger_table_subActor =MyActorSystem.actorOf(Props(new Init_fingertable_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
        val requestMap_Pair = (sender(),init_finger_table_subActor)
        requestorMap += requestMap_Pair
        init_finger_table_subActor ! Init_Finger_Table_subActor_Init(identifier)
        init_finger_table_subActor ! Init_Finger_Table_request(nodeID)

      case Init_Finger_Table_response(finger_table_array) =>
        requestorMap.find(node => { node._2 == sender()}).get._1 ! Init_Finger_Table_response(finger_table_array)

        sender ! Kill

      case Update_Finger_table_request(s, i) =>
      if(joining == true) {
        sender ! Update_Finger_table_response(true)
      }
      else {
        SubActorCount +=1
        val update_finger_table_subActor =MyActorSystem.actorOf(Props(new Update_Finger_table_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
        val requestMap_Pair = (sender(), update_finger_table_subActor)
        requestorMap += requestMap_Pair
        update_finger_table_subActor ! Update_Finger_table_subActor_Init(identifier)
        update_finger_table_subActor ! Update_Finger_table_request(s,i)

      }


      case Update_Finger_table_response(success) =>
        requestorMap.find(node => { node._2 == sender()}).get._1 ! Update_Finger_table_response(success)

        sender ! Kill

      case Update_others_request() =>
          SubActorCount +=1
          val update_others_subActor =MyActorSystem.actorOf(Props(new Update_others_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
          val requestMap_Pair = (sender(), update_others_subActor)
          requestorMap += requestMap_Pair
          update_others_subActor ! Update_others_subActor_Init(identifier)
          update_others_subActor ! Update_others_request()

      case Update_others_response(success) =>
        requestorMap.find(node => { node._2 == sender()}).get._1 ! Update_others_response(success)

        sender ! Kill

      case Find_successor_request(id) =>

        if(id == identifier) {
            sender ! Find_successor_response (identifier)
          }
        else {
          SubActorCount+=1
          val find_successor_subActorRef = MyActorSystem.actorOf(Props(new Find_successor_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
          val requestMap_Pair = (sender(),find_successor_subActorRef)
          requestorMap += requestMap_Pair
          find_successor_subActorRef ! Find_successor_subActor_Init(identifier)
          find_successor_subActorRef ! Find_successor_request(id)

        }


      case Find_successor_response(id) =>
        msgcount += 1
        requestorMap.find(node => { node._2 == sender()}).get._1 ! Find_successor_response(id)

        sender ! Kill

      case Find_predecessor_request(id) =>
        if(id == identifier) {
          sender ! Find_predecessor_response (predecessorID)
        }
        else {
          SubActorCount += 1
          val find_predecessor_subActorRef = MyActorSystem.actorOf(Props(new Find_predecessor_subActor), name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
          val requestMap_Pair = (sender(),find_predecessor_subActorRef)
          requestorMap += requestMap_Pair
          find_predecessor_subActorRef ! Find_predessor_subActor_Init(identifier)
          find_predecessor_subActorRef ! Find_predecessor_request(id)
        }


      case Find_predecessor_response(id) =>
        requestorMap.find(node => { node._2 == sender()}).get._1 ! Find_predecessor_response(id)

        sender ! Kill

      case Closest_preceding_finger_request(id) =>
        var success : Boolean = false
        var position :Long = -1

        for ( i <- m-1 to 0 by -1) {

          if(success == false) {
            if(BelongsTo(finger_table(i)._2,identifier,id,false,false))  {
              success = true
              position = i
            }
          }

        }
        if(success == true)
          sender ! Closest_preceding_finger_response(finger_table(position.toInt)._2)
        else
          sender ! Closest_preceding_finger_response(identifier)

    }
  }
  class Join_subActor extends  Actor with ActorLogging {

    var MyparentID :Long =_
    implicit val TimeoutDuration = Timeout(5 seconds)

    val finger_table_in_the_join_subActor : Array[(Long,Long)] = new Array[(Long, Long)](m)
    def receive = {
      case Join_SubActor_Init(nodeID) =>
        MyparentID = nodeID

      case Join(nodeID) =>
        if(nodeID == -1) {

          for(i <-0 until m) {
            finger_table_in_the_join_subActor(i) = {(finger_start(MyparentID,i+1),MyparentID)}
          }
          val setFingertable_future : Future[SetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? SetFinger_tableReq(finger_table_in_the_join_subActor)).mapTo[SetFinger_tableRsp]
          val setFingertable_result = Await.result(setFingertable_future,TimeoutDuration.duration)


          val set_predecessor_future : Future [SetpredecessorRsp] = (NodeIDtoActorRef(MyparentID) ? SetpredecessorReq(MyparentID)).mapTo[SetpredecessorRsp]
          val set_predecessor_result = Await.result(set_predecessor_future,TimeoutDuration.duration)


          val set_successor_future : Future [SetSuccesssorRsp] = (NodeIDtoActorRef(MyparentID) ? SetsuccessorReq(MyparentID)).mapTo[SetSuccesssorRsp]
          val set_successor_result = Await.result(set_successor_future,TimeoutDuration.duration)


          NodeIDtoActorRef(MyparentID) ! Join_response(true)

        }
        else {

          val init_Finger_Table_future : Future[Init_Finger_Table_response] = (NodeIDtoActorRef(MyparentID) ? Init_Finger_Table_request(nodeID)).mapTo[Init_Finger_Table_response]
          val init_Finger_table_result = Await.result(init_Finger_Table_future,TimeoutDuration.duration)

          val setFinger_table_future : Future[SetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? SetFinger_tableReq(init_Finger_table_result.Finger_table)).mapTo[SetFinger_tableRsp]
          val setFinger_table_result = Await.result(setFinger_table_future,TimeoutDuration.duration)

          val update_others_future : Future[Update_others_response] = (NodeIDtoActorRef(MyparentID) ? Update_others_request()).mapTo[Update_others_response]
          val update_others_result = Await.result(update_others_future,TimeoutDuration.duration)

          NodeIDtoActorRef(MyparentID) ! Join_response(true)

        }


      case  Kill =>
        context.stop(self)
    }


  }
  class Update_others_subActor extends Actor with ActorLogging {

    var MyparentID: Long = _
    implicit val TimeoutDuration = Timeout(5 seconds)
    var nminus2tothepower1minus1 : Long = _

    def receive = {

      case Update_others_subActor_Init(nodeID) =>
        MyparentID = nodeID

      case Update_others_request() =>

        for(i <- 0 until m) {
          if ((MyparentID - (1 << i)) < 0)
            nminus2tothepower1minus1 = (MyparentID - (1 << i)) + max_number_of_nodes
          else
            nminus2tothepower1minus1 = MyparentID - (1 << i)
          var p: Long = nminus2tothepower1minus1

          if (Nodes.indexWhere(node => {node._1 == p}) == -1) {

            val p_future: Future[Find_predecessor_response] = (NodeIDtoActorRef(MyparentID) ? Find_predecessor_request(p)).mapTo[Find_predecessor_response]
            val p_result = Await.result(p_future, TimeoutDuration.duration)

            p = p_result.nodeID

          }
          val update_Finger_table_future: Future[Update_Finger_table_response] = (NodeIDtoActorRef(p) ? Update_Finger_table_request(MyparentID, i)).mapTo[Update_Finger_table_response]
          val update_Finger_table_result = Await.result(update_Finger_table_future, TimeoutDuration.duration)
        }
        NodeIDtoActorRef(MyparentID) ! Update_others_response(true)



      case Kill =>
        context.stop(self)

    }
  }

  class Update_Finger_table_subActor extends Actor with  ActorLogging {

    var MyparentID : Long =_
    implicit val TimeoutDuration = Timeout(5 seconds)

    def receive = {

      case Update_Finger_table_subActor_Init(nodeID) =>

        MyparentID = nodeID

      case Update_Finger_table_request(nodeID: Long, i:Int) =>

        var finger_table_inside_subActor : Array[(Long, Long)] = new Array[(Long, Long)](m)

        val get_finger_table_future :Future[GetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? GetFinger_tableReq).mapTo[GetFinger_tableRsp]
        val get_finger_table_result = Await.result(get_finger_table_future,TimeoutDuration.duration)

        finger_table_inside_subActor = get_finger_table_result.Finger_Table

        if(BelongsTo(nodeID,MyparentID,finger_table_inside_subActor(i)._2,true,false))  {

          finger_table_inside_subActor(i) = {(finger_table_inside_subActor(i)._1,nodeID)}


          val setFinger_table_future : Future[SetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? SetFinger_tableReq(finger_table_inside_subActor)).mapTo[SetFinger_tableRsp]
          val setFinger_table_result = Await.result(setFinger_table_future,TimeoutDuration.duration)

          if(i==0) {
            val set_successor_future : Future [SetSuccesssorRsp] = (NodeIDtoActorRef(MyparentID) ? SetsuccessorReq(finger_table_inside_subActor(0)._2)).mapTo[SetSuccesssorRsp]
            val set_successor_result = Await.result(set_successor_future,TimeoutDuration.duration)

          }

          val p_future : Future [GetpredecessorRsp] = (NodeIDtoActorRef(MyparentID) ? GetpredecessorReq).mapTo[GetpredecessorRsp]
          val p_result = Await.result(p_future,TimeoutDuration.duration)

          val p : Long = p_result.nodeID
          val update_finger_table_future : Future[Update_Finger_table_response] = (NodeIDtoActorRef(p) ? Update_Finger_table_request(nodeID,i)).mapTo[Update_Finger_table_response]
          val update_finger_table_result = Await.result(update_finger_table_future,TimeoutDuration.duration)

        }
        sender ! Update_Finger_table_response(true)

      case Kill =>
        context.stop(self)

    }
  }
  class Init_fingertable_subActor extends Actor with ActorLogging {

    var MyparentID :Long =_
    implicit val TimeoutDuration = Timeout(5 seconds)

    def receive = {

      case Init_Finger_Table_subActor_Init(nodeID) =>
        MyparentID = nodeID

      case Init_Finger_Table_request(nodeID) =>

        val finger_table_inside_subActor : Array[(Long, Long)] = new Array[(Long, Long)](m)
        for(i <- 0 until m) {
          finger_table_inside_subActor(i) = {(finger_start(MyparentID,i+1),-1.toLong)}
        }
        val find_successsor_future : Future[Find_successor_response] = (NodeIDtoActorRef(nodeID) ? Find_successor_request(finger_table_inside_subActor(0)._1)).mapTo[Find_successor_response]
        val find_successor_result = Await.result(find_successsor_future,TimeoutDuration.duration)
        finger_table_inside_subActor(0) = {(finger_table_inside_subActor(0)._1,find_successor_result.nodeID)}

        val set_successor_future : Future [SetSuccesssorRsp] = (NodeIDtoActorRef(MyparentID) ? SetsuccessorReq(finger_table_inside_subActor(0)._2)).mapTo[SetSuccesssorRsp]
        Await.result(set_successor_future,TimeoutDuration.duration)

        val get_successors_predecessor_future : Future [GetpredecessorRsp] = (NodeIDtoActorRef(finger_table_inside_subActor(0)._2) ? GetpredecessorReq).mapTo[GetpredecessorRsp]
        val get_successors_predecessor_result = Await.result(get_successors_predecessor_future,TimeoutDuration.duration)

        val successors_predecessor : Long = get_successors_predecessor_result.nodeID

        val set_predecessor_future : Future [SetpredecessorRsp] = (NodeIDtoActorRef(MyparentID) ? SetpredecessorReq(successors_predecessor)).mapTo[SetpredecessorRsp]
        val set_predecessor_result = Await.result(set_predecessor_future,TimeoutDuration.duration)

        val set_successors_predecessor_future : Future[SetpredecessorRsp] = (NodeIDtoActorRef(finger_table_inside_subActor(0)._2) ? SetpredecessorReq(MyparentID)).mapTo[SetpredecessorRsp]
        val set_successors_predecessor_result = Await.result(set_successors_predecessor_future,TimeoutDuration.duration)

        for(i <- 0 until m-1) {
          if(BelongsTo(finger_table_inside_subActor(i+1)._1,MyparentID,finger_table_inside_subActor(i)._2,true,false) ) {

            finger_table_inside_subActor(i+1) = {(finger_table_inside_subActor(i+1)._1,finger_table_inside_subActor(i)._2)}
          }
          else {

            val find_successor_ndash_future :Future[Find_successor_response] = (NodeIDtoActorRef(nodeID) ? Find_successor_request(finger_table_inside_subActor(i+1)._1)).mapTo[Find_successor_response]
            val find_successor_ndash_result = Await.result(find_successor_ndash_future,TimeoutDuration.duration)

            finger_table_inside_subActor(i+1) = {(finger_table_inside_subActor(i+1)._1,find_successor_ndash_result.nodeID)}
          }
        }
        NodeIDtoActorRef(MyparentID) ! Init_Finger_Table_response(finger_table_inside_subActor)


      case Kill =>
        context.stop(self)
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
        val get_successor_of_ndash_future : Future[GetSuccesssorRsp] = (NodeIDtoActorRef(ndashID) ? GetsuccessorReq).mapTo[GetSuccesssorRsp]
        val get_successor_of_ndash_result = Await.result(get_successor_of_ndash_future,TimeoutDuration.duration)

        val successorID_of_ndash : Long = get_successor_of_ndash_result.nodeID

       NodeIDtoActorRef(MyparentID) ! Find_successor_response(successorID_of_ndash)

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
        var newNdashId: Long = MyparentID
        var Exit_Loop : Boolean = false
        val getsuccessor_future : Future[GetSuccesssorRsp] = (NodeIDtoActorRef(ndashId) ? GetsuccessorReq).mapTo[GetSuccesssorRsp]
        val getSuccessor_result = Await.result(getsuccessor_future,TimeoutDuration.duration)

        var successorID_of_nDash : Long = getSuccessor_result.nodeID


        while((!BelongsTo(id,ndashId,successorID_of_nDash,false,true)) && (Exit_Loop == false)) {

          val closest_preceding_finger_future: Future[Closest_preceding_finger_response] = (NodeIDtoActorRef(ndashId) ? Closest_preceding_finger_request(id)).mapTo[Closest_preceding_finger_response]
          val closest_preceding_finger_result = Await.result(closest_preceding_finger_future, TimeoutDuration.duration)

          newNdashId = closest_preceding_finger_result.NodeID
          if (newNdashId == ndashId) {
            Exit_Loop = true
          }
          ndashId = newNdashId

          val getsuccessor1_future: Future[GetSuccesssorRsp] = (NodeIDtoActorRef(ndashId) ? GetsuccessorReq).mapTo[GetSuccesssorRsp]
          val getSuccessor1_result = Await.result(getsuccessor1_future, TimeoutDuration.duration)
          successorID_of_nDash = getSuccessor1_result.nodeID

        }

         NodeIDtoActorRef(MyparentID)! Find_predecessor_response(ndashId)

      case Kill =>
        context.stop(self)

    }
  }

  class Master extends Actor with ActorLogging {

    var no_of_nodes: Int = _
    var no_of_requests: Int =_
    var identifier: Long =_

    implicit val TimeoutDuration = Timeout(5 seconds)

    def receive = {

      case MasterInit(noOfNodes,noOfRequests) =>

        no_of_nodes = noOfNodes
        no_of_requests = noOfRequests
        println("No of Nodes: " + no_of_nodes)
        println("No of bits (m) " + m)
        println("No of requests : " + no_of_requests)
        println("Maximum_number of nodes allowed " + max_number_of_nodes )

        for(i <-0 until no_of_nodes) {

          var RandomNumber : Long = Random.nextInt(no_of_nodes * 1000)

          identifier = SHA1(RandomNumber, m)

          while(Nodes.indexWhere(node =>{node._1 == identifier})!= -1) {
            RandomNumber = Random.nextInt(no_of_nodes * 1000)
            identifier = SHA1(RandomNumber, m)
          }


          val NodeRef = MyActorSystem.actorOf(Props(new Node), name = identifier.toString)
          val Node_ID_pair = (identifier, NodeRef)
          Nodes += Node_ID_pair
          NodeRef ! NodeInit(identifier)

          if(i==0) {
            val join_future : Future[Join_response] = (Nodes(i)._2 ? Join(-1)).mapTo[Join_response]
            val join_result = Await.result(join_future,TimeoutDuration.duration)

            println(i + " th Node: " + identifier + " Joined")

          }

          else {
            val join_future : Future[Join_response] = (Nodes(i)._2 ? Join(Nodes(i-1)._1)).mapTo[Join_response]
            val join_result = Await.result(join_future,TimeoutDuration.duration)
            println(i + " th Node: " + identifier + " Joined")


          }
        }

        for(i <-0 until no_of_nodes) {

          for( j<- 0 until no_of_requests) {
            val find_successsor_future : Future[Find_successor_response] = (Nodes(i)._2 ? Find_successor_request(no_of_nodes/2)).mapTo[Find_successor_response]
            val find_successor_result = Await.result(find_successsor_future,TimeoutDuration.duration)
          }

        }
        var msgcountsum : Int = 0
        var avgmsgcount : Float = 0
        for(i <-0 until no_of_nodes) {


          val find_avg_msgcount_future : Future[Find_avg_msg_count_response] = (Nodes(i)._2 ? Find_avg_msg_count_request()).mapTo[Find_avg_msg_count_response]
          val find_avg_msgcount_result = Await.result(find_avg_msgcount_future,TimeoutDuration.duration)

          msgcountsum += find_avg_msgcount_result.messagecount
//          println(find_avg_msgcount_result.messagecount)
          //          Thread.sleep(1000)
        }
        avgmsgcount = msgcountsum / no_of_nodes

        println("*********************************************")
        println()
        println()

        println("Average msg count is : " + avgmsgcount)

        println()
        println()

        println("*********************************************")
        context.system.shutdown()
    }
  }

}
