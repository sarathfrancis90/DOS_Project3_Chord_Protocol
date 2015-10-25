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




  var Nodes :ListBuffer[(Long,ActorRef)] = new ListBuffer[(Long,ActorRef)]()
  val m: Int  = 4
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

  def NodeIDtoActorRef (nodeID : Long ) : ActorRef = {

    val NodeActorRef: ActorRef = Nodes.find(nodeid => {nodeid._1 == nodeID}).get._2
    NodeActorRef

  }

  def ActorReftoNodeID ( NodeActorRef: ActorRef) : Long = {

    val NodeID :Long = Nodes.find(nodeid => {nodeid._2 == NodeActorRef}).get._1
    NodeID
  }

  def fallsIn(id: Long, start: Long, startStrict: Boolean, end: Long, endStrict: Boolean): Boolean = {
    var result: Boolean = false
    if (start < end) {
      if (startStrict == true && endStrict == true) {
        if (start < id && id < end) {
          result = true
        }
      } else if (startStrict == false && endStrict == false) {
        if (start <= id && id <= end) {
          result = true
        }
      } else if (startStrict == true && endStrict == false) {
        if (start < id && id <= end) {
          result = true
        }
      } else if (startStrict == false && endStrict == true) {
        if (start <= id && id < end) {
          result = true
        }
      }
    } else if (end < start) {
      result = true
      val newStart = end
      val newEnd = start
      val newStartStrict = !endStrict
      val newEndStrict = !startStrict
      if (newStartStrict == true && newEndStrict == true) {
        if (newStart < id && id < newEnd) {
          result = false
        }
      } else if (newStartStrict == false && newEndStrict == false) {
        if (newStart <= id && id <= newEnd) {
          result = false
        }
      } else if (newStartStrict == true && newEndStrict == false) {
        if (newStart < id && id <= newEnd) {
          result = false
        }
      } else if (newStartStrict == false && newEndStrict == true) {
        if (newStart <= id && id < newEnd) {
          result = false
        }
      }
    } else {
      if (id == start && (startStrict == false || endStrict == false)) {
        result = true
      } else {
        if (startStrict == false && endStrict == true) {
          result = true
        }
      }
    }

    return result
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

    def receive = {

      case NodeInit(id) =>
        identifier = id
        log.info("My Id is " + id)

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
        println(nodeID)
        successorID = nodeID
        sender ! SetSuccesssorRsp(msgreceived)

      case SetpredecessorReq(nodeID) =>
        predecessorID = nodeID
        sender ! SetpredecessorRsp(msgreceived)

      case GetFinger_tableReq =>
        log.info("At the get finger table request ")
        sender ! GetFinger_tableRsp(finger_table)

      case SetFinger_tableReq(new_finger_table) =>
        finger_table = new_finger_table
        println("*********************************** at the Node")
        log.info(finger_table.foreach(println(_)).toString)

        sender ! SetFinger_tableRsp(msgreceived)

      case Join(nodeID) =>

        joining = true
        log.info( "At the join request in the Node")

          SubActorCount +=1
          val join_subActor =MyActorSystem.actorOf(Props(new Join_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)

          val requestMap_Pair = (sender(),join_subActor)
          requestorMap += requestMap_Pair

          join_subActor ! Join_SubActor_Init(identifier)
          join_subActor ! Join(nodeID)

      case Join_response(success: Boolean) =>

        joining =false


//        requestorMap.foreach(forwardingMapPair => {
//          log.info("from " + forwardingMapPair._1.path.name + " fwd'ed to " + forwardingMapPair._2.path.name)
//        })
        requestorMap.find(node => { node._2 == sender}).get._1 ! Join_response(success)


        sender ! Kill

      case Printrequest =>

        val print_success = print(identifier,successorID,predecessorID,finger_table)

        sender ! Printresponse(print_success)


      case Init_Finger_Table_request(nodeID) =>

        log.info("At the Init_finger_table_request in the Node")
        SubActorCount +=1
        val init_finger_table_subActor =MyActorSystem.actorOf(Props(new Init_fingertable_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
        val requestMap_Pair = (sender(),init_finger_table_subActor)
        requestorMap += requestMap_Pair
        init_finger_table_subActor ! Init_Finger_Table_subActor_Init(identifier)
        init_finger_table_subActor ! Init_Finger_Table_request(nodeID)

      case Init_Finger_Table_response(finger_table_array) =>

        log.info("After init finger_table response")

        println("***********************************")
        log.info(finger_table_array.foreach(println(_)).toString)
//        Thread.sleep(100000)
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

        log.info("At the update finger table response")

        requestorMap.find(node => { node._2 == sender()}).get._1 ! Update_Finger_table_response(success)

        sender ! Kill

      case Update_others_request() =>
        log.info("At the update others request")

//        if(joining == true) {
//          sender ! Update_others_response(true)
//        }
//        else {
          SubActorCount +=1
          val update_others_subActor =MyActorSystem.actorOf(Props(new Update_others_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
          val requestMap_Pair = (sender(), update_others_subActor)
          requestorMap += requestMap_Pair
          update_others_subActor ! Update_others_subActor_Init(identifier)
          update_others_subActor ! Update_others_request()
//        }

      case Update_others_response(success) =>
        log.info("At the node : At the update others response")

        requestorMap.find(node => { node._2 == sender()}).get._1 ! Update_others_response(success)

        sender ! Kill

      case Find_successor_request(id) =>

        if(id == identifier) {
            sender ! Find_successor_response (identifier)
          }
        else {
          log.info("At the node : Find successor request")
          SubActorCount+=1
          val find_successor_subActorRef = MyActorSystem.actorOf(Props(new Find_successor_subActor),name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
          val requestMap_Pair = (sender(),find_successor_subActorRef)
          requestorMap += requestMap_Pair
          find_successor_subActorRef ! Find_successor_subActor_Init(identifier)
          find_successor_subActorRef ! Find_successor_request(id)

        }


      case Find_successor_response(id) =>

        log.info("At the node : find successor response " + id)
//        Thread.sleep(10000)
        requestorMap.find(node => { node._2 == sender()}).get._1 ! Find_successor_response(id)

        sender ! Kill

      case Find_predecessor_request(id) =>

        if(id == identifier) {
          sender ! Find_predecessor_response (predecessorID)
        }
        else {

          log.info("At th Node : Find predecessor request")
          SubActorCount += 1
          val find_predecessor_subActorRef = MyActorSystem.actorOf(Props(new Find_predecessor_subActor), name = "Node" + identifier.toString + "sSubActorNumber" + SubActorCount.toString)
          val requestMap_Pair = (sender(),find_predecessor_subActorRef)
          requestorMap += requestMap_Pair
          find_predecessor_subActorRef ! Find_predessor_subActor_Init(identifier)
          find_predecessor_subActorRef ! Find_predecessor_request(id)
        }


      case Find_predecessor_response(id) =>

        log.info ("At the Node: Find predecesssor response")

        requestorMap.find(node => { node._2 == sender()}).get._1 ! Find_predecessor_response(id)

        sender ! Kill

      case Closest_preceding_finger_request(id) =>

        log.info("At the closest preceding finger request in the Node " + identifier.toString + " id is " + id.toString)
//        Thread.sleep(10000)

        var success : Boolean = false
        var position :Long = -1

        for ( i <- m-1 to 0 by -1) {

          if(success == false) {
//            println(finger_table(1)._2)
//            Thread.sleep(10000)
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

//      println("Node Receive")
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
//          log.info("At Join Sub Actor: Joining Node is " + MyparentID + " and existing node is " + nodeID)
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
          log.info("At Join Sub Actor: Joining Node is " + MyparentID + " and existing node is " + nodeID)

//          Thread.sleep(10000)

          val init_Finger_Table_future : Future[Init_Finger_Table_response] = (NodeIDtoActorRef(MyparentID) ? Init_Finger_Table_request(nodeID)).mapTo[Init_Finger_Table_response]
          val init_Finger_table_result = Await.result(init_Finger_Table_future,TimeoutDuration.duration)
//          log.info("@@@@@***********************************")
//          log.info(init_Finger_table_result.Finger_table.foreach(println(_)).toString)

//          Thread.sleep(10000)
          val setFinger_table_future : Future[SetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? SetFinger_tableReq(init_Finger_table_result.Finger_table)).mapTo[SetFinger_tableRsp]
          val setFinger_table_result = Await.result(setFinger_table_future,TimeoutDuration.duration)

//          NodeIDtoActorRef(MyparentID) ! SetFinger_tableReq(init_Finger_table_result.Finger_table) //Ask to Joji
//
//          Thread.sleep(10000)
          val update_others_future : Future[Update_others_response] = (NodeIDtoActorRef(MyparentID) ? Update_others_request()).mapTo[Update_others_response]
          val update_others_result = Await.result(update_others_future,TimeoutDuration.duration)
//          log.info("@@@@@***********************************$$$$$$$$$$$$")
//          Thread.sleep(10000)

          log.info("At the Join Sub Actor: After Update others response")
//          Thread.sleep(10000)
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

        log.info("At the Update others requesttttttttt")
//        Thread.sleep(10000)
        for(i <- 0 until m)  {

          if((MyparentID - (1 << i)) < 0)
            nminus2tothepower1minus1 =  (MyparentID - (1 << i)) + max_number_of_nodes
          else
            nminus2tothepower1minus1 = MyparentID - (1 << i)

          println(MyparentID + " -------@@@@@@@@------- " + nminus2tothepower1minus1)

//          Thread.sleep(10000)
          val p_future : Future [Find_predecessor_response] = (NodeIDtoActorRef(MyparentID) ? Find_predecessor_request(nminus2tothepower1minus1)).mapTo[Find_predecessor_response]
//val p_future : Future [Find_predecessor_response] = (NodeIDtoActorRef(MyparentID) ? Find_predecessor_request(0)).mapTo[Find_predecessor_response]
          val p_result = Await.result(p_future,TimeoutDuration.duration)

          val p : Long = p_result.nodeID
          log.info("pppppp: " +p)
//          Thread.sleep(10000)
          val update_Finger_table_future : Future[Update_Finger_table_response] = (NodeIDtoActorRef(p) ? Update_Finger_table_request(MyparentID,i)).mapTo[Update_Finger_table_response]
          val update_Finger_table_result = Await.result(update_Finger_table_future,TimeoutDuration.duration)
//          NodeIDtoActorRef(p) ! Update_Finger_table_request(MyparentID,i)
      }
        log.info("After Update finger table response")
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
        log.info("Atttttttt theeeeeee update finger table request "+nodeID.toString+" "+i.toString)

//        Thread.sleep(100000)

        var finger_table_inside_subActor : Array[(Long, Long)] = new Array[(Long, Long)](m)

        val get_finger_table_future :Future[GetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? GetFinger_tableReq).mapTo[GetFinger_tableRsp]
        val get_finger_table_result = Await.result(get_finger_table_future,TimeoutDuration.duration)

        finger_table_inside_subActor = get_finger_table_result.Finger_Table

        finger_table_inside_subActor.foreach(x => log.info(x._1.toString+" "+x._2.toString))
//        Thread.sleep(10000)

        if(BelongsTo(nodeID,MyparentID,finger_table_inside_subActor(i)._2,true,false))  {
          log.info(" theeeeeee update finger table request "+nodeID.toString+" "+i.toString)
//          Thread.sleep(10000)
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

          log.info(s"pppp@@@@@@@ $p")
//          Thread.sleep(10000)
//
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

        log.info("At Init Finger Table Sub Actor : Joining Node is " + MyparentID + " and existing node is " + nodeID)
//        Thread.sleep(10000)

        val finger_table_inside_subActor : Array[(Long, Long)] = new Array[(Long, Long)](m)
        for(i <- 0 until m) {
          finger_table_inside_subActor(i) = {(finger_start(MyparentID,i+1),-1.toLong)}
        }

//        finger_table_inside_subActor.foreach(println(_))
//        Thread.sleep(100000)


        log.info(s"asking $nodeID")
        log.info("asking for "+finger_table_inside_subActor(0)._1.toString)
//        Thread.sleep(10000)
        val find_successsor_future : Future[Find_successor_response] = (NodeIDtoActorRef(nodeID) ? Find_successor_request(finger_table_inside_subActor(0)._1)).mapTo[Find_successor_response]
//val find_successsor_future : Future[Find_successor_response] = (NodeIDtoActorRef(3) ? Find_successor_request(5.toLong)).mapTo[Find_successor_response]
        val find_successor_result = Await.result(find_successsor_future,TimeoutDuration.duration)


        log.info("successor responseeeeeeeeeeeeeee " + find_successor_result.nodeID.toString )
//        Thread.sleep(10000)

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
//            val setFingertable_future : Future[SetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? SetFinger_tableReq(finger_table_inside_subActor)).mapTo[SetFinger_tableRsp]
//            val setFingertable_result = Await.result(setFingertable_future,TimeoutDuration.duration)
          }
          else {

            val find_successor_ndash_future :Future[Find_successor_response] = (NodeIDtoActorRef(nodeID) ? Find_successor_request(finger_table_inside_subActor(i+1)._1)).mapTo[Find_successor_response]
            val find_successor_ndash_result = Await.result(find_successor_ndash_future,TimeoutDuration.duration)

            finger_table_inside_subActor(i+1) = {(finger_table_inside_subActor(i+1)._1,find_successor_ndash_result.nodeID)}
//            val setFingertable_future : Future[SetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? SetFinger_tableReq(finger_table_inside_subActor)).mapTo[SetFinger_tableRsp]
//            val setFingertable_result = Await.result(setFingertable_future,TimeoutDuration.duration)
          }
        }
        println("***********************************")
        log.info(finger_table_inside_subActor.foreach(println(_)).toString)
//        Thread.sleep(100000)
//
//        val setFingertable_future : Future[SetFinger_tableRsp] = (NodeIDtoActorRef(MyparentID) ? SetFinger_tableReq(finger_table_inside_subActor)).mapTo[SetFinger_tableRsp]
//        val setFingertable_result = Await.result(setFingertable_future,TimeoutDuration.duration)

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

        log.info("At the Find successor sub actor : find successor request: ndash is " + MyparentID + " and finger(1).start of n is " + id)
//        Thread.sleep(10000)

        val find_predessor_future : Future[Find_predecessor_response] = (Nodes.find(nodeid => {nodeid._1 == MyparentID}).get._2 ? Find_predecessor_request(id)).mapTo[Find_predecessor_response]
        val find_predessor_result = Await.result(find_predessor_future,TimeoutDuration.duration)

        val ndashID : Long = find_predessor_result.nodeID

        log.info("predecessorrrrrrrrrr response " + ndashID)
//        Thread.sleep(10000)

        val get_successor_of_ndash_future : Future[GetSuccesssorRsp] = (NodeIDtoActorRef(ndashID) ? GetsuccessorReq).mapTo[GetSuccesssorRsp]
        val get_successor_of_ndash_result = Await.result(get_successor_of_ndash_future,TimeoutDuration.duration)

        val successorID_of_ndash : Long = get_successor_of_ndash_result.nodeID
//        log.info("Successor of ndash id after fnd predecessor " + successorID_of_ndash)
//        Thread.sleep(10000)

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
//        log.info("find predecessor init")
        MyparentID = nodeID

      case Find_predecessor_request(id) =>
        log.info("At the find predecessor sub actor: find predecesssor request: Joining Node is " + MyparentID + " and id is " + id)
//        Thread.sleep(10000)
        var ndashId :Long = MyparentID
        var newNdashId: Long = MyparentID
        var Exit_Loop : Boolean = false
        val getsuccessor_future : Future[GetSuccesssorRsp] = (NodeIDtoActorRef(ndashId) ? GetsuccessorReq).mapTo[GetSuccesssorRsp]
        val getSuccessor_result = Await.result(getsuccessor_future,TimeoutDuration.duration)

        var successorID_of_nDash : Long = getSuccessor_result.nodeID

        log.info("ndashID = " + ndashId + " and successor of ndash is " + successorID_of_nDash)
//        Thread.sleep(100000)



        while((!BelongsTo(id,ndashId,successorID_of_nDash,false,true)) && (Exit_Loop == false)) {

          println("in while")
          val closest_preceding_finger_future: Future[Closest_preceding_finger_response] = (NodeIDtoActorRef(ndashId) ? Closest_preceding_finger_request(id)).mapTo[Closest_preceding_finger_response]
          val closest_preceding_finger_result = Await.result(closest_preceding_finger_future, TimeoutDuration.duration)

          newNdashId = closest_preceding_finger_result.NodeID
//          log.info("ndashID = " + ndashId + " and new ndash is " + newNdashId)
//          Thread.sleep(10000)
          if (newNdashId == ndashId) {
            Exit_Loop = true
//            println("hi")
//            Thread.sleep(100000)
          }
          ndashId = newNdashId

          log.info("ndaaaaaaash "+ndashId.toString)
          val getsuccessor1_future: Future[GetSuccesssorRsp] = (NodeIDtoActorRef(ndashId) ? GetsuccessorReq).mapTo[GetSuccesssorRsp]
          val getSuccessor1_result = Await.result(getsuccessor1_future, TimeoutDuration.duration)
          successorID_of_nDash = getSuccessor1_result.nodeID

          println (ndashId + " " + successorID_of_nDash)
//          Thread.sleep(10000)

        }
//        log.info("ndashid: " +ndashId)
//        Thread.sleep(10000)
         NodeIDtoActorRef(MyparentID)! Find_predecessor_response(ndashId)

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
        println("Maximum_number of nodes allowed " + max_number_of_nodes )

        val NodeRef1 = MyActorSystem.actorOf(Props(new Node), name = 0.toString)
        val NodeRef2 = MyActorSystem.actorOf(Props(new Node), name = 1.toString)
        val NodeRef3 = MyActorSystem.actorOf(Props(new Node), name = 3.toString)
        val NodeRef4 = MyActorSystem.actorOf(Props(new Node), name = 6.toString)

        Nodes += {(0,NodeRef1)}
        Nodes += {(3,NodeRef2)}
        Nodes += {(4,NodeRef3)}
        Nodes += {(13,NodeRef4)}


//        NodeRef1 ! NodeInit(0)
//        NodeRef2 ! NodeInit(1)
//        NodeRef3 ! NodeInit(3)

        val zerossfingeratable : Array[(Long,Long)] = new Array[(Long, Long)](m)
        val onesfingertable : Array[(Long,Long)] = new Array[(Long, Long)](m)
        val threesfingertable : Array[(Long,Long)] = new Array[(Long, Long)](m)
        val sixsfingertable  : Array[(Long,Long)] = new Array[(Long, Long)](m)

        zerossfingeratable(0) = {(1,3)}
        zerossfingeratable(1) = {(2,3)}
        zerossfingeratable(2) = {(4,4)}
        zerossfingeratable(3) = {(8,0)}
////
        onesfingertable(0) = {(4,4)}
        onesfingertable(1) = {(5,0)}
        onesfingertable(2) = {(7,0)}
        onesfingertable(3) = {(11,0)}
//
        threesfingertable(0) = {(5,0)}
        threesfingertable(1) = {(6,0)}
        threesfingertable(2) = {(8,0)}
        threesfingertable(3) = {(12,0)}

//        sixsfingertable(0) = {(7,0)}
//        sixsfingertable(1) = {(0,0)}
//        sixsfingertable(2) = {(2,3)}

        NodeRef1 ! TestInit(3,4,zerossfingeratable,0)
        NodeRef2 ! TestInit(4,0,onesfingertable,3)
        NodeRef3 ! TestInit(0,3,threesfingertable,4)
//        NodeRef4 ! TestInit(0,3,sixsfingertable,6)


//        NodeRef1 ! NodeInit(0)
//        NodeRef2 ! NodeInit(1)
//        NodeRef3 ! NodeInit(3)
        NodeRef4 ! NodeInit(13)


//        for(i <-0 until no_of_nodes) {
//          val aRandomNumber: Int = Random.nextInt(no_of_nodes * 100)

//          identifier = SHA1(aRandomNumber, m)
//          println(aRandomNumber + " " + identifier)
//          Thread.sleep(1000)

//          val NodeRef = MyActorSystem.actorOf(Props(new Node), name = identifier.toString)


//          val Node_ID_pair = (identifier, NodeRef)

//          Nodes += Node_ID_pair

//          Nodes.foreach(println(_))
//          NodeRef ! NodeInit(identifier)

//          println("Joining Node : " + Nodes(1)._2.path.name + " and existing Node:  " + Nodes(2)._1)
//        Thread.sleep(100000)


        val join_future : Future[Join_response] = (Nodes(3)._2 ? Join(Nodes(0)._1)).mapTo[Join_response]
        val join_result = Await.result(join_future,TimeoutDuration.duration)

        log.info("-------------------------")
//        Thread.sleep(10000)


//        }
//
//
//        for(i <-0 until 4) {
//
//          if(i==0) {
//
//            val join_future : Future[Join_response] = (Nodes(i)._2 ? Join(-1)).mapTo[Join_response]
//            val join_result = Await.result(join_future,TimeoutDuration.duration)
//
//          }
//
//          else {
////
//////            println("not new node")
//            val join_future : Future[Join_response] = (Nodes(i)._2 ? Join(Nodes(i-1)._1)).mapTo[Join_response]
//            val join_result = Await.result(join_future,TimeoutDuration.duration)
//////            Nodes(i)._2 ! Join(Nodes(i-1)._1)
//
//          }
//
//       }
        for(i <-0 until 4) {

          val print_future: Future[Printresponse] = (Nodes(i)._2 ? Printrequest).mapTo[Printresponse]
          val print_result = Await.result(print_future,TimeoutDuration.duration)
          Thread.sleep(1000)
        }

        context.system.shutdown()





    }
  }

}
