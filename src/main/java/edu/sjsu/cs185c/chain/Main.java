package edu.sjsu.cs185c.chain;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.logging.Logger;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.GeneratedMessageV3;

import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.IncRequest;
import edu.sjsu.cs249.chain.ReplicaGrpc;
import edu.sjsu.cs249.chain.StateTransferRequest;
import edu.sjsu.cs249.chain.StateTransferResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc;
import edu.sjsu.cs249.chain.UpdateRequest;
import edu.sjsu.cs249.chain.UpdateResponse;
import edu.sjsu.cs249.chain.ChainDebugGrpc.ChainDebugImplBase;
import edu.sjsu.cs249.chain.HeadChainReplicaGrpc.HeadChainReplicaBlockingStub;
import edu.sjsu.cs249.chain.HeadChainReplicaGrpc.HeadChainReplicaImplBase;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaBlockingStub;
import edu.sjsu.cs249.chain.ReplicaGrpc.ReplicaImplBase;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaBlockingStub;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaImplBase;
import edu.sjsu.cs249.chain.AckRequest;
import edu.sjsu.cs249.chain.AckResponse;
import edu.sjsu.cs249.chain.Chain;
import edu.sjsu.cs249.chain.ChainDebugRequest;
import edu.sjsu.cs249.chain.ChainDebugResponse;
import edu.sjsu.cs249.chain.ExitRequest;
import edu.sjsu.cs249.chain.ExitResponse;
import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.HeadChainReplicaGrpc;

public class Main {

  private static final Logger LOGGER = Logger.getAnonymousLogger();
  public static Object lock = new Object();
  public static boolean newTail = true;
  // public boolean tail = true;
  public static String name = "jaime";
  public static String hostPort;
  public static String controlPath;
  public static String data;
  public static String replicaId;
  public static long sessionId;
  public static ZooKeeper zk;
  public static QueueThread predThread;
  public static QueueThread succThread;
  public static int lastXid = 0;
  public static int lastAck = 0;
  public static HashMap<Integer, UpdateRequest> sent = new HashMap<>();
  public static Map<String, Integer> values = new HashMap<>();

  static class HeadChainReplicaImpl extends HeadChainReplicaImplBase {

    public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
      System.out.println("INCREMENT() RUNNING");
      HeadResponse response;
      // if not head return response 1
      if (predThread != null) {
        System.out.println("I HAVE A PREDECESSOR");
        response = HeadResponse.newBuilder().setRc(1).build();
      } else {
        System.out.println("I AM HEAD");
        response = HeadResponse.newBuilder().setRc(0).build();
        // increment value
        String key = request.getKey();
        int incVal = request.getIncValue();
        Integer value = values.get(key);
        if (value != null)
          value += incVal;
        else
          value = incVal;

        int xid;
        // processUpdate() updates map and sent list, and adds request to appropriate queue
        System.out.println("LOCKING");
        synchronized (lock) {
          xid = ++lastXid;
          System.out.println("UPDATING KEY " + key + " TO " + value);
          processUpdate(key, value, xid); // lock until this request has been ack'd... right?
          if (succThread != null) {
            try {
              System.out.println("WAITING");
              while (lastAck < xid) {
                lock.wait();
              }
              System.out.println("DONE WAITING");
            } catch (Exception e) {
              e.printStackTrace();
              System.exit(1);
            }
          }
        }
        System.out.println("UNLOCKING");

      }

      // return response
      System.out.println("SENDING CONFIRMATION OF INC REQUEST");
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

  }

  static class TailChainReplicaImpl extends TailChainReplicaImplBase {
    // gets the current value of a key or 0 if the key does not exist
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
      GetResponse response;
      System.out.println("GET() RUNNING");
      String key = request.getKey();
      Integer value = values.get(key);
      if (value == null) value = 0;
      if (succThread != null) response = GetResponse.newBuilder().setValue(value).setRc(1).build();
      else response = GetResponse.newBuilder().setValue(value).setRc(0).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      System.out.println("GET() COMPLETED");
    }
  }

  static class ReplicaImpl extends ReplicaImplBase {
    // recieve an update (called by predecessor)
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
      System.out.println("UPDATE() RUNNING");
      String key = request.getKey();
      int newValue = request.getNewValue();
      int xid = request.getXid();
      if (lastXid < xid) lastXid = xid;

      UpdateResponse response = UpdateResponse.newBuilder().setRc(0).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();

      // processUpdate() updates map and sent list, and adds request to appropriate
      // queue
      System.out.println("LOCKING");
      synchronized (lock) {
        System.out.println("UPDATING KEY " + key + " TO " + newValue);
        processUpdate(key, newValue, xid); // lock until this request has been ack'd... right?
        try {
          while (lastAck < xid) {
            lock.wait();
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(1);
        }
      }
      System.out.println("UNLOCKING");
      
    }

    
    // does a state transfer (called by predecessor)
    // ANYTIME a new successor is connected
    public void stateTransfer(StateTransferRequest request, StreamObserver<StateTransferResponse> responseObserver) {
      System.out.println("STATETRANSFER() RUNNING");
      StateTransferResponse response;
      // get sent list
      List<UpdateRequest> sentList = request.getSentList();
      lastXid = request.getXid();
      
      if (!newTail) {
        // ack all requests in sent list with ids lower than or equal to last ack
        Iterator<UpdateRequest> listIterator = sentList.iterator();
        // UpdateRequest item = (UpdateRequest) listIterator.next();
        int currXid;
        while (listIterator.hasNext()) {
          UpdateRequest item = (UpdateRequest) listIterator.next();
          currXid = item.getXid();
          if (currXid <= lastAck) {
            System.out.println("LOCKING");
            synchronized (lock) {
              AckRequest ackRequest = AckRequest.newBuilder().setXid(currXid).build();
              predThread.addToQueue(ackRequest);
            }
            System.out.println("UNLOCKING");
          }
        }
        response = StateTransferResponse.newBuilder().setRc(1).build();
      } else {
        // update values map
        Map<String, Integer> newState = request.getStateMap();
        Iterator<Map.Entry<String, Integer>> mapIterator = newState.entrySet().iterator();
        while (mapIterator.hasNext()) {
          Map.Entry item = (Map.Entry) mapIterator.next();
          values.put((String) item.getKey(), (Integer) item.getValue());
        }
        // ack all requests in sent list
        Iterator<UpdateRequest> listIterator = sentList.iterator();
        UpdateRequest currItem;
        int currXid;
        while (listIterator.hasNext()) {
          currItem = (UpdateRequest) listIterator.next();
          currXid = currItem.getXid();
          System.out.println("LOCKING");
          synchronized (lock) {
            AckRequest ackRequest = AckRequest.newBuilder().setXid(currXid).build();
            predThread.addToQueue(ackRequest);
          }
          System.out.println("UNLOCKING");
          if (lastAck < currXid) lastAck = currXid;
        }
        response = StateTransferResponse.newBuilder().setRc(0).build();
        newTail = false;
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      System.out.println("STATETRANSFER() COMPLETED");
    }
    
    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
      System.out.println("ACK() RUNNING");
      int xid = request.getXid();
      System.out.println("LOCKING");
      synchronized (lock) {
        // remove from sent list
        sent.remove(xid);
        // set lastAck
        lastAck = xid;
        lock.notifyAll();
        // ack predecessor
        if (predThread != null) {
          System.out.println("FORWARDING ACK REQUEST");
          System.out.println("LOCKING");
          synchronized (lock) {
            AckRequest ackRequest = AckRequest.newBuilder().setXid(xid).build();
            predThread.addToQueue(ackRequest);
          }
          System.out.println("UNLOCKING");
        }
      }
      System.out.println("UNLOCKING");

      AckResponse response = AckResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  static class ChainDebug extends ChainDebugImplBase {
    public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
      System.out.println("DEBUG() RUNNING");
      // TODO: is xid supposed to be lastXid or lastAck?
      ChainDebugResponse response = ChainDebugResponse.newBuilder().putAllState(values).setXid(lastXid)
          .addAllSent(sent.values()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
      ExitResponse response = ExitResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      System.exit(0);
    }
  }

  @Command(name = "chain_replica", mixinStandardHelpOptions = true, description = "replicate")
  static class Cli implements Callable<Integer> {

    @CommandLine.Parameters(index = "0")
    String zookeeper_server_list;

    @CommandLine.Parameters(index = "1")
    String control_path; // this is like /lunch

    @CommandLine.Parameters(index = "2")
    String host_port;

    @Override
    public Integer call() {
      // got this line from Hamir to format the logged output nicely
      System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tT] [%4$-7s] [%2$-63s] %5$s%n");
      hostPort = host_port;
      controlPath = control_path;
      int port = Integer.parseInt(host_port.substring(hostPort.length() - 4));
      var server = ServerBuilder.forPort(port).addService(new HeadChainReplicaImpl())
          .addService(new TailChainReplicaImpl()).addService(new ReplicaImpl()).addService(new ChainDebug()).build();
      try {
        server.start();
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }

      try {
        zk = new ZooKeeper(zookeeper_server_list, 10000, (e) -> {
          System.out.println(e);
        });
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }

      // if controlPath doesn't exist, create it
      createControlPath();

      // create your znode
      data = hostPort + "\n" + name;
      createZNode();

      // set watch on children
      watchChildren();

      // set predecessor and successor as applicable
      // System.out.println("SETTING CONNECTIONS FOR THE FIRST TIME");
      // updateConnections();

      try {
        server.awaitTermination();
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
      }

      return 0;
    }
  }

  // creates control path if it doesn't exist
  public static void createControlPath() {
    System.out.println("CREATING CONTROL PATH " + controlPath);
    boolean complete = false;
    while (!complete) {
      try {
        Stat stat = zk.exists(controlPath, false);
        if (stat == null)
          zk.create(controlPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        complete = true;
      } catch (KeeperException.ConnectionLossException e) {
        System.out.println("ConnectionLossException handled");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  // creates zNode if it doesn't exist
  public static void createZNode() {
    boolean exists = false;
    boolean complete = false;
    while (!complete) {
      try {
        List<String> children = zk.getChildren(controlPath, null);
        for (String child : children) {
          Stat stat = new Stat();
          zk.getData(controlPath + "/" + child, null, stat);
          if (stat.getEphemeralOwner() == zk.getSessionId()) {
            replicaId = child;
            exists = true;
          }
        }
        if (!exists) {
          System.out.println("CREATING REPLICA ZNODE FOR " + hostPort);
          String completePath = zk.create(controlPath + "/replica-", data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.EPHEMERAL_SEQUENTIAL);
          replicaId = completePath.substring(controlPath.length() + 1);
        }
        complete = true;
      } catch (KeeperException.ConnectionLossException e) {
        System.out.println("ConnectionLossException handled");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  public static void watchChildren() {
    boolean complete = false;
    while (!complete) {
      try {
        zk.getChildren(controlPath, (e) -> {
          // set watcher again
          watchChildren();
        });

        // synchronized (lock) {
        updateConnections();
        // }
        complete = true;
      } catch (KeeperException.ConnectionLossException e) {
        System.out.println("ConnectionLossException handled");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  public static void updateConnections() {
    System.out.println("SETTING CONNECTIONS");
    boolean complete = false;
    while (!complete) {
      try {
        List<String> children = zk.getChildren(controlPath, false);
        Collections.sort(children);
        // System.out.println(children.get(0));
        int index = children.indexOf(replicaId);
        Scanner scanner;

        if (index == 0) { // you are head
          // kill old predecessor thread, if it exists
          newTail = false;
          if (predThread != null) {
            predThread.kill();
            predThread = null;
          }
        } else {
          scanner = new Scanner(new String(zk.getData(controlPath + "/" + children.get(index - 1), false, new Stat())));
          String predHostPort = scanner.next();
          scanner.close();

          if (predThread != null) {
            if (!predThread.getHostPort().equals(predHostPort)) { // predecessor changed
              // kill old predecessor thread
              predThread.kill();
              // set up new predecessor thread
              predThread = new QueueThread(predHostPort, null);
              System.out.println("PREDECESSOR IS " + predThread.getHostPort());
              predThread.start();
            }
          } else { // you just started up, and you have a predecessor to connect to
            // set up new predecessor thread
            predThread = new QueueThread(predHostPort, null);
            System.out.println("PREDECESSOR IS " + predThread.getHostPort());
            predThread.start();
          }
        }

        if (index == children.size() - 1) { // you are tail
          // kill old successor thread, if it exists
          if (succThread != null) {
            succThread.kill();
            succThread = null;
          }
        } else {
          // get successor hostPort
          scanner = new Scanner(new String(zk.getData(controlPath + "/" + children.get(index + 1), false, new Stat())));
          String succHostPort = scanner.next();
          scanner.close();
          if (succThread != null) { // you're still not the tail
            if (!succThread.getHostPort().equals(succHostPort)) { // you have a new successor
              // kill old successor thread
              succThread.kill();
              // set up stateTransfer
              StateTransferRequest stRequest = StateTransferRequest.newBuilder().putAllState(values).setXid(lastXid)
                  .addAllSent(sent.values()).build();
              // set up new successor thread
              succThread = new QueueThread(succHostPort, stRequest);
              System.out.println("SUCCESSOR IS " + succThread.getHostPort());
              succThread.start();
            }
          } else { // you're no longer tail, you have a new successor
            // set up stateTransfer
            StateTransferRequest stRequest = StateTransferRequest.newBuilder().putAllState(values).setXid(lastXid)
                .addAllSent(sent.values()).build();
            // set up new successor thread
            succThread = new QueueThread(succHostPort, stRequest);
            System.out.println("SUCCESSOR IS " + succThread.getHostPort());
            succThread.start();
          }
        }
        complete = true;
      } catch (KeeperException.ConnectionLossException e) {
        System.out.println("ConnectionLossException handled");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  public static void processUpdate(String key, int newVal, int xid) {
    if (succThread != null) {
      System.out.println("SENDING UPDATEREQUEST");
      // add updateRequest to sent list and successor queue
      System.out.println("LOCKING");
      synchronized (lock) {
        values.put(key, newVal);
        UpdateRequest updateRequest = UpdateRequest.newBuilder().setKey(key).setNewValue(newVal).setXid(xid).build();
        sent.put(xid, updateRequest);
        succThread.addToQueue(updateRequest);
      }
      System.out.println("UNLOCKING");
      System.out.println("SENT UPDATEREQUEST");
    } else if (predThread != null) {
      System.out.println("SENDING ACK");
      // add ackReqest to predecessor queue
      System.out.println("LOCKING");
      synchronized (lock) {
        if (lastXid < xid) lastXid = xid;
        values.put(key, newVal);
        AckRequest ackRequest = AckRequest.newBuilder().setXid(xid).build();
        predThread.addToQueue(ackRequest);
      }
      System.out.println("UNLOCKING");
    }

  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new Cli()).execute(args));
  }

  static class QueueThread extends Thread {
    private String hostPort;
    private ReplicaBlockingStub stub;
    private LinkedBlockingQueue<GeneratedMessageV3> queue;
    private boolean finished = false;

    public QueueThread(String newHostPort, StateTransferRequest stRequest) {
      hostPort = newHostPort;
      ManagedChannel channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();
      stub = ReplicaGrpc.newBlockingStub(channel);
      queue = new LinkedBlockingQueue<>();
      if (stRequest != null)
        queue.add(stRequest);
    }

    public void run() {
      System.out.println("THREAD RUNNING");
      boolean sendSuccess;
      while (!finished) {
        try {
          System.out.println("TAKING NEXT QUEUE ITEM");
          GeneratedMessageV3 request = queue.take();
          System.out.println("TOOK NEXT QUEUE ITEM");
          sendSuccess = false;
          if (request.getClass().isAssignableFrom(AckRequest.class)) {
            while (!sendSuccess && !finished) {
              System.out.println("PROCESSING ACKREQUEST");
              try {
                stub.ack((AckRequest) request);
                sendSuccess = true;
              } catch (Exception e) { // SPECIFIC EXCEPTION? ------------------------------------------------
                e.printStackTrace();
                Thread.sleep(2000);
                System.out.println("SENDING ACKREQUEST AGAIN");
              }
            }
          } else if (request.getClass().isAssignableFrom(UpdateRequest.class)) {
            while (!sendSuccess && !finished) {
              System.out.println("PROCESSING UPDATEREQUEST");
              try {
                stub.update((UpdateRequest) request);
                sendSuccess = true;
              } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(2000);
                System.out.println("SENDING UPDATEREQUEST AGAIN");
              }
            }
          } else if (request.getClass().isAssignableFrom(StateTransferRequest.class)) {
            while (!sendSuccess && !finished) {
              System.out.println("PROCESSING STATETRANSFERREQUEST");
              try {
                stub.stateTransfer((StateTransferRequest) request);
                sendSuccess = true;
              } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(2000);
                System.out.println("SENDING STATETRANSFERREQUEST AGAIN");
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(1);
        }
      }
    }

    public void addToQueue(GeneratedMessageV3 request) {
        queue.add(request);
    }

    public String getHostPort() {
      return hostPort;
    }

    public void kill() {
      finished = true;
      // add anything to queue to allow kill signal to be read
      queue.add(AckRequest.newBuilder().setXid(0).build());
    }
  }
}




// subsequent updates don't get processed
// handle StatusRuntimeException for when another replica doesn't respond