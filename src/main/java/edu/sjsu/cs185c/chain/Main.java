package edu.sjsu.cs185c.chain;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

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

@Command(name = "chain_replica", mixinStandardHelpOptions = true, description = "replicate")

public class Main implements Callable<Integer> {

  public Object lock = new Object();
  public boolean newTail = true;
  public boolean tail = true;
  public String name = "jaime";
  public String hostPort;
  public String controlPath;
  public String data;
  public String replicaId;
  public ZooKeeper zk;
  public QueueThread predThread;
  public QueueThread succThread;
  // public String predHostPort = null; // is it bad to have these? do we need locks? ---------------------
  // public String succHostPort = null;
  public int lastXid = 0;
  public int lastAck = 0;
  public HashMap<Integer, UpdateRequest> sent = new HashMap<>();
  public HashMap<String, Integer> values = new HashMap<>();

  class HeadChainReplicaImpl extends HeadChainReplicaImplBase {

    public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
      HeadResponse response;
      // if not head return response 1
      if (predThread != null) {
        response = HeadResponse.newBuilder().setRc(1).build();
      } else {
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
        synchronized (lock) {
          xid = ++lastXid;
          processUpdate(key, value, xid);// lock until this request has been ack'd... right?
          try {
            while (lastAck < xid) {
              lock.wait();
            }
          } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
          }
        }
        
      }

      // return response
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

  }

  class TailChainReplicaImpl extends TailChainReplicaImplBase {
    // gets the current value of a key or 0 if the key does not exist
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
      String key = request.getKey();
      GetResponse response = GetResponse.newBuilder().setValue(values.get(key)).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  class ReplicaImpl extends ReplicaImplBase {
    // recieve an update (called by predecessor)
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
      String key = request.getKey();
      Integer newValue = request.getNewValue();
      Integer xid = request.getXid();

      // processUpdate() updates map and sent list, and adds request to appropriate queue
      synchronized (lock) {
        processUpdate(key, newValue, xid);// lock until this request has been ack'd... right?
        try {
          while (lastAck < xid) {
            lock.wait();
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(1);
        }
      }
    }

    // does a state transfer (called by predecessor)
    // ANYTIME a new successor is connected
    // TODO: check this implementation
    public void stateTransfer(StateTransferRequest request, StreamObserver<StateTransferResponse> responseObserver) {
      StateTransferResponse response;
      // get sent list
      List<UpdateRequest> sentList = request.getSentList();
      
      if (!newTail) {
        // ack all requests in sent list with ids lower than or equal to last ack
        Iterator listIterator = sentList.iterator();
        int currXid = ((UpdateRequest) listIterator.next()).getXid();
        while (listIterator.hasNext()) {
          if (currXid <= lastAck) {
            AckRequest ackRequest = AckRequest.newBuilder().setXid(currXid).build();
            predThread.addToQueue(ackRequest);
          }
        }
        response = StateTransferResponse.newBuilder().setRc(1).build();
      } else {
        // update values map
        Map<String, Integer> newState = request.getStateMap();
        Iterator mapIterator = newState.entrySet().iterator();
        while (mapIterator.hasNext()) {
          Map.Entry item = (Map.Entry) mapIterator.next();
          values.put((String) item.getKey(), (Integer) item.getValue());
        }
        // ack all requests in sent list
        Iterator listIterator = sentList.iterator();
        int currXid = ((UpdateRequest) listIterator.next()).getXid();
        while (listIterator.hasNext()) {
          AckRequest ackRequest = AckRequest.newBuilder().setXid(currXid).build();
          predThread.addToQueue(ackRequest);
          if (lastAck < currXid) lastAck = currXid;
        }
        response = StateTransferResponse.newBuilder().setRc(0).build();
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
      int xid = request.getXid();
      synchronized (lock) {
        // remove from sent list
        sent.remove(xid);
        // set lastAck
        lastAck = xid;
        lock.notifyAll();
        // ack predecessor
        if (predThread != null) {
          AckRequest ackRequest = AckRequest.newBuilder().setXid(xid).build();
          predThread.addToQueue(ackRequest);
        }
      }

      AckResponse response = AckResponse.newBuilder().build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  }

  class ChainDebugImpl extends ChainDebugImplBase {
    public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
      // TODO: is xid supposed to be lastXid or lastAck?
      ChainDebugResponse response = ChainDebugResponse.newBuilder().putAllState(values).setXid(lastXid).addAllSent(sent.values()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
      System.exit(0);
    }
  }

  @CommandLine.Parameters(index = "2")
  String zookeeper_server_list;

  @CommandLine.Parameters(index = "3")
  String control_path; // this is like /lunch

  @CommandLine.Parameters(index = "4")
  String host_port;

  @Override
  public Integer call() {
    hostPort = host_port;
    controlPath = control_path;
    int port = Integer.parseInt(host_port.substring(hostPort.length() - 4));
    var server = ServerBuilder.forPort(port).addService(new HeadChainReplicaImpl())
        .addService(new TailChainReplicaImpl()).addService(new ReplicaImpl()).build();
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
    updateConnections();

    try {
      server.awaitTermination();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

    return 0;
  }
  
  // creates control path if it doesn't exist
  public void createControlPath() {
    try {
      Stat stat = zk.exists(controlPath, false);
      if (stat == null) zk.create(controlPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (Exception e) {
      if (e.getClass().isAssignableFrom(ConnectionLossException.class)) {
        createControlPath();
      } else {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  public void createZNode() {
    try {
      replicaId = zk.create(controlPath + "/replica-", data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL_SEQUENTIAL);
    } catch (Exception e) {
      if (e.getClass().isAssignableFrom(ConnectionLossException.class)) {
        createZNode();
      } else {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  public void watchChildren() {
    try {
      zk.getChildren(controlPath, (e) -> {
      // set watcher again
      watchChildren();
    });

    // synchronized (lock) {
      updateConnections();
    // }
    } catch (Exception e) {
      watchChildren();
      // if (e.getClass().isAssignableFrom(ConnectionLossException.class)) {
      //   watchChildren();
      // } else {
      //   e.printStackTrace();
      //   System.exit(1);
      // }
    }
  }

  public void updateConnections() {
    try {
      List<String> children = zk.getChildren(controlPath, false);
      Collections.sort(children);

      int index = children.indexOf(replicaId);
      Scanner scanner;

      if (index == 0) { // you are head
        // kill old predecessor thread, if it exists
        if (predThread != null) predThread.kill();
      } else {
        scanner = new Scanner(new String(zk.getData(children.get(index-1), false, new Stat())));
        String predHostPort = scanner.next();
        scanner.close();

        if (predThread != null) {
          if (!predThread.getHostPort().equals(predHostPort)) { // predecessor changed
            // kill old predecessor thread
            predThread.kill();
            // set up new predecessor thread
            predThread = new QueueThread(predHostPort, null);
          }
        } else { // you just started up, and you have a predecessor to connect to
          // set up new predecessor thread
          predThread = new QueueThread(predHostPort, null);
        }
      }

      if (index == children.size()-1) { // you are tail
        // kill old successor thread, if it exists
        if (succThread != null) succThread.kill();
      } else {
        // get successor hostPort
        scanner = new Scanner(new String(zk.getData(children.get(index+1), false, new Stat())));
        String succHostPort = scanner.next();
        scanner.close();
        if (succThread != null) { // you're still not the tail
          if (!succThread.getHostPort().equals(succHostPort)) { // you have a new successor
            // kill old successor thread
            succThread.kill();
            // set up stateTransfer
            StateTransferRequest stRequest = StateTransferRequest.newBuilder().putAllState(values).setXid(lastXid).addAllSent(sent.values()).build();
            // set up new successor thread
            succThread = new QueueThread(succHostPort, stRequest);
          }
        } else { // you're no longer tail, you have a new successor
        // set up stateTransfer
        StateTransferRequest stRequest = StateTransferRequest.newBuilder().putAllState(values).setXid(lastXid).addAllSent(sent.values()).build();
        // set up new successor thread
        succThread = new QueueThread(succHostPort, stRequest);
        }
      }
    } catch (Exception e) {
      if (e.getClass().isAssignableFrom(ConnectionLossException.class)) {
        updateConnections();
      } else {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }
  
  public void processUpdate(String key, int newVal, int xid) {
    
    values.put(key, newVal);

    if (succThread != null) {
      // add updateRequest to sent list and successor queue
      UpdateRequest updateRequest = UpdateRequest.newBuilder().setKey(key).setNewValue(newVal).setXid(xid).build();
      sent.put(xid, updateRequest);
      succThread.addToQueue(updateRequest);
    } else if (predThread != null) {
      // add ackReqest to predecessor queue
      AckRequest ackRequest = AckRequest.newBuilder().setXid(xid).build();
      predThread.addToQueue(ackRequest);
    }
    
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }

  class QueueThread extends Thread {
    private String hostPort;
    private ReplicaBlockingStub stub;
    private LinkedBlockingQueue<GeneratedMessageV3> queue;
    private boolean finished = false;

    public QueueThread(String newHostPort, StateTransferRequest stRequest) {
      hostPort = newHostPort;
      ManagedChannel channel = ManagedChannelBuilder.forTarget(hostPort).usePlaintext().build();
      stub = ReplicaGrpc.newBlockingStub(channel);
      queue = new LinkedBlockingQueue<>();
      if (stRequest != null) queue.add(stRequest);
    }

    public void run() {
      boolean sendSuccess;
      while (!finished) {
        try {
          GeneratedMessageV3 request = queue.take();
          sendSuccess = false;
          if (request.getClass().isAssignableFrom(AckRequest.class)) {
            while (!sendSuccess) {
              try {
                stub.ack((AckRequest)request);
                sendSuccess = true;
              } catch (Exception e) { // SPECIFIC EXCEPTION? ------------------------------------------------
                System.out.println("Sending AckRequest again");
              }
            }
          } else if (request.getClass().isAssignableFrom(UpdateRequest.class)) {
            while (!sendSuccess) {
              try {
                stub.update((UpdateRequest)request);
                sendSuccess = true;
              } catch (Exception e) {
                System.out.println("Sending UpdateRequest again");
              }
            }
          } else if (request.getClass().isAssignableFrom(StateTransferRequest.class)) {
            while (!sendSuccess) {
              try {
                stub.stateTransfer((StateTransferRequest)request);
                sendSuccess = true;
              } catch (Exception e) {
                System.out.println("Sending StateTransferRequest again");
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
      synchronized (lock) {
        queue.add(request);
      }
    }

    public String getHostPort() {
      return hostPort;
    }

    public void kill() {
      finished = true;
      queue.add(AckRequest.newBuilder().setXid(0).build());
    }
  }
}
