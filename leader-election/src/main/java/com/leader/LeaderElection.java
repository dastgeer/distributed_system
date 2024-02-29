package com.leader;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements  Watcher{

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION=3000;
    private static final String ELECTION_NAMESPACE="/election";
    private static final String TARGET_ZNODE ="/target_znode";
    private ZooKeeper zookeeper;
    private String currentZnodeName;


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection eletion = new LeaderElection();
        eletion.connectToZooKeeper();
        eletion.volunteerForLeader();
        eletion.reelectLeader();
        //to watch event preciously than go out we will wait the thread on zookeeper object
        System.out.println("main ka thread name check-->"+Thread.currentThread().getName());
        eletion.run();
        eletion.close();
    }

    private void close() throws InterruptedException {
        zookeeper.close();
    }

    private void volunteerForLeader() throws KeeperException, InterruptedException {
        String zNodePrefix = ELECTION_NAMESPACE+"/c_";//here c just given to represent candiddate under election znode
        //to create znode in zookeepr,and want to put data inside znode like doing from zcli creating znode puttin data into it.
        //emphemarl means if disconneted from zookeeper then znode will be deleted.here name of znode appened with sequqnce nummber while creating znode
        //this sequential create sequenctial node
        String znodeFullPath = zookeeper.create(zNodePrefix,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name: "+znodeFullPath);
        currentZnodeName  = znodeFullPath.replace(ELECTION_NAMESPACE+"/","");
    }


    public void run()  {
        synchronized(zookeeper){
            try {
                System.out.println("before wait ka kon sa thread"+Thread.currentThread().getName());
                zookeeper.wait();
                System.out.println("after wait ka kon sa thread"+Thread.currentThread().getName());

            }catch (InterruptedException e){
                System.out.println("interupted from cmd line "+e);
            }
        }
    }
    //this method used to connect wwith zookeeper instance
    public void connectToZooKeeper() throws IOException {
        zookeeper= new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION,  this);
    }
//this method is used to track all activity for creation,deletion of znode, modification of znode, there childeren of znode, data of znode etc
    // it will be get call from event watcher method everytime to just notify it.
    public void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stats = zookeeper.exists(TARGET_ZNODE, this);// this will give stats class of zookeepr  give all information about znode an dchilds
        if(stats==null){ //if not present that node
            return;
        }
        byte[] data = zookeeper.getData(TARGET_ZNODE, this, stats);///it will give the znode details
        List<String> children = zookeeper.getChildren(TARGET_ZNODE, this);
        System.out.println("Data :"+new String(data)+" childerens: "+children);

    }

    //for elect or re-elect leader
    private void reelectLeader() throws KeeperException, InterruptedException {

        String predecessorZnodenmae="";
        Stat predecessorStats=null;
        //this while loop will run 2 suitaition , 1. if still no leader or this instance becaome leader till that time
        //2. when predecessor node deleted and it will notify by watch and nodeleted event we have to attach handler to call this method() to get selected as leader
        while(predecessorStats==null) {
            //get childerens znode of election znode
            List<String> childrens = zookeeper.getChildren(ELECTION_NAMESPACE, false);
            //we have find the smaller znode and make them leader,for that first sort in lexicography
            Collections.sort(childrens);
            String smallestChild = childrens.get(0);//because after sort first one will be small
            if (smallestChild.equalsIgnoreCase(currentZnodeName)) {
                System.out.println("I am the leader: " + currentZnodeName);
                return;
            } else {
                System.out.println("I am not the leader : " + smallestChild + " is the leader");
                int predecessorIndex = Collections.binarySearch(childrens, currentZnodeName) - 1;//it will give previous node index value from current znode name
                predecessorZnodenmae = childrens.get(predecessorIndex);
                predecessorStats = zookeeper.exists(ELECTION_NAMESPACE+"/"+predecessorZnodenmae, this);
            }
        }
        System.out.println("watchning znode:"+predecessorZnodenmae);
        System.out.println();
    }

    //this method is from watcher iterface it will notify for all event when you register with zookeeper but we have register everytime ,it means
    //have to pass object refence to zookeeper method so keep track of it.
    @Override
    public void process(WatchedEvent watchedEvent) {

        System.out.println("procee thread name-->"+Thread.currentThread().getName());
        switch(watchedEvent.getType()){
            //it will notify for connetion and disconnection of zookepper
            case None:
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("successfully connected to zookeepr");
                }else{
                    System.out.println("disconnected form zookeepr evenet");
                    synchronized (zookeeper){
                        zookeeper.notify();
                    }
                }
                break;
            case NodeDeleted:
                System.out.println(TARGET_ZNODE +" was deleted;");
                try {
                    reelectLeader();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE +"was created: ");
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE +" data changed :");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE +" childeren changed :");
                break;
        }
        try{
            watchTargetZnode();//here we called for 2 purpose  get all update to date data and print to screen, and all eent are one time trigger and
            //interseted in calling same event infortion then have to call it again by registring that to zookeeper methods.
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
