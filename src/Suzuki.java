
import com.sun.corba.se.impl.corba.CORBAObjectImpl;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.beans.beancontext.BeanContext;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

public class Suzuki {
    // Charset
    static final Charset charset = StandardCharsets.UTF_8;

    //PORT NUMS
    static final int PORT1 = 51010;
    static final int COPORT = 51015;
    static ServerSocketChannel nodeServer;

    //CLOCK
    static volatile int llc_value = 0;
    static Object llc_lock = new Object();

    //COORDINATOR PROCESS PARAMS
    static int NUMBER_OF_PROCS;
    static volatile int numberOfProcessRegistered = 0;
    static volatile int numberOfProcessReady = 0;
    static final Object coordinatorLock = new Object();
    static final String CONFIG = "dsConfig";
    static Map<Integer, Set<Integer>> neighbours = new HashMap<>();
    static Map<Integer, String> pidToHostnameMap = new HashMap<>();
    static int PROCESSID;
    static boolean isCoordinator = false;
    static boolean coorServerStart = false;

    //NON COORDINATOR PROCESS PARAMS
    static volatile boolean canSendHello = false;
    static volatile boolean canSendReady = true;
    static volatile boolean canSendCSRequestAndTokens = false;
    static BlockingQueue<Integer> blockingQueue = new ArrayBlockingQueue<>(1);
    static volatile int numberOfNeighbours = 0;
    static volatile int neighbourCounter = 0;
    static Object lock = new Object();
    static Set<Neighbour> localNeighbourSet = new HashSet<>();
    static Map<Integer, String> localNeighbourMap = new HashMap<>();
    static Map<Integer, SocketChannel> neihgbourSockets = new HashMap<>();
    static volatile int[] sendArray;
    static volatile int[] recvArray;


    //SUZUKI KASAMI
    static volatile Object sk_lock1 = new Object();
    static volatile Object sk_lock2 = new Object();
    static volatile ArrayBlockingQueue<Integer> skValveSendReq = new ArrayBlockingQueue<Integer>(1);
    static volatile ArrayBlockingQueue<Integer> skValveSendToken = new ArrayBlockingQueue<Integer>(1);
    static volatile Object skSendTokenLock = new Object();
    static volatile BlockingQueue<Integer> skBlockingQueue ;
    static volatile int requestNumber = 0;
    static volatile CountDownLatch latch ;
    static volatile StringBuilder tokenBuilder;
    static volatile boolean terminate = false;

    /* SUZUKI KASAMI TOKEN RELATED */
    static volatile boolean hasToken = false;
    static volatile int[] tokenArray;
    static volatile Queue<Integer> queueOfProcsVchWillReceiveTheTokenNext;

    /* SUZUKI KASAMI STATS RELATED */
    static volatile Map<Integer,Long> forWaitTimeCalculation = Collections.synchronizedMap(new HashMap<>());
    static volatile Map<Integer,List<Long>> forWaitTimeCalculationFinal = Collections.synchronizedMap(new HashMap<>());
    static volatile Map<Integer,List<Long>> forSyncDelayCalculation = Collections.synchronizedMap(new HashMap<>());


    public static void main(String[] args) throws InterruptedException, IOException{
        Thread coorThread = null;

        if (args.length != 0 && args[0].equalsIgnoreCase("-c")) {
            isCoordinator = true;
            // Start CoOrdinate thread
            coorThread = new Thread(Suzuki::startCoordinate);
            coorThread.start();
        }

        try {
            // Start initializing
            startInitializing();
        }
        catch (Exception e) {
            e.printStackTrace();
            if (coorThread != null) {
                coorThread.interrupt();
            }
        }

        if (coorThread != null) {
            coorThread.join();
        }


        // Running the non coordinator part of the process
        /*Thread initializingThread = startInitializingThread();

        Thread clientThreadMain = startClientThreadMain();

        Thread serverThreadMain = startServerThreadMain();

        try {
            initializingThread.join();
            clientThreadMain.join();
            serverThreadMain.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
    }


    private static void startCoordinate() {

        //Running the coordinator part of the process
        runConfiguration(CONFIG);
        hasToken = true;

        Map<Integer, SocketChannel> clients = new HashMap<>();

        System.out.println("<i> ** Coordinator process initiated ** </i>");
        System.out.println("<i> ** Waiting for processes to register ** </i>");

        try {
            Selector clientSelector = Selector.open();
            SocketAddress co_addr = new InetSocketAddress(COPORT);
            ServerSocketChannel serverSocket = ServerSocketChannel.open();
            serverSocket.bind(co_addr);

            synchronized (coordinatorLock) {
                coorServerStart = true;
                coordinatorLock.notifyAll();
            }

            int i = 0;
            while (i < NUMBER_OF_PROCS) {
                SocketChannel client = serverSocket.accept();
                client.configureBlocking(true);

                ByteBuffer readbuf = ByteBuffer.allocate(64);
                client.read(readbuf);

                String recv = new String(readbuf.array(), charset).trim();

                if (!recv.equalsIgnoreCase("register")) {
                    client.close();
                    continue;
                }

                i++;
                int pid = i;
                clients.put(pid, client);

                String hostname = ((InetSocketAddress) client.getRemoteAddress()).getHostName();
                pidToHostnameMap.put(pid, hostname);

                System.out.printf("<i> Register from : %s , # of procs registered : %d </i>\n", hostname, pid);
            }

            // Send neighbour info to client
            // Fully connected network
            for (Map.Entry<Integer, SocketChannel> entry : clients.entrySet()) {
                int pid = entry.getKey();
                StringBuilder stringBuilder = new StringBuilder();

                stringBuilder.append(pid + "," + NUMBER_OF_PROCS);
                for (Map.Entry<Integer, String> e : pidToHostnameMap.entrySet()) {
                    if (e.getKey() != pid) {
                        stringBuilder.append(',');
                        stringBuilder.append(e.getKey() + " " + e.getValue());
                    }
                }

                ByteBuffer buf = ByteBuffer.wrap(stringBuilder.toString().getBytes(charset));

                entry.getValue().write(buf);
            }

            // Wait for clients to say READY
            i = 0;
            while (i < clients.size()) {
                int pid = i + 1;
                ByteBuffer readbuf = ByteBuffer.allocate(64);
                clients.get(pid).read(readbuf);

                String rcv = new String(readbuf.array(), charset).trim();

                if (rcv.equalsIgnoreCase("READY")) {
                    System.out.println("<i> Receive READY " + i + " </i>");
                    i++;
                }
            }

            // Say compute to all and end initialing
            for (SocketChannel client : clients.values()) {
                ByteBuffer buf = ByteBuffer.wrap("COMPUTE".getBytes(charset));
                client.write(buf);
                client.close();
            }
            serverSocket.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    private static void startInitializing() throws IOException, InterruptedException {
        if (isCoordinator) {
            // Wait for Coordinate thread to open Server
            synchronized (coordinatorLock) {
                while (!coorServerStart) {
                    coordinatorLock.wait();
                }
            }
        }

        // Create local server to receive hello from neighbours
        SocketAddress nodeAddr = new InetSocketAddress("localhost", PORT1);
        nodeServer = ServerSocketChannel.open();
        nodeServer.bind(nodeAddr);

        // Create connection to Coordinator
        BufferedReader reader = new BufferedReader(new FileReader(CONFIG));
        String[] params = reader.readLine().split(" ");

        SocketAddress coorAddr = new InetSocketAddress(params[1], COPORT);
        SocketChannel coor = SocketChannel.open(coorAddr);

        //PrintWriter out = new PrintWriter(client.getOutputStream(), true);
        //BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
        coor.write(ByteBuffer.wrap("REGISTER".getBytes(charset)));

        System.out.println("  Registration initiated");

        String[] totalNumberOfProcs = reader.readLine().split(" ");
        int arraySize = Integer.parseInt(totalNumberOfProcs[3]);
        sendArray = new int[arraySize];
        recvArray = new int[arraySize];
        tokenArray = new int[arraySize];
        queueOfProcsVchWillReceiveTheTokenNext = new LinkedList<>();
        skBlockingQueue = new ArrayBlockingQueue<>(1);
        skBlockingQueue.add(1);

        // Read neighbour info from CoOrdinator
        ByteBuffer readbuf = ByteBuffer.allocate(25 * 128);
        coor.read(readbuf);

        String line = new String(readbuf.array(), charset).trim();
        String[] parsedReceivedLine = line.split(",");
        System.out.println("  Registration success");

        PROCESSID = Integer.parseInt(parsedReceivedLine[0]);
        int NumProcs = Integer.parseInt(parsedReceivedLine[1]);

        System.out.printf("    PID: %d\n", PROCESSID);
        System.out.printf("    Neighbours----------PID:\n");
        for (int i = 2; i < parsedReceivedLine.length; i++) {
            String[] detailsOfNeighbour = parsedReceivedLine[i].split(" ");
            int npid = Integer.parseInt(detailsOfNeighbour[0]);
            String nhostname = detailsOfNeighbour[1];

            //localNeighbourSet.add(new Neighbour(detailsOfNeighbour[0], Integer.parseInt(detailsOfNeighbour[1])));
            localNeighbourMap.put(npid, nhostname);
            numberOfNeighbours++;
            System.out.println("    " + npid + ": " + nhostname);
        }
        System.out.println("    -----------------------");

        // Say hello and setup network to neighbours
        if (!sayHello()) {
            System.err.println("  Can not say HELLO to all neighbours");
            return;
        }

        // Say READY to coordinator
        System.out.println("  Send READY to coordinator");
        coor.write(ByteBuffer.wrap("READY".getBytes(charset)));

        // Wait for COMPUTE
        readbuf = ByteBuffer.allocate(64);
        coor.read(readbuf);
        line = new String(readbuf.array(), charset).trim();
        if (!line.equalsIgnoreCase("COMPUTE")) {
            System.err.println("  expect 'COMPUTE' but received " + line);
            return;
        }

        //
        System.out.println(">> PID " + PROCESSID + " start COMPUTE <<");
        System.out.println("--------------------------");

        // Close connection and end initializing
        coor.close();
    }


    private static boolean sayHello() {
        // Send HELLO to processes with larger PID
        Thread sendThread = new Thread(() -> {
            try {
                for (Map.Entry<Integer, String> entry : localNeighbourMap.entrySet()) {
                    int pid = entry.getKey();
                    if (pid <= PROCESSID) {
                        continue;
                    }

                    SocketAddress connectToAddr = new InetSocketAddress(entry.getValue(), PORT1);
                    SocketChannel connectTo = SocketChannel.open(connectToAddr);

                    // Add to neighbourSockets
                    neihgbourSockets.put(pid, connectTo);

                    connectTo.write(ByteBuffer.wrap(("HELLO " + PROCESSID).getBytes(charset)));
                }
            }
            catch (IOException ex) {
                ex.printStackTrace();
            }
        });
        sendThread.start();

        ByteBuffer readbuf = ByteBuffer.allocate(32);
        // Receive HELLO from processes with smaller PID
        try {
            for (int pid : localNeighbourMap.keySet()) {
                if (pid >= PROCESSID) {
                    continue;
                }

                SocketChannel connectFrom = nodeServer.accept();
                connectFrom.read(readbuf);

                String[] recv = (new String(readbuf.array(), charset)).split(" ");
                int frmpid = Integer.parseInt(recv[1]);

                System.out.println("  Received " + recv[0] + " from " + recv[1]);

                // Add to neighbourSockets
                neihgbourSockets.put(frmpid, connectFrom);
            }

            sendThread.join();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }

        return true;
    }


    private static Thread startInitializingThread() {
        Thread initializingThread = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //register with coordinator
            try {
                BufferedReader reader = new BufferedReader(new FileReader(CONFIG));
                String[] params = reader.readLine().split(" ");
                Socket client = new Socket(params[1], COPORT);
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                synchronized (llc_lock) {
                    llc_value++;
                    out.println(llc_value + ",register");
                    System.out.println();
                    System.out.printf("<%d> registration initiated%n", llc_value);
                    System.out.println();
                }

                String[] totalNumberOfProcs = reader.readLine().split(" ");
                int arraySize = Integer.parseInt(totalNumberOfProcs[3]);
                sendArray = new int[arraySize];
                recvArray = new int[arraySize];
                tokenArray = new int[arraySize];
                queueOfProcsVchWillReceiveTheTokenNext = new LinkedList<>();
                skBlockingQueue = new ArrayBlockingQueue<>(1);
                skBlockingQueue.add(1);

                String line;
                while ((line = in.readLine()) != null) {
                    String[] parsedReceivedLine = line.split(",");
                    if (parsedReceivedLine[1].equals("registered")) {
                        System.out.println();
                        System.out.println("<i> registration success");
                        System.out.println();
                        synchronized (llc_lock) {
                            int senderProcTS = Integer.parseInt(parsedReceivedLine[0]);
                            int maxOfTS = Math.max(senderProcTS, llc_value);
                            llc_value = maxOfTS + 1;
                            System.out.printf("<%d> received PID and neighbour list from coordinator%n", llc_value);
                            PROCESSID = Integer.parseInt(parsedReceivedLine[2]);
                            System.out.printf("    PID : %d%n", PROCESSID);
                            System.out.printf("    Neighbours----------PID%n");
                            for (int i = 3; i < parsedReceivedLine.length; i++) {
                                String[] detailsOfNeighbour = parsedReceivedLine[i].split(" ");
                                localNeighbourSet.add(new Neighbour(detailsOfNeighbour[0], Integer.parseInt(detailsOfNeighbour[1])));
                                numberOfNeighbours++;
                                System.out.println("    " + detailsOfNeighbour[0] + "   " + detailsOfNeighbour[1]);
                            }
                            System.out.println("    -----------------------");
                            System.out.println();
                        }
                        synchronized (lock) {
                            canSendHello = true;
                        }
                    }
                    while (true) {
                        if (!blockingQueue.isEmpty())
                            break;
                    }
                    if (canSendReady) {
                        synchronized (llc_lock) {
                            llc_value++;
                            System.out.println();
                            System.out.printf("<%d> sending ready to coordinator%n", llc_value);
                            System.out.println();
                            out.println(llc_value + ",ready");
                        }
                        canSendReady = false;
                    }
                    if (parsedReceivedLine[1].equals("compute")) {
                        synchronized (lock) {
                            synchronized (llc_lock) {
                                int senderProcTS = Integer.parseInt(parsedReceivedLine[0]);
                                int maxOfTS = Math.max(senderProcTS, llc_value);
                                llc_value = maxOfTS + 1;
                                System.out.println();
                                System.out.printf("<%d> received compute from coordinator%n", llc_value);
                                System.out.println();
                            }
                            canSendCSRequestAndTokens = true;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        initializingThread.start();
        return initializingThread;
    }


    private static Thread startClientThreadMain() {
        Thread clientThreadMain = new Thread(() -> {
            while (true) {
                if (canSendHello)
                    break;
                try {
                    Thread.sleep(900);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            Thread skProcessingThread = new Thread(()->{
                while (true) {
                    if (canSendCSRequestAndTokens) {

                        if(!hasToken){
                            latch = new CountDownLatch(localNeighbourSet.size());
                            llc_value++;
                            requestNumber++;
                            if(isCoordinator){
                                forWaitTimeCalculation.put(PROCESSID,System.currentTimeMillis());
                                if(requestNumber==10){
                                    //print stats
                                    System.out.printf("PROCESS ID  |   AVERAGE WAIT   |   ROUND WISE WAITS%n");
                                    for(Map.Entry<Integer,List<Long>> entry : forWaitTimeCalculationFinal.entrySet()){
                                        System.out.printf("    %d              %s           %s%n"
                                                ,entry.getKey()
                                                ,Stream.of(entry.getValue().toArray(new Long[entry.getValue().size()])).mapToLong(i->i).average().getAsDouble()
                                                ,Arrays.toString(entry.getValue().toArray()));
                                    }
                                    System.out.printf("PROCESS ID  |   AVERAGE SYNC DELAY   |   ROUND WISE SYNC DELAY%n");
                                    for(Map.Entry<Integer,List<Long>> entry : forSyncDelayCalculation.entrySet()){
                                        System.out.printf("    %d              %s           %s%n"
                                                ,entry.getKey()
                                                ,Stream.of(entry.getValue().toArray(new Long[entry.getValue().size()])).mapToLong(i->i).average().getAsDouble()
                                                ,Arrays.toString(entry.getValue().toArray()));
                                    }
                                    terminate=true;
                                    break;
                                }
                            }
                            skValveSendReq.add(1);
                            try {
                                latch.await();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            skValveSendReq.remove();
                            canSendCSRequestAndTokens=false;
                            System.out.printf("<%d> done sending requests for now %n",llc_value);
                        }

                        if(hasToken){
                            latch = new CountDownLatch(localNeighbourSet.size());
                            if (!skBlockingQueue.isEmpty()) {
                                skBlockingQueue.remove();
                                try {
                                    llc_value++;
                                    System.out.printf("<%d> executing CS...%n",llc_value);
                                    Thread.sleep(3000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                //post CS execution work
                                for (int i = 0; i < tokenArray.length; i++) {
                                    if (tokenArray[i] + 1 == recvArray[i]) {
                                        if(!queueOfProcsVchWillReceiveTheTokenNext.contains(Integer.valueOf(i+1))){
                                            queueOfProcsVchWillReceiveTheTokenNext.add(Integer.valueOf(i+1));
                                            System.out.println("queue does not has the value........................so add it");
                                        }

                                    }
                                }
                                if(queueOfProcsVchWillReceiveTheTokenNext.size()==0){
                                    skBlockingQueue.add(1);
                                } else{
                                    llc_value++;
                                    //sendArray[n.getId() - 1]++;
                                    tokenBuilder = new StringBuilder();
                                    Long time = System.currentTimeMillis();
                                    tokenBuilder.append(llc_value);tokenBuilder.append(",");
                                    tokenBuilder.append("token");tokenBuilder.append(",");
                                    tokenBuilder.append(PROCESSID);tokenBuilder.append(",");
                                    tokenBuilder.append(queueOfProcsVchWillReceiveTheTokenNext.peek());tokenBuilder.append(",");
                                    tokenBuilder.append(Arrays.toString(tokenArray).replace(",",":").replace(" ",""));tokenBuilder.append(",");
                                    tokenBuilder.append(queueOfProcsVchWillReceiveTheTokenNext.toString().replace(",",":").replace(" ",""));tokenBuilder.append(",");
                                    tokenBuilder.append(time);


                                    if(isCoordinator){
                                        Long waitTime = System.currentTimeMillis() - forWaitTimeCalculation.get(queueOfProcsVchWillReceiveTheTokenNext.peek());
                                        Long syncDelay = Math.abs(System.currentTimeMillis() - time + 10);

                                        if(!forWaitTimeCalculationFinal.containsKey(queueOfProcsVchWillReceiveTheTokenNext.peek())){
                                            List<Long> waitTimeHolder = new ArrayList<>();
                                            waitTimeHolder.add(waitTime);
                                            forWaitTimeCalculationFinal.put(queueOfProcsVchWillReceiveTheTokenNext.peek(),waitTimeHolder);
                                        } else {
                                            List<Long> waitTimeHolder = forWaitTimeCalculationFinal.get(queueOfProcsVchWillReceiveTheTokenNext.peek());
                                            waitTimeHolder.add(waitTime);
                                            forWaitTimeCalculationFinal.put(queueOfProcsVchWillReceiveTheTokenNext.peek(),waitTimeHolder);
                                        }

                                        if(!forSyncDelayCalculation.containsKey(queueOfProcsVchWillReceiveTheTokenNext.peek())){
                                            List<Long> syncDelayHolder = new ArrayList<>();
                                            syncDelayHolder.add(syncDelay);
                                            forSyncDelayCalculation.put(queueOfProcsVchWillReceiveTheTokenNext.peek(),syncDelayHolder);
                                        } else {
                                            List<Long> syncDelayHolder = forSyncDelayCalculation.get(queueOfProcsVchWillReceiveTheTokenNext.peek());
                                            syncDelayHolder.add(syncDelay);
                                            forSyncDelayCalculation.put(queueOfProcsVchWillReceiveTheTokenNext.peek(),syncDelayHolder);
                                        }
                                    }
                                    skValveSendToken.add(1);
                                    try {
                                        latch.await();
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    skValveSendToken.remove();

                                    hasToken = false;
                                    skBlockingQueue.add(1);
                                    //limit the request flow
                                    //canSendCSRequestAndTokens = false;
                                }
                            }
                        }

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                }
            });
            skProcessingThread.start();

            //for each neighbour, create new thread and establish new socket
            for (Neighbour n : localNeighbourSet) {
                Thread clientThreadAncillary = new Thread(() -> {
                    String line;
                    try {
                        Socket client = new Socket(n.getHostname(), PORT1);
                        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
                        synchronized (llc_lock) {
                            llc_value++;
                            sendArray[n.getId() - 1]++;
                            out.println(llc_value + ",hello," + PROCESSID);
                            System.out.printf("<%d> sending hello to PID %d%n", llc_value, n.getId());
                        }

                        while (true){
                            if(!skValveSendReq.isEmpty()){
                                System.out.printf("<%d> SEND : REQ >> PID#%d%n",llc_value,n.getId());
                                out.println(llc_value + ",request,"+PROCESSID+","+requestNumber);
                                latch.countDown();
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            if(!skValveSendToken.isEmpty()){
                                out.println(tokenBuilder.toString());
                                System.out.printf("<%d> SEND : TOKN >> %s ID %s%n", llc_value,tokenBuilder.toString(),n.getId());
                                latch.countDown();
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            if(terminate){
                                out.println(llc_value + ",terminate," + PROCESSID);
                                System.out.println("**************terminating processes");
                                break;
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                clientThreadAncillary.start();
            }
        });
        clientThreadMain.start();
        return clientThreadMain;
    }


    private static Thread startServerThreadMain() {
        Thread serverThreadMain = new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(PORT1);
                while (true) {
                    Socket client = serverSocket.accept();
                    Thread serverThreadAncillary = new Thread(() -> {
                        try {
                            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                            String line;
                            while ((line = in.readLine()) != null) {
                                String[] parsedLine = line.split(",");

                                if (parsedLine[1].equalsIgnoreCase("hello")) {
                                    synchronized (llc_lock) {
                                        int senderProcTS = Integer.parseInt(parsedLine[0]);
                                        int maxOfTS = Math.max(senderProcTS, llc_value);
                                        llc_value = maxOfTS + 1;
                                        System.out.printf("<%d> received hello from PID %d%n", llc_value, Integer.parseInt(parsedLine[parsedLine.length - 1]));
                                    }
                                    synchronized (lock) {
                                        neighbourCounter++;
                                    }
                                    if (neighbourCounter == numberOfNeighbours)
                                        blockingQueue.add(1);
                                }

                                if (parsedLine[1].equalsIgnoreCase("token")) {
                                    if(isCoordinator){
                                        Long waitTime = System.currentTimeMillis() - forWaitTimeCalculation.get(Integer.parseInt(parsedLine[3]));
                                        Long syncDelay = Math.abs(System.currentTimeMillis() - Long.parseLong(parsedLine[6]));

                                        if(!forWaitTimeCalculationFinal.containsKey(Integer.parseInt(parsedLine[3]))){
                                            List<Long> waitTimeHolder = new ArrayList<>();
                                            waitTimeHolder.add(waitTime);
                                            forWaitTimeCalculationFinal.put(Integer.parseInt(parsedLine[3]),waitTimeHolder);
                                        } else {
                                            List<Long> waitTimeHolder = forWaitTimeCalculationFinal.get(Integer.parseInt(parsedLine[3]));
                                            waitTimeHolder.add(waitTime);
                                            forWaitTimeCalculationFinal.put(Integer.parseInt(parsedLine[3]),waitTimeHolder);
                                        }

                                        if(!forSyncDelayCalculation.containsKey(Integer.parseInt(parsedLine[3]))){
                                            List<Long> syncDelayHolder = new ArrayList<>();
                                            syncDelayHolder.add(syncDelay);
                                            forSyncDelayCalculation.put(Integer.parseInt(parsedLine[3]),syncDelayHolder);
                                        } else {
                                            List<Long> syncDelayHolder = forSyncDelayCalculation.get(Integer.parseInt(parsedLine[3]));
                                            syncDelayHolder.add(syncDelay);
                                            forSyncDelayCalculation.put(Integer.parseInt(parsedLine[3]),syncDelayHolder);
                                        }
                                    }
                                    if(Integer.parseInt(parsedLine[3])==PROCESSID){

                                        synchronized (llc_lock) {
                                            int senderProcTS = Integer.parseInt(parsedLine[0]);
                                            int maxOfTS = Math.max(senderProcTS, llc_value);
                                            llc_value = maxOfTS + 1;
                                            System.out.printf("<%d> RECV : TOKN << %s%n", llc_value, line);
                                        }

                                        String[] tempParsedToken = parsedLine[4].replace("[", "").replace("]", "").split(":");
                                        for (int i = 0; i < tempParsedToken.length; i++) {
                                            tokenArray[i] = Integer.parseInt(tempParsedToken[i]);
                                        }


                                        queueOfProcsVchWillReceiveTheTokenNext = new LinkedList<>();
                                        tempParsedToken = parsedLine[5].replace("[", "").replace("]", "").split(":");
                                        for (int i = 0; i < tempParsedToken.length; i++) {
                                            queueOfProcsVchWillReceiveTheTokenNext.add(Integer.parseInt(tempParsedToken[i]));
                                        }

                                        System.out.println("queue before removal "+queueOfProcsVchWillReceiveTheTokenNext.toString());
                                        queueOfProcsVchWillReceiveTheTokenNext.remove();
                                        System.out.println("queue after removal "+queueOfProcsVchWillReceiveTheTokenNext.toString());
                                        tokenArray[PROCESSID - 1] = requestNumber;
                                        hasToken = true;
                                        canSendCSRequestAndTokens = true;

                                    } else {
                                        /*synchronized (llc_lock) {
                                            int senderProcTS = Integer.parseInt(parsedLine[0]);
                                            int maxOfTS = Math.max(senderProcTS, llc_value);
                                            llc_value = maxOfTS + 1;
                                            System.out.printf("<%d> RECV : WRONG TOKN << %s%n", llc_value, line);
                                        }*/
                                    }
                                }

                                if (parsedLine[1].equalsIgnoreCase("request")) {
                                    synchronized (sk_lock2) {
                                        int senderProcTS = Integer.parseInt(parsedLine[0]);
                                        int maxOfTS = Math.max(senderProcTS, llc_value);
                                        llc_value = maxOfTS + 1;
                                        if(recvArray[Integer.parseInt(parsedLine[2]) - 1]<Integer.parseInt(parsedLine[3]))
                                            recvArray[Integer.parseInt(parsedLine[2]) - 1] = Integer.parseInt(parsedLine[3]);
                                        System.out.printf("<%d> received cs request#%d from PID %d%n", llc_value, Integer.parseInt(parsedLine[3]),Integer.parseInt(parsedLine[2]));

                                        /*System.out.println("RECV " + Arrays.toString(recvArray));
                                        System.out.println("TOKN " + Arrays.toString(tokenArray));
                                        System.out.println("QUEU " + queueOfProcsVchWillReceiveTheTokenNext.toString());*/
                                        if(isCoordinator){
                                            System.out.println("=================================coordinator putting request for " + Integer.parseInt(parsedLine[2]));
                                            forWaitTimeCalculation.put(Integer.parseInt(parsedLine[2]),System.currentTimeMillis());
                                        }
                                    }
                                }

                                if(parsedLine[1].equalsIgnoreCase("terminate")){
                                    //print stats and exit
                                    System.out.println("____________PROCESS TERMINATED____________");
                                    System.exit(0);
                                }
                            }
                            out.close();
                            in.close();
                            client.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    serverThreadAncillary.start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        serverThreadMain.start();
        return serverThreadMain;
    }


    private static void runConfiguration(String fileLocation) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileLocation));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parsedLines = line.split(" ");
                if (parsedLines[0].equalsIgnoreCase("COORDINATOR"))
                    continue;
                if (parsedLines[0].equalsIgnoreCase("NUMBER")) {
                    NUMBER_OF_PROCS = Integer.parseInt(parsedLines[3]);
                }
                if (parsedLines[0].equalsIgnoreCase("INTERVAL"))
                    continue;
                if (parsedLines[0].equalsIgnoreCase("TERMINATE"))
                    continue;
                if (parsedLines[0].equalsIgnoreCase("NEIGHBOR")) {
                    while ((line = reader.readLine()) != null) {
                        if (line.equals("")) {
                            break;
                        }
                        String[] arrayOfProcesses = line.split(" ");
                        Set<Integer> setOfNeighbourProcesses = new HashSet<>();
                        for (int i = 1; i < arrayOfProcesses.length; i++) {
                            setOfNeighbourProcesses.add(Integer.parseInt(arrayOfProcesses[i]));
                        }
                        neighbours.put(Integer.parseInt(arrayOfProcesses[0]), setOfNeighbourProcesses);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void executeCS() throws InterruptedException {
        System.out.println("acquiring CS...");
        Thread.sleep(10000);
    }
}
