import java.io.*;
import java.net.InetSocketAddress;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Raymond {
    /* Charset */
    static final Charset charset = StandardCharsets.UTF_8;

    /* PORT NUMS */
    private static final int PORT1 = 51010;
    private static final int COPORT = 51015;
    private static ServerSocketChannel nodeServer;

    /* Lamport clock */
    static volatile int clock = 0;
    static final Object llc_lock = new Object();

    //COORDINATOR PROCESS PARAMS
    static int NUMBER_OF_PROCS;
    static final Object coordinatorLock = new Object();
    static final String CONFIG = "dsConfig";
    static Map<Integer, String> pidToHostnameMap = new HashMap<>();
    static int PROCESSID;
    static int TERMINATE;
    static boolean isCoordinator = false;
    static boolean coorServerStart = false;
    static int TIME1;
    static int TIME2;
    static int CSTIME;

    // NON COORDINATOR PROCESS PARAMS
    static Map<Integer, String> localNeighbourMap = new HashMap<>();
    static Map<Integer, SocketChannel> neighbourSockets = new HashMap<>();
    private static volatile boolean terminated = false;


    /* RAYMOND RELATED */
    private static Queue<Integer> csQueue = new LinkedList<>();
    private static AtomicBoolean haveToken = new AtomicBoolean(false);
    private static final Lock csLock = new ReentrantLock();
    private static final Condition csEnter = csLock.newCondition();
    private static boolean csRunning = false;
    private static int holderPID;
    private static int csSeq = 0;


    /* STATS RELATED */
    static volatile Map<Integer,Long> forWaitTimeCalculation = Collections.synchronizedMap(new HashMap<>());
    static volatile Map<Integer,List<Long>> forWaitTimeCalculationFinal = Collections.synchronizedMap(new HashMap<>());
    static volatile Map<Integer,List<Long>> forSyncDelayCalculation = Collections.synchronizedMap(new HashMap<>());


    public static void main(String[] args) throws InterruptedException, IOException{
        Thread coorThread = null;
        Thread csBackground = null;

        try {
            if (args.length != 0 && args[0].equalsIgnoreCase("-c")) {
                isCoordinator = true;
                // Start CoOrdinate thread
                coorThread = new Thread(Raymond::startCoordinate);
                coorThread.start();
            }

            // Start initializing
            startInitializing();

            // Run the compute simulation
            csBackground = new Thread(Raymond::runBackground);
            csBackground.start();
            runCompute();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (coorThread != null) {
                coorThread.join();
            }
            if (csBackground != null) {
                csBackground.join();
            }
        }
    }

    private static void startCoordinate() {
        //Running the coordinator part of the process
        runConfiguration(CONFIG);

        Map<Integer, SocketChannel> clients = new HashMap<>();

        System.out.println("<i> ** Coordinator process initiated ** </i>");
        System.out.println("<i> ** Waiting for processes to register ** </i>");

        try {
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
            // Binary Tree network
            for (Map.Entry<Integer, SocketChannel> entry : clients.entrySet()) {
                int pid = entry.getKey();
                StringBuilder stringBuilder = new StringBuilder();

                stringBuilder.append(pid);

                List<Integer> neighbors = getTreeNeighbours(pid, NUMBER_OF_PROCS);
                int pPID = (pid == 1) ? 1 : neighbors.get(0); // parentProcess

                stringBuilder.append(',');
                stringBuilder.append(pPID);

                for (Integer npid : neighbors) {
                    String nhostname = pidToHostnameMap.get(npid);
                    stringBuilder.append(',');
                    stringBuilder.append(npid);
                    stringBuilder.append(' ');
                    stringBuilder.append(nhostname);
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
                    System.out.println("<i> Receive READY " + pid + " </i>");
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

    private static List<Integer> getTreeNeighbours(int pid, int numProcs) {
        ArrayList<Integer> neighs = new ArrayList<>(3);

        int parent = pid / 2;
        if (parent > 0) {
            neighs.add(parent);
        }

        int lchild = pid * 2;
        if (lchild <= NUMBER_OF_PROCS) {
            neighs.add(lchild);
        }
        else {
            return neighs;
        }

        int rchild = lchild + 1;
        if (rchild <= NUMBER_OF_PROCS) {
            neighs.add(rchild);
        }

        return neighs;
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
        SocketAddress nodeAddr = new InetSocketAddress(PORT1);
        nodeServer = ServerSocketChannel.open();
        nodeServer.bind(nodeAddr);

        // Create connection to Coordinator
        BufferedReader reader = new BufferedReader(new FileReader(CONFIG));
        String[] params = reader.readLine().split(" ");

        SocketAddress coorAddr = new InetSocketAddress(params[1], COPORT);
        SocketChannel coor = SocketChannel.open(coorAddr);

        // PrintWriter out = new PrintWriter(client.getOutputStream(), true);
        // BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
        coor.write(ByteBuffer.wrap("REGISTER".getBytes(charset)));

        System.out.println("  Registration initiated");

        String[] totalNumberOfProcs = reader.readLine().split(" ");
        int arraySize = Integer.parseInt(totalNumberOfProcs[3]);
        //sendArray = new int[arraySize];
        //recvArray = new int[arraySize];

        // Read neighbour info from CoOrdinator
        ByteBuffer readbuf = ByteBuffer.allocate(25 * 128);
        coor.read(readbuf);

        String line = new String(readbuf.array(), charset).trim();
        String[] parsedReceivedLine = line.split(",");
        System.out.println("  Registration success");

        PROCESSID = Integer.parseInt(parsedReceivedLine[0]);
        holderPID = Integer.parseInt(parsedReceivedLine[1]);

        if (PROCESSID == 1) {
            haveToken.set(true);
        }

        System.out.printf("    PID: %d\n", PROCESSID);
        System.out.printf("    Neighbours----------PID:\n");
        for (int i = 2; i < parsedReceivedLine.length; i++) {
            String[] detailsOfNeighbour = parsedReceivedLine[i].split(" ");
            int npid = Integer.parseInt(detailsOfNeighbour[0]);
            String nhostname = detailsOfNeighbour[1];

            //localNeighbourSet.add(new Neighbour(detailsOfNeighbour[0], Integer.parseInt(detailsOfNeighbour[1])));
            localNeighbourMap.put(npid, nhostname);
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
                    neighbourSockets.put(pid, connectTo);

                    connectTo.write(ByteBuffer.wrap(("HELLO " + PROCESSID).getBytes(charset)));
                }
            }
            catch (IOException ex) {
                ex.printStackTrace();
            }
        });
        sendThread.start();

        // Receive HELLO from processes with smaller PID
        try {
            for (int pid : localNeighbourMap.keySet()) {
                if (pid >= PROCESSID) {
                    continue;
                }

                SocketChannel connectFrom = nodeServer.accept();

                ByteBuffer readbuf = ByteBuffer.allocate(32);
                connectFrom.read(readbuf);

                String[] recv = (new String(readbuf.array(), charset).trim()).split(" ");
                int frmpid = Integer.parseInt(recv[1]);

                System.out.println("  Received " + recv[0] + " from " + recv[1]);

                // Add to neighbourSockets
                neighbourSockets.put(frmpid, connectFrom);
            }

            sendThread.join();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }

        return true;
    }

    private static void runCompute() {
        System.out.println(">> PID " + PROCESSID + " start COMPUTE <<");
        System.out.println("--------------------------");

        Random waitTimeRand = new Random();
        Random intervalRand = new Random();
        int waitTimeRange = TIME2 - TIME1 + 1;
        int num_repeat = 20 + (new Random()).nextInt(21);

        for (int i = 0; i < num_repeat; ++i) {
            // Sleep before requesting CS
            try {
                Thread.sleep(TIME1 + waitTimeRand.nextInt(waitTimeRange));
            }
            catch (InterruptedException e) {
                System.err.println("First sleep interrupted");
            }

            // Execute the CS
            executeCS(CSTIME);

            // Sleep after finishing CS
            try {
                Thread.sleep(20 + intervalRand.nextInt(21));
            }
            catch (InterruptedException e) {
                System.err.println("Second sleep interrupted");
            }
        }

        terminated = true;
    }

    private static void runBackground() {
        try {
            Selector selector = Selector.open();

            for (SocketChannel socket : neighbourSockets.values()) {
                socket.configureBlocking(false);
                socket.register(selector, SelectionKey.OP_READ);
            }

            while (!terminated) {
                selector.select(500);
                Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();

                while (keyIter.hasNext()) {
                    SelectionKey key = keyIter.next();
                    keyIter.remove();

                    if (key.isReadable()) {
                        SocketChannel socket = (SocketChannel) key.channel();
                        ByteBuffer buf = ByteBuffer.allocate(64);

                        if (socket.read(buf) < 0) {
                            // Client connection closed from client side
                            key.cancel();
                            socket.close();
                            continue;
                        };

                        String[] msg = new String(buf.array(), charset).trim().split(" ");

                        int i = 0;
                        while (i < msg.length) {
                            int msg_clock = Integer.parseInt(msg[i]);
                            synchronized (llc_lock) {
                                clock = Integer.max(clock + 1, msg_clock + 1);
                                System.out.println(clock + "': received " + msg[i+1]);
                            }
                            i++;
                            // On receiving REQUEST
                            if (msg[i].equals("REQUEST")) {
                                int asked_pid = Integer.parseInt(msg[i+1]);
                                csLock.lock();
                                try {
                                    if (holderPID == PROCESSID) {
                                        if (!csRunning && csQueue.isEmpty()) {
                                            sendToken(asked_pid);
                                        }
                                        else {
                                            csQueue.add(asked_pid);
                                        }
                                    }
                                    else {
                                        if (csQueue.isEmpty()) {
                                            csQueue.add(asked_pid);
                                            sendCSRequest();
                                        }
                                        else {
                                            csQueue.add(asked_pid);
                                        }
                                    }
                                }
                                finally {
                                    csLock.unlock();
                                }
                                i += 2;
                            }
                            // On receiving TOKEN
                            else if (msg[i].equals("TOKEN")) {
                                csLock.lock();
                                try {
                                    int req_pid = csQueue.remove();

                                    if (req_pid == PROCESSID) {
                                        holderPID = PROCESSID;
                                        csEnter.signal();
                                    }
                                    else {
                                        sendToken(req_pid);
                                        if (!csQueue.isEmpty()) {
                                            sendCSRequest();
                                        }
                                    }
                                }
                                finally {
                                    csLock.unlock();
                                }
                                i++;
                            }
                        }
                    }
                }

            }
            selector.close();
        }
        catch (IOException e) {
            System.err.println("IO Exception");
            e.printStackTrace();
        }
    }

    private static void sendCSRequest() {
        String msg;
        synchronized (llc_lock) {
            clock++;
            System.out.println(clock + "': Send REQUEST to " + holderPID);
            msg = clock + " REQUEST " + PROCESSID + " ";
        }

        SocketChannel tokenChannel = neighbourSockets.get(holderPID);

        try {
            tokenChannel.write(ByteBuffer.wrap(msg.getBytes(charset)));
        }
        catch (IOException e) {
            System.err.println("Failed to send CS request");
        }
    }

    private static void sendToken(int topid) {
        String msg;
        synchronized (llc_lock) {
            clock++;
            System.out.println(clock + "': Send TOKEN to " + topid);
            msg = clock + " TOKEN ";
        }

        SocketChannel toChannel = neighbourSockets.get(topid);

        try {
            toChannel.write(ByteBuffer.wrap(msg.getBytes(charset)));
            holderPID = topid;
        }
        catch (IOException e) {
            System.err.println("Failed to send TOKEN");
        }
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
                if (parsedLines[0].equals("TERMINATE")) {
                    TERMINATE = Integer.parseInt(parsedLines[1]);
                }
                if (parsedLines[0].equalsIgnoreCase("WAITTIME")) {
                    TIME1 = Integer.parseInt(parsedLines[1]);
                    TIME2 = Integer.parseInt(parsedLines[2]);
                }
                if (parsedLines[0].equalsIgnoreCase("CSTIME")){
                    CSTIME = Integer.parseInt(parsedLines[1]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void executeCS(int cs_time) {
        csSeq++;
        System.out.println(clock + "': request CS " + csSeq);

        csLock.lock();
        try {
            if (holderPID != PROCESSID || !csQueue.isEmpty()) {
                if (csQueue.isEmpty()) {
                    sendCSRequest();
                }
                csQueue.add(PROCESSID);

                csEnter.await();
            }
            csRunning = true;
        }
        catch (InterruptedException e){
            System.err.println("Interrupted while waiting for CS");
        }
        finally {
            csLock.unlock();
        }

        // Enter CS
        synchronized (llc_lock) {
            clock++;
            System.out.println(clock + "': enter CS " + csSeq);
        }
        try {
            Thread.sleep(cs_time);
        }
        catch (InterruptedException e) {
            System.err.println(e);
            e.printStackTrace();
        }

        // Leaving CS
        System.out.println(clock + "': leave CS " + csSeq);
        csLock.lock();
        try {
            csRunning = false;
            if (!csQueue.isEmpty())  {
                int req_pid = csQueue.remove();
                sendToken(req_pid);

                if (!csQueue.isEmpty()) {
                    sendCSRequest();
                }
            }
        }
        finally {
            csLock.unlock();
        }
    }
}
