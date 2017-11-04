
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

    /* COORDINATOR PROCESS PARAMS */
    static int NUMBER_OF_PROCS;
    static final Object coordinatorLock = new Object();
    static final String CONFIG = "dsConfig";
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
    private static SocketChannel coorSocket;


    /* RAYMOND RELATED */
    private static Queue<Integer> csQueue = new LinkedList<>();
    private static final Lock csLock = new ReentrantLock();
    private static final Condition csEnter = csLock.newCondition();
    private static boolean csRunning = false;
    private static int holderPID;
    private static int csSeq = 0;


    /* STATS RELATED */
    private static int num_mess_sent = 0;
    private static int total_wait_time = 0;
    private static int max_sync_delay = -1;
    private static int total_sync_delay = 0;
    private static int max_sync_delay_time = -1;
    private static int total_sync_delay_time = 0;

    public static void main(String[] args) throws InterruptedException, IOException{
        Thread coorThread = null;
        Thread csBackground = null;

        try {
            if (args.length != 0 && args[0].equals("-c")) {
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

            // finish running
            completeRunning();
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
        Map<Integer, String> pidToHostnameMap = new HashMap<>();

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
            }

            // Wait for all to say complete
            i = 0;
            while (i < clients.size()) {
                int pid = i + 1;
                ByteBuffer readbuf = ByteBuffer.allocate(64);
                clients.get(pid).read(readbuf);

                String[] rcv = new String(readbuf.array(), charset).trim().split(" ");

                if (rcv[0].equals("COMPLETE")) {
                    System.out.println("<i> Receive COMPLETE from " + pid + " </i>");
                    i++;
                }
            }

            // Send ready to collect stat signal
            System.out.println("Ready to collect stats");
            for (SocketChannel client : clients.values()) {
                client.write(ByteBuffer.wrap("STAT".getBytes(charset)));
            }

            // Wait for all to send stats
            int total_mess_sent = 0;
            int total_cs = 0;
            int all_wait_time = 0;
            int all_sync_delay = 0;
            int max_all_sync_delay = 0;
            int all_sync_delay_time = 0;
            int max_sync_delay_time = 0;
            i = 0;
            while (i < clients.size()) {
                int pid = i + 1;
                ByteBuffer readbuf = ByteBuffer.allocate(64);
                clients.get(pid).read(readbuf);

                String[] rcv = new String(readbuf.array(), charset).trim().split(" ");

                if (rcv[0].equals("STAT")) {
                    i++;
                    total_cs += Integer.parseInt(rcv[1]);
                    total_mess_sent += Integer.parseInt(rcv[2]);
                    all_wait_time += Integer.parseInt(rcv[3]);

                    int m_sync_delay = Integer.parseInt(rcv[4]);
                    if (m_sync_delay > max_all_sync_delay) {
                        max_all_sync_delay = m_sync_delay;
                    }
                    all_sync_delay += Integer.parseInt(rcv[5]);

                    int m_sync_delay_time = Integer.parseInt(rcv[6]);
                    if (m_sync_delay_time > max_sync_delay_time) {
                        max_sync_delay_time = m_sync_delay_time;
                    }
                    all_sync_delay_time += Integer.parseInt(rcv[7]);
                }
            }
            System.out.println("Total CS Request: " + total_cs);
            System.out.println("Total messages sent: " + total_mess_sent);
            System.out.println("Total wait time: " + all_wait_time +" ms");
            System.out.println("Total sync delay msg: " + all_sync_delay );
            System.out.println("Total sync delay time: " + all_sync_delay_time + " ms");
            System.out.format("Messages per CS: %.4f%n", (double) total_mess_sent/total_cs);
            System.out.format("Wait time per CS: %.4f ms%n", (double) all_wait_time/total_cs);
            System.out.println("Max sync delay: " + max_all_sync_delay);
            System.out.format("Sync delay per CS: %.4f%n", (double) all_sync_delay/total_cs);
            System.out.println("Max sync delay time: " + max_sync_delay_time + " ms");
            System.out.format("Sync delay time per CS: %.4f ms%n", (double) all_sync_delay_time/total_cs);

            // Send TERMINATED to all and end connection
            for (SocketChannel client : clients.values()) {
                ByteBuffer buf = ByteBuffer.wrap("TERMINATE".getBytes(charset));
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
        if (lchild <= numProcs) {
            neighs.add(lchild);
        }
        else {
            return neighs;
        }

        int rchild = lchild + 1;
        if (rchild <= numProcs) {
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
            holderPID = 1;
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

        // Finish Initializing
        coorSocket = coor;
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
    }

    private static void completeRunning() {
        try {
            // Send complete to coordinator
            System.out.println("Send COMPLETE");
            coorSocket.write(ByteBuffer.wrap("COMPLETE".getBytes(charset)));

            // Wait for coordinator to ready to collect stat
            ByteBuffer readbuf = ByteBuffer.allocate(32);
            coorSocket.read(readbuf);
            String msg = new String(readbuf.array(), charset).trim();
            if (msg.equals("STAT")) {
                System.out.println("Send Stats to coordinator");
            }

            // Send Stats to coordinator
            StringBuilder builder = new StringBuilder();
            builder.append("STAT ");
            builder.append(csSeq);
            builder.append(' ');
            builder.append(num_mess_sent);
            builder.append(' ');
            builder.append(total_wait_time);
            builder.append(' ');
            builder.append(max_sync_delay);
            builder.append(' ');
            builder.append(total_sync_delay);
            builder.append(' ');
            builder.append(max_sync_delay_time);
            builder.append(' ');
            builder.append(total_sync_delay_time);
            String complete_msg = builder.toString();
            coorSocket.write(ByteBuffer.wrap(complete_msg.getBytes(charset)));

            // Wait for terminate
            readbuf = ByteBuffer.allocate(32);
            coorSocket.read(readbuf);
            msg = (new String(readbuf.array(), charset)).trim();
            if (!msg.equals("TERMINATE")) {
                System.err.println("not terminate received");
            }
            else {
                System.out.println("Received TERMINATE signal");
            }

            // Finish our computation
            terminated = true;
            coorSocket.close();
            System.out.println("END");
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
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
                            long msg_clock = Long.parseLong(msg[i]);
                            i++;

                            // On receiving REQUEST
                            if (msg[i].equals("REQUEST")) {
                                log("received " + "REQUEST " + msg[i+1]);
                                int asked_pid = Integer.parseInt(msg[i+1]);
                                csLock.lock();
                                try {
                                    if (holderPID == PROCESSID) {
                                        if (!csRunning && csQueue.isEmpty()) {
                                            sendToken(asked_pid, 0, System.currentTimeMillis());
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
                                log("received " + msg[i]);
                                int prev_hop = Integer.parseInt(msg[i+1]);
                                long token_time = Long.parseLong(msg[i+2]);
                                int hop = prev_hop + 1;

                                csLock.lock();
                                try {
                                    int req_pid = csQueue.remove();

                                    if (req_pid == PROCESSID) {
                                        computeSyncDelay(hop, (int) (System.currentTimeMillis() - token_time));
                                        holderPID = PROCESSID;
                                        csEnter.signal();
                                    }
                                    else {
                                        sendToken(req_pid, hop, token_time);
                                        if (!csQueue.isEmpty()) {
                                            sendCSRequest();
                                        }
                                    }
                                }
                                finally {
                                    csLock.unlock();
                                }
                                i += 3;
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
        long clock = System.currentTimeMillis();
        log(clock, "Send REQUEST to " + holderPID);
        msg = clock + " REQUEST " + PROCESSID + " ";

        SocketChannel tokenChannel = neighbourSockets.get(holderPID);

        try {
            tokenChannel.write(ByteBuffer.wrap(msg.getBytes(charset)));
            num_mess_sent++;
        }
        catch (IOException e) {
            System.err.println("Failed to send CS request");
        }
    }

    private static void sendToken(int topid, int hop, long token_time) {
        String msg;
        long clock = System.currentTimeMillis();
        log(clock, "Send TOKEN to " + topid);
        msg = clock + " TOKEN " + hop + " " + token_time + " ";

        SocketChannel toChannel = neighbourSockets.get(topid);

        try {
            toChannel.write(ByteBuffer.wrap(msg.getBytes(charset)));
            num_mess_sent++;
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

    private static void computeSyncDelay(int sync_delay, int sync_delay_time) {
        if (sync_delay > max_sync_delay) {
            max_sync_delay = sync_delay;
        }
        total_sync_delay += sync_delay;

        if (sync_delay_time > max_sync_delay_time) {
            max_sync_delay_time = sync_delay_time;
        }
        total_sync_delay_time += sync_delay_time;
    }

    private static void executeCS(int cs_time) {
        csSeq++;
        long req_time = System.currentTimeMillis();
        log(req_time, "request CS " + csSeq);

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
        long enter_time = System.currentTimeMillis();
        total_wait_time += enter_time - req_time;
        log(enter_time, "enter CS " + csSeq);
        try {
            Thread.sleep(cs_time);
        }
        catch (InterruptedException e) {
            System.err.println(e);
            e.printStackTrace();
        }

        // Leaving CS
        log("leave CS " + csSeq);
        csLock.lock();
        try {
            csRunning = false;
            if (!csQueue.isEmpty())  {
                int req_pid = csQueue.remove();
                sendToken(req_pid, 0, System.currentTimeMillis());

                if (!csQueue.isEmpty()) {
                    sendCSRequest();
                }
            }
        }
        finally {
            csLock.unlock();
        }
    }

    private static void log(String message) {
        // print message with timestamp:
        log(System.currentTimeMillis(), message);
    }

    private static void log(long clock, String message) {
        int miliseconds = (int) (clock % 1000);
        int seconds = (int) ((clock / 1000) % 60) ;
        int minutes = (int) ((clock / (1000*60)) % 60);

        System.out.format("%02d:%02d.%03d: %s%n", minutes, seconds, miliseconds, message);
    }
}
