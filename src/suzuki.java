import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

public class deploy {

    //PORT NUMS
    static final int PORT1 = 5010;
    static final int PORT2 = 5011;


    //CLOCK
    static volatile int llc_value = 0;
    static Object llc_lock = new Object();

    //COORDINATOR PROCESS PARAMS
    static int NUMBER_OF_PROCS;
    static volatile int numberOfProcessRegistered = 0;
    static volatile int numberOfProcessReady = 0;
    static Object coordinatorLock = new Object();
    static final String CONFIG = "/home/013/d/dx/dxc141530/dsConfig";
    static Map<Integer, Set<Integer>> neighbours = new HashMap<>();
    static Map<Integer, Neighbour> pidToHostnameMap = new HashMap<>();
    static int PROCESSID;
    static boolean isCoordinator = false;

    //NON COORDINATOR PROCESS PARAMS
    static volatile boolean canSendHello = false;
    static volatile boolean canSendReady = true;
    static volatile boolean canSendCSRequestAndTokens = false;
    static BlockingQueue<Integer> blockingQueue = new ArrayBlockingQueue<>(1);
    static volatile int numberOfNeighbours = 0;
    static volatile int neighbourCounter = 0;
    static Object lock = new Object();
    static Set<Neighbour> localNeighbourSet = new HashSet<>();
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

    //SUZUKI KASAMI TOKEN RELATED
    static volatile boolean hasToken = false;
    static volatile int[] tokenArray;
    static volatile Queue<Integer> queueOfProcsVchWillReceiveTheTokenNext;

    //SUZUKI KASAMI STATS RELATED
    static volatile Map<Integer,Long> forWaitTimeCalculation = Collections.synchronizedMap(new HashMap<>());
    static volatile Map<Integer,List<Long>> forWaitTimeCalculationFinal = Collections.synchronizedMap(new HashMap<>());
    static volatile Map<Integer,List<Long>> forSyncDelayCalculation = Collections.synchronizedMap(new HashMap<>());


    public static void main(String[] args) {

        if (args.length != 0) {
            if (args[0].equalsIgnoreCase("-c")) {
                //Running the coordinator part of the process
                runConfiguration(CONFIG);
                hasToken = true;
                isCoordinator = true;

                Thread coordinatorThread = new Thread(() -> {
                    System.out.println();
                    System.out.println("<i> ********Coordinator process initiated");
                    System.out.println("<i> ********Waiting for processes to register...");
                    System.out.println();
                    try {
                        ServerSocket serverSocket = new ServerSocket(PORT2);
                        while (true) {
                            Socket client = serverSocket.accept();
                            Thread handlerThread = new Thread(() -> {
                                Thread clThread = new Thread(() -> {
                                    //keep a track of llc_value. initiate CLA every 100 llc_value
                                });
                                clThread.start();

                                try {
                                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                                    String line;
                                    int pid;
                                    while ((line = in.readLine()) != null) {
                                        String[] lineRecvdByCoord = line.split(",");
                                        if (lineRecvdByCoord[1].equalsIgnoreCase("register")) {
                                            synchronized (coordinatorLock) {
                                                numberOfProcessRegistered++;
                                                pid = numberOfProcessRegistered;
                                                pidToHostnameMap.put(pid,
                                                        new Neighbour(String.valueOf(client.getInetAddress().getHostName()), pid));
                                                synchronized (llc_lock) {
                                                    int senderProcTS = Integer.parseInt(lineRecvdByCoord[0]);
                                                    int maxOfTS = Math.max(senderProcTS, llc_value);
                                                    llc_value = maxOfTS + 1;
                                                    System.out.printf("<%d> ********register frm : %s , # of procs registered : %d%n", llc_value, client.getInetAddress().getHostName(), numberOfProcessRegistered);
                                                }
                                            }
                                            while (true) {
                                                if (numberOfProcessRegistered == NUMBER_OF_PROCS) {
                                                    break;
                                                }
                                            }
                                            System.out.println();
                                            Thread.sleep(200);
                                            //modify below code to apprise the clients about their host id and neighbours
                                            StringBuilder stringBuilder = new StringBuilder();
                                            stringBuilder.append(pid);
                                            Set<Integer> tempSet = neighbours.get(pid);
                                            for (Integer i : tempSet) {
                                                stringBuilder.append(",");
                                                Neighbour neighbour = pidToHostnameMap.get(i);
                                                stringBuilder.append(neighbour.getHostname() + " " + neighbour.getId());
                                            }
                                            synchronized (llc_lock) {
                                                llc_value++;
                                                out.println(llc_value + ",registered," + stringBuilder.toString());
                                                System.out.printf("<%d> ********sending registered to %s%n", llc_value, client.getInetAddress().getHostName());
                                            }
                                        }
                                        if (lineRecvdByCoord[1].equalsIgnoreCase("ready")) {
                                            synchronized (coordinatorLock) {
                                                numberOfProcessReady++;
                                                synchronized (llc_lock) {
                                                    int senderProcTS = Integer.parseInt(lineRecvdByCoord[0]);
                                                    int maxOfTS = Math.max(senderProcTS, llc_value);
                                                    llc_value = maxOfTS + 1;
                                                    System.out.printf("<%d> ********ready frm : %s , # of procs ready : %d%n", llc_value, client.getInetAddress().getHostName(), numberOfProcessReady);
                                                }
                                            }
                                            while (true) {
                                                if (numberOfProcessReady == NUMBER_OF_PROCS) {
                                                    break;
                                                }
                                            }
                                            System.out.println();
                                            Thread.sleep(200);
                                            //modify below code to apprise the clients about their host id and neighbours
                                            synchronized (llc_lock) {
                                                llc_value++;
                                                System.out.printf("<%d> ********sending compute to %s%n", llc_value, client.getInetAddress().getHostName());
                                                out.println(llc_value + ",compute");
                                            }
                                        }
                                    }
                                    out.close();
                                    in.close();
                                    client.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            });
                            handlerThread.start();
                            //handlerThread.join();
                        }
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                });
                coordinatorThread.start();
            }
        }

        //Running the non coordinator part of the process
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
                Socket client = new Socket(params[1], PORT2);
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
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

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

        initializingThread.start();
        clientThreadMain.start();
        serverThreadMain.start();


        try {
            initializingThread.join();
            clientThreadMain.join();
            serverThreadMain.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
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

/*
class Neighbour {
    private int id;
    private String hostname;
    private String portnum;

    public Neighbour(){}

    public Neighbour(String hostname,int id){
        this.hostname=hostname;
        this.id=id;
    }

    public Neighbour(String hostname){
        this.hostname=hostname;
    }
    public Neighbour(String hostname, String portnum) {
        this.hostname = hostname;
        this.portnum = portnum;
    }

    public Neighbour(int id, String hostname, String portnum) {
        this.id = id;
        this.hostname = hostname;
        this.portnum = portnum;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPortnum() {
        return portnum;
    }

    public void setPortnum(String portnum) {
        this.portnum = portnum;
    }

    public int getId() {
        return id;
    }
}
*/
