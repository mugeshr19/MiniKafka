import java.util.Base64;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;

import java.util.ArrayList;
import java.util.HashMap;

public class Broker{
    static long SESSION_TIMEOUT_MS = 10000;
    static long HEARTBEAT_CHECK_INTERVAL_MS = 3000;   
    static HashMap<String,ArrayList<ArrayList<String>>> topics = new HashMap<>();
    static HashMap<String,Integer> logStartOffsets = new HashMap<>();
    static HashMap<String,Integer> partitionCounters = new HashMap<>();
    static HashMap<String,Integer> topicPartitions = new HashMap<>();
    static HashMap<String,ArrayList<String>> topicISR = new HashMap<>();
    static HashMap<String,String> partitionLeaders = new HashMap<>();
    static HashMap<String,Integer> consumerOffsets = new HashMap<>();
    static int MAX_MESSAGES_PER_PARTITION = 10;
    static HashMap<String,BrokerNode> brokers = new HashMap<>();
    static String activeLeader = "broker1";
    static int minInSyncReplicas = 1;

    static ArrayList<ConsumerHandler> consumers = new ArrayList<>();
    static HashMap<String,ArrayList<ConsumerHandler>> consumerGroups = new HashMap<>();
    public static void main(String[] args){
        try{
            brokers.put(
                "broker1",
                new BrokerNode(
                    "broker1",
                    "leader"
                )
            );
            brokers.put(
                "broker2",
                new BrokerNode(
                    "broker2",
                    "replica"
                )
            );

            partitionLeaders.put("user-events:0","broker1");
            partitionLeaders.put("user-events:1","broker2");

            loadOffsets();
            loadTopicsFromLogs();
            for(String topic:topicPartitions.keySet()){
                int partitionCount = topicPartitions.get(topic);
                assignPartitionLeaders(topic,partitionCount);
            }
            recoverBroker("broker2");
            System.out.println("Topic ISR : " + topicISR);


            System.out.println(
                "Broker1 Data : "
                + brokers.get("broker1").messages
            );

            System.out.println(
                "Broker2 Data : "
                + brokers.get("broker2").messages
            );
            // brokers.get("broker2").role = "dead";

            System.out.println("Recovered Topics : " + brokers.get(activeLeader).messages);

            // new Thread(()->{
            //     try{
            //         Thread.sleep(10000);
            //         brokers.get("broker1").role = "dead";
            //         electNewleader();
            //         Thread.sleep(10000);
            //         System.out.println("\nBroker1 recovered!");
            //         recoverBroker("broker1");
            //     }
            //     catch(Exception e){
            //         e.printStackTrace();
            //     }
            // }).start();

            ServerSocket serverSocket = new ServerSocket(9092);
            System.out.println("Broker is running on port 9092");
            startHeartbeatMonitor();

            // new Thread(()->{
            //     try{
            //         Thread.sleep(10000);
            //         brokers.get("broker1").role = "dead";
            //         electNewleader();
            //     }
            //     catch(Exception e){
            //         e.printStackTrace();
            //     }
            // }).start();

            while(true){

                Socket socket = serverSocket.accept();
                System.out.println("Client connected");
                
                new Thread(()->{
                    handleClient(socket);
                }).start();
                
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void produceMessage(String topic,String message,String key,PrintWriter writer){
        try{
            if(!partitionCounters.containsKey(topic)){
                partitionCounters.put(topic,0);
            }
            int partitionCount = topicPartitions.getOrDefault(topic, 2);
            int partition;
            if(key != null&&!key.isEmpty()){
                partition = Math.abs(key.hashCode())%partitionCount;
            }
            else{
                partition = partitionCounters.get(topic) % partitionCount;
                partitionCounters.put(topic,partitionCounters.get(topic)+1);
            }
            String pKey = topic + ":" + partition;
            String leaderId = partitionLeaders.get(pKey);
            if(leaderId == null){
                System.out.println("No leader assigned for " + pKey + " - dropping message");
                writer.println("{\"status\":\"NO_LEADER\"}");
                return;
            }
            BrokerNode leaderBroker = brokers.get(leaderId);
            if(!leaderBroker.messages.containsKey(topic)){
                ArrayList<ArrayList<String>> partitions = new ArrayList<>();
                int partitionCount = topicPartitions.getOrDefault(topic, 2);
                for(int i = 0;i<partitionCount;i++){
                    partitions.add(new ArrayList<>());
                }
                leaderBroker.messages.put(topic,partitions);
            }
            leaderBroker.messages.get(topic).get(partition).add(message);
            applyRetention(topic, partition);
            ArrayList<String> isr = topicISR.getOrDefault(topic + ":" + partition, new ArrayList<>());    
            int  replicatedCount = 1;

            for(String brokerName : brokers.keySet()){
                BrokerNode broker = brokers.get(brokerName);

                if(broker.role.equals(leaderId)||broker.role.equals("dead")){
                    continue;
                }
                if(!isr.contains(brokerName)){
                    System.out.println("Skipping " + brokerName + "- not in ISR for " + topic);
                    continue;
                }
                if(!broker.messages.containsKey(topic)){
                    int partitionsCount = topicPartitions.get(topic);
                    ArrayList<ArrayList<String>> partitions = new ArrayList<>(); 
                    for(int i = 0;i<partitionsCount;i++){
                        partitions.add(new ArrayList<>());
                    }
                    broker.messages.put(topic,partitions);
                }
                broker.messages.get(topic).get(partition).add(message);
                replicatedCount++;
                System.out.println("Replicated to" + broker.brokerId);
            }

            System.out.println("Leader Data : " + brokers.get("broker1").messages);

            System.out.println("Current Leader : " + leaderId);
            System.out.println("Leader Data " + leaderBroker.messages);
            String logFilePath = "logs/" + topic + "-" + partition + ".log";

            FileWriter fileWriter = new FileWriter(logFilePath,true);
            String compressedMessage = compress(message);

            fileWriter.write(compressedMessage + "\n");

            fileWriter.close();
            // int replicaCount = brokers.size()-1;
            if(replicatedCount>=minInSyncReplicas){
                writer.println("{\"status\":\"ACK\"}");
            }
            else{
                writer.println("{\"status\":\"REPLICATION_FAILED\"}");
            }

            System.out.println("stored in topic" + topic);
            System.out.println("\nCurrent topic : ");
            System.out.println(leaderBroker.messages);

            for(ConsumerHandler consumer : consumers){
                if(consumer.topic.equals(topic)&&consumer.partitions.contains(partition)){
                    System.out.println("Sending message to consumer");
                    int offset = leaderBroker.messages.get(topic).get(partition).size() - 1 + logStartOffsets.getOrDefault(pKey, 0);
                    consumer.writer.println("{\"offset\":\"" + offset + "\"," + "\"partition\":\"" + partition + "\","
                    + "\"topic\":\"" + topic + "\","
                    + "\"message\":\"" + message + "\"}");
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void handleClient(Socket socket){
            try{
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream())
                );
                PrintWriter writer = new PrintWriter(socket.getOutputStream(),true);
                String data;
                while ((data = reader.readLine()) != null) {

                    System.out.println("Received Message: " + data);

                    if (data.contains("\"type\":\"create-topic\"")) {
                        String topic = extractValue(data, "topic");
                        int partitions = Integer.parseInt(extractValue(data, "partitions"));
                        topicPartitions.put(topic, partitions);
                        assignPartitionLeaders(topic,partitions);
                        saveTopics();
                        BrokerNode leaderBroker = brokers.get(activeLeader);
                        ArrayList<ArrayList<String>> topicData = new ArrayList<>();
                        for(int i = 0;i<partitions;i++){
                            topicData.add(new ArrayList<>());
                        }
                        leaderBroker.messages.put(topic,topicData);

                        File logDir = new File("logs");
                        if(!logDir.exists()){
                            logDir.mkdirs();
                        }
                        for(int i = 0;i<partitions;i++){
                            File partitionFile = new File("logs/" + topic + "-" + i + ".log");
                            partitionFile.createNewFile();
                        }

                        System.out.println("Created Topic : "+ topic + " with " + partitions + " partitions");
                    }else if(data.contains("\"type\":\"produce\"")){

                        String topic = extractValue(data,"topic");

                        BrokerNode leaderBroker = brokers.get(activeLeader);

                        System.out.println("Current Leader : " + leaderBroker.brokerId);

                        String message = extractValue(data,"message");
                        String key = extractValue(data,"key");
                        produceMessage(topic, message, key,writer);
                        

                    }
                    else if(data.contains("\"type\":\"produce-batch\"")){
                        String topic = extractValue(data, "topic");
                        BrokerNode leaderBroker = brokers.get(activeLeader);

                        System.out.println("Current Leader : " + leaderBroker.brokerId);

                        String messagesStr = extractValue(data,"messages");
                        String[] messages = messagesStr.split(",");
                        for(String msg : messages){
                            produceMessage(topic,msg.trim(),"",writer);
                        }

                    }
                    else if(data.contains("\"type\":\"consume\"")){
                        String consumerId = extractValue(data, "consumerId");
                        String groupId = extractValue(data,"groupId");
                        String topic = extractValue(data, "topic");
                        int offset = Integer.parseInt(extractValue(data, "offset"));
                        if(!consumerGroups.containsKey(groupId))
                        {
                            consumerGroups.put(
                                    groupId,
                                    new ArrayList<>()
                            );
                        }

                        ConsumerHandler consumer = new ConsumerHandler(consumerId,groupId,topic,new ArrayList<>(),writer);
                        consumerGroups.get(groupId).add(consumer);
                        consumers.add(consumer);
                        rebalance(groupId);

                        for(int partition : consumer.partitions){
                            String key =    groupId + ":" + topic + ":" + partition;
                            int committedOffset = consumerOffsets.getOrDefault(key, -1);
                            System.out.println("Partition " + partition + "starting from offset " + (committedOffset+1));
                        }

                        System.out.println("Consumer joined group " + groupId);

                    }
                    else if(data.contains("\"type\":\"commit\"")){
                        String groupId = extractValue(data,"groupId");
                        String topic = extractValue(data, "topic");
                        int partition = Integer.parseInt(extractValue(data,"partition"));
                        int offset = Integer.parseInt(extractValue(data,"offset"));
                        
                        String key = groupId + ":" + topic + ":" + partition;

                        consumerOffsets.put(key,offset);
                        saveOffsets();
                        System.out.println("committed " + key + " -> " + offset);
                    }
                    else if(data.contains("\"type\":\"show-lag\"")){
                        showConsumerlag();
                    }
                    else if(data.contains("\"type\":\"heartbeat\"")){
                        String consumerId = extractValue(data, "consumerId");
                        String groupId = extractValue(data, "groupId");

                        for(ConsumerHandler consumer : consumers){
                            if(consumer.consumerId.equals(consumerId) && consumer.groupId.equals(groupId)){
                                consumer.lastHeartbeat = System.currentTimeMillis();
                                break;
                            }
                        }
                        System.out.println("Heartbeat received from " + consumerId + " in group " + groupId);
                    }

                }
                ConsumerHandler disconnectedConsumer = null;
                for(ConsumerHandler consumer: consumers){
                    if(consumer.writer==writer){
                        disconnectedConsumer = consumer;
                        break;
                    }
                }
                if(disconnectedConsumer!=null){
                    consumers.remove(disconnectedConsumer);
                    consumerGroups.get(disconnectedConsumer.groupId).remove(disconnectedConsumer);
                    System.out.println(disconnectedConsumer.consumerId + "disconnected");
                    rebalance(disconnectedConsumer.groupId);
                }
                System.out.println("Client disconnected");

                socket.close();
                
            }catch(Exception e){
                    e.printStackTrace();
                }
    }
    public static String extractValue(String json,String key){
        String search = "\"" + key + "\":\"";

        int start = json.indexOf(search);

        if(start == -1) return "";

        start += search.length();

        int end = json.indexOf("\"", start);

        return json.substring(start, end);
    }

    public static void electNewleader(String topic,int partition){
        String pKey = topic + ":" + partition;
        String oldLeader = partitionLeaders.get(pKey);
        ArrayList<String> isr = topicISR.getOrDefault(pKey, new ArrayList<>());

        for(String candidate : isr){
            if(candidate.equals(oldLeader)) continue;
            if(brokers.get(candidate).role.equals("dead")) continue;

            partitionLeaders.put(pKey,candidate);
            System.out.println("New leader for " + pKey + ": " + candidate + " (was " + oldLeader + ")");
            return;
        }
        System.out.println("No eligible replica in ISR for " + pKey + "- partition unavailable!");
    }
    public static void startHeartbeatMonitor(){
        new Thread(() -> {
            while(true){
                try{
                    Thread.sleep(HEARTBEAT_CHECK_INTERVAL_MS);
                    checkConsumerLiveness();
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static void checkConsumerLiveness(){
        long now = System.currentTimeMillis();
        ArrayList<ConsumerHandler> expired = new ArrayList<>();

        for(ConsumerHandler consumer : consumers){
            if(now - consumer.lastHeartbeat > SESSION_TIMEOUT_MS){
                expired.add(consumer);
            }
        }

        for(ConsumerHandler consumer : expired){
            System.out.println(consumer.consumerId + " timed out (no heartbeat for "
                + (now - consumer.lastHeartbeat) + "ms) - removing from group " + consumer.groupId);

            consumers.remove(consumer);
            ArrayList<ConsumerHandler> groupMembers = consumerGroups.get(consumer.groupId);
            if(groupMembers != null){
                groupMembers.remove(consumer);
            }
            rebalance(consumer.groupId);
        }
    }

    public static void handleBrokerFailure(String deadBrokerName){
        brokers.get(deadBrokerName).role = ("dead");
        System.out.println("\nBroker " + deadBrokerName + " marked dead. Re-electing affected partitions...");

        for(String pKey : partitionLeaders.keySet()){
            if(partitionLeaders.get(pKey).equals(deadBrokerName)){
                String[] parts = pKey.split(":");
                String topic = parts[0];
                int partition = Integer.parseInt(parts[1]);
                electNewleader(topic, partition);
            }
            topicISR.getOrDefault(pKey, new ArrayList<>()).remove(deadBrokerName);
        }
    }

    public static void assignPartitionLeaders(String topic,int partitionCount){
        ArrayList<String> brokerIds = new ArrayList<>(brokers.keySet());
        int numBrokers = brokerIds.size();
        for(int p = 0;p<partitionCount;p++){
            String pKey = topic + ":" + p;
            String leader = brokerIds.get(p%numBrokers);
            partitionLeaders.put(pKey, leader);

            ArrayList<String> isr = new ArrayList<>(brokerIds);
            topicISR.put(topic, isr);

            System.out.println("Assigned partition " + pKey + " -> leader " + leader);
        }

    }

    public static void recoverBroker(String brokerName){
        BrokerNode recoveringBroker = brokers.get(brokerName);
        recoveringBroker.messages.clear();

        for(String topic : topicPartitions.keySet()){
            int partitionCount = topicPartitions.get(topic);
            if(!recoveringBroker.messages.containsKey(topic)){
                ArrayList<ArrayList<String>> partitions = new ArrayList<>();
                for(int i = 0;i<partitionCount;i++){
                    partitions.add(new ArrayList<>());
                }
                recoveringBroker.messages.put(topic,partitions);
            }
            for(int p = 0;p<partitionCount;p++){
                String pKey = topic + ":" + p;
                String leaderId = partitionLeaders.get(pKey);

                if(leaderId == null){
                    System.out.println("No leader found for " + pKey + " - skipping recovery for this partition");
                    continue;
                }
                if(leaderId.equals(brokerName)){
                    System.out.println(pKey + ": " + brokerName + " is the leader , skipping self-recovery");
                    continue;
                }
                BrokerNode sourceBroker = brokers.get(leaderId);
                if(sourceBroker==null||!sourceBroker.messages.containsKey(topic)){
                    continue;
                }
                ArrayList<String> sourcePartitionData = sourceBroker.messages.get(topic).get(p);
                ArrayList<String> copiedData = new ArrayList<>(sourcePartitionData);
                recoveringBroker.messages.get(topic).set(p,copiedData);

                System.out.println(brokerName + " synced " + pKey + " from leader " + leaderId + " (" + copiedData.size() + " messages)");
                ArrayList<String> isr = topicISR.getOrDefault(pKey, new ArrayList<>());
                if(!isr.contains(brokerName)){
                    isr.add(brokerName);
                }
                topicISR.put(pKey,isr); 
            }
        }
        recoveringBroker.role = "replica";
        System.out.println("Updated ISR : " + topicISR);
        System.out.println(brokerName + " Synchronized with cluster");

        for(String b:brokers.keySet()){
            System.out.println(b + " Data : " + brokers.get(b).messages);
        }
    }

    public static void loadOffsets(){
        try{
            File file = new File("logs/offsets.log");

            System.out.println(
                "Looking for offsets file at: "
                + file.getAbsolutePath()
            );

            if(!file.exists()){
                return;
            }
            BufferedReader reader = new BufferedReader(new FileReader(file));
            
            String line;
             
            while((line = reader.readLine())!=null){
                String[] parts = line.split("=");

                consumerOffsets.put(parts[0],Integer.parseInt(parts[1]));
            }

            reader.close();
            System.out.println("Loaded Offsets : " + consumerOffsets);

        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void saveOffsets(){
        try{
            FileWriter writer = new FileWriter("logs/offsets.log");
            for(String key:consumerOffsets.keySet()){
                writer.write(key + "=" + consumerOffsets.get(key) + "\n");
            }
            writer.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void applyRetention(String topic,int partition){
        try{
            String leaderId = partitionLeaders.get(topic+":"+partition);
            BrokerNode leaderBroker = brokers.get(leaderId);
            ArrayList<String> messages = leaderBroker.messages.get(topic).get(partition);
            String key = topic + ":" + partition;
            int currentStart = logStartOffsets.getOrDefault(key,0);
            while(messages.size()>MAX_MESSAGES_PER_PARTITION){
                messages.remove(0);
                currentStart++;
            }
            logStartOffsets.put(key,currentStart);
            rewritePartitionLog(topic,partition);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void rewritePartitionLog(String topic,int partition){
        try{
            String leaderId = partitionLeaders.get(topic+":"+partition);
            BrokerNode leaderBroker = brokers.get(leaderId);
            String logFilePath = "logs/" + topic + "-" + partition + ".log";
            FileWriter fileWriter = new FileWriter(logFilePath);
            for(String message:leaderBroker.messages.get(topic).get(partition)){
                fileWriter.write(message + "\n");
            }
            fileWriter.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void showConsumerlag(){
        System.out.println("\nConsumer lag Report\n");
        for(String key : consumerOffsets.keySet()){
            String[] parts = key.split(":");
            String groupId = parts[0];
            String topic = parts[1];
            int partition = Integer.parseInt(parts[2]);
            
            String leaderId = partitionLeaders.get(topic+":"+partition);
            BrokerNode leaderBroker = brokers.get(leaderId);
            int logStart = logStartOffsets.getOrDefault(topic+":"+partition,0);
            int lastestOffset = logStart + leaderBroker.messages.get(topic).get(partition).size()-1;
            int committedOffset = consumerOffsets.get(key);
            int lag = lastestOffset - committedOffset;
            System.out.println(key+" | Latest="+lastestOffset+" | Committed="+committedOffset+" | Lag="+lag);
        }
    }

    public static void saveTopics(){
        try{
            FileWriter writer = new FileWriter("logs/topics.log");
            for(String topic : topicPartitions.keySet()){
                writer.write(topic + "=" + topicPartitions.get(topic) + "\n");
            }
            writer.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void loadTopicsFromLogs(){
        try{
            HashMap<String, Integer> maxPartitions = new HashMap<>();
            File logDir = new File("logs");
            if(!logDir.exists()){
                return;
            }
            File[] files = logDir.listFiles();
            if(files==null){
                return;
            }
            for(File file : files){
                if(file.getName().equals("offsets.log")){
                    continue;
                }
                String fileName = file.getName();
                String baseName = fileName.replace(".log","");

                int lastDash = baseName.lastIndexOf("-");
                String topic = baseName.substring(0, lastDash);
                int partition = Integer.parseInt(baseName.substring(lastDash + 1)); 

                int currentMax = maxPartitions.getOrDefault(topic,-1);
                if(partition>currentMax){
                    maxPartitions.put(topic,partition);
                }   
                System.out.println("Topic = " + topic + ", Partition = " + partition);
            }
            for(String topic :  maxPartitions.keySet()){
                int partitionCount = maxPartitions.get(topic)+1;

                topicPartitions.put(topic,partitionCount);
                System.out.println(topic+" has "+partitionCount+" partitions");
            }
            BrokerNode leaderBroker = brokers.get(activeLeader);
            for(String topic:topicPartitions.keySet()){
                int partitionCount = topicPartitions.get(topic);
                ArrayList<ArrayList<String>> partitions = new ArrayList<>() ;
                for(int i = 0; i < partitionCount; i++){
                    partitions.add(new ArrayList<>());
                }
                leaderBroker.messages.put(topic,partitions);
            }

            for(File file : files){

                if(file.getName().equals("offsets.log")){
                    continue;
                }

                String fileName = file.getName();
                String baseName = fileName.replace(".log","");
                int lastDash = baseName.lastIndexOf("-");
                String topic = baseName.substring(0,lastDash);
                int partition = Integer.parseInt(baseName.substring(lastDash + 1));

                BufferedReader reader = new BufferedReader(new FileReader(file));

                String line;
                while((line = reader.readLine()) != null){
                    String originalMessage = decompress(line);
                    leaderBroker.messages.get(topic).get(partition).add(originalMessage);
                }
                reader.close();
            }

            for(String topic : topicPartitions.keySet()){
                int totalMessages = 0;
                ArrayList<ArrayList<String>> partitions = leaderBroker.messages.get(topic);
                for(ArrayList<String> partitionData : partitions){
                    totalMessages += partitionData.size();
                }
                partitionCounters.put(topic,totalMessages);
                System.out.println("Recovered counter for " + topic + " = " + totalMessages);
            }
            System.out.println("Recovered Topics : " + leaderBroker.messages.keySet());
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
    }

    public static void recoverPartition(ConsumerHandler consumer,int partition){
        try{
            String pkey = consumer.topic + ":" + partition;
            int logStart = logStartOffsets.getOrDefault(pkey, 0);
            String key = consumer.groupId + ":" + consumer.topic + ":" + partition;
            int offset = consumerOffsets.getOrDefault(key, -1)+1;
            int startOffset = Math.max(offset,logStart);
            if(offset<logStart){
                System.out.println("Offset " + offset + " was deleted by retention, jumping to " + logStart);
            }
            System.out.println("Recovering partition " + partition + " from offset " + offset);
            String logFilePath = "logs/" + consumer.topic + "-" + partition + ".log";
            File file = new File(logFilePath);
            if(!file.exists()){
                return;
            }
            BufferedReader fileReader = new BufferedReader(new FileReader(file));
            ArrayList<String> messages = new ArrayList<>();
            String line;
            while((line = fileReader.readLine())!=null){
                String originalMessage = decompress(line);
                messages.add(originalMessage);
            } 
            fileReader.close();
            int startIndex = startOffset - logStart;
            for(int i = Math.max(startIndex, 0);i<messages.size();i++){
                consumer.writer.println(
                    "{\"offset\":\""
                    + i
                    + "\","
                    + "\"partition\":\"" 
                    + partition + "\"," + "\"topic\":\"" + consumer.topic + "\"," + "\"message\":\"" + messages.get(i) + "\"}" );
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    
    public static void rebalance(String groupId){
        ArrayList<ConsumerHandler> groupConsumers = consumerGroups.get(groupId);
        if(groupConsumers == null||groupConsumers.isEmpty()){
            return;
        }
        int partitioncount = topicPartitions.getOrDefault(groupConsumers.get(0).topic, 2);
        for(ConsumerHandler consumer : groupConsumers)
        {
            consumer.partitions.clear();
        }
        for(int partition = 0;partition<partitioncount;partition++){
            ConsumerHandler consumer = groupConsumers.get(partition % groupConsumers.size());
            consumer.partitions.add(partition);
        }
        for(ConsumerHandler consumer : groupConsumers){
            for(int partition:consumer.partitions){
                recoverPartition(consumer, partition);
            }
        }
        System.out.println("\n After Rebalance:");
        for(ConsumerHandler consumer: groupConsumers){
            System.out.println(consumer.consumerId + "->" + consumer.partitions);

            consumer.writer.println("{\"type\":\"assignment\"," + "\"partitions\":\"" + consumer.partitions + "\"}");
        }
    }

    public static String compress(String message){
        try{
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(baos);
            gzip.write(message.getBytes());
            gzip.close();
            return Base64.getEncoder().encodeToString(baos.toByteArray());

        }
        catch(Exception e){
            e.printStackTrace();
        }
        return message;
    }
    public static String decompress(String compressedMessage){
        try{
            byte[] compressedBytes = Base64.getDecoder().decode(compressedMessage);
            ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
            GZIPInputStream gzip = new GZIPInputStream(bais);
            BufferedReader reader = new BufferedReader(new InputStreamReader(gzip));
            StringBuilder result = new StringBuilder();
            String line;
            while((line = reader.readLine())!=null){
                result.append(line);
            }
            reader.close();
            return result.toString();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return compressedMessage;
    }

    static class ConsumerHandler{
        String consumerId;
        String groupId;
        String topic;
        ArrayList<Integer> partitions = new ArrayList<>();
        PrintWriter writer;
        long lastHeartbeat;

        ConsumerHandler(String consumerId,String groupId,String topic,ArrayList<Integer> partitions,PrintWriter writer){
            this.consumerId = consumerId;
            this.groupId = groupId;
            this.topic = topic;
            this.partitions = partitions;
            this.writer = writer;
            this.lastHeartbeat = System.currentTimeMillis();
        }
    }
    static class BrokerNode{
        String brokerId;
        String role;

        HashMap<String,ArrayList<ArrayList<String>>> messages = new HashMap<>();

        BrokerNode(String brokerId,String role){
            this.brokerId = brokerId;
            this.role = role;   
        }
    }
}
