import java.util.Base64;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;

import java.util.ArrayList;
import java.util.HashMap;

public class Broker{
    static HashMap<String,ArrayList<ArrayList<String>>> topics = new HashMap<>();
    static HashMap<String,Integer> partitionCounters = new HashMap<>();
    static HashMap<String,Integer> topicPartitions = new HashMap<>();
    static HashMap<String,ArrayList<String>> topicISR = new HashMap<>();
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

            loadOffsets();
            loadTopicsFromLogs();
            for(String topic:topicPartitions.keySet()){
                ArrayList<String> isr = new ArrayList<>();
                isr.add("broker1");
                isr.add("broker2");
                topicISR.put(topic,isr);
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
                        ArrayList<String> isr = new ArrayList<>();
                        isr.add("broker1");
                        isr.add("broker2");
                        topicISR.put(topic,isr);
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

                        if(!leaderBroker.messages.containsKey(topic)){
                            ArrayList<ArrayList<String>> partitions = new ArrayList<>();
                            int partitionCount = topicPartitions.getOrDefault(topic, 2);
                            for(int i = 0;i<partitionCount;i++){
                                partitions.add(new ArrayList<>());
                            }
      
                            leaderBroker.messages.put(
                                topic,
                                partitions
                            );
                        }
                        if(!partitionCounters.containsKey(topic)){
                            partitionCounters.put(topic,0);
                        }
                        int partitionCount = topicPartitions.getOrDefault(topic, 2);
                        int partition;
                        if(key!=null&&!key.isEmpty()){
                            partition = Math.abs(key.hashCode())%partitionCount;
                        }
                        else{
                            partition = partitionCounters.get(topic) % partitionCount;
                            partitionCounters.put(topic,partitionCounters.get(topic)+1);
                        }
                        leaderBroker.messages.get(topic).get(partition).add(message);
                        applyRetention(topic, partition);

                        int  replicatedCount = 0;

                        for(String brokerName : brokers.keySet()){
                            BrokerNode broker = brokers.get(brokerName);

                            if(broker.role.equals("leader")||broker.role.equals("dead")){
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

                        System.out.println(
                            "Leader Data : "
                            + brokers.get("broker1").messages
                        );

                        System.out.println(
                            "Current Leader : "
                            + activeLeader
                        );

                        System.out.println(
                                "Leader Data "
                                + brokers.get(activeLeader).messages
                        );
                        String logFilePath = "logs/" + topic + "-" + partition + ".log";

                        FileWriter fileWriter = new FileWriter(logFilePath,true);

                        fileWriter.write(message + "\n");

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
                                System.out.println(
                                "Sending message to consumer"
                                );
                                int offset = leaderBroker.messages.get(topic).get(partition).size() - 1;
                                consumer.writer.println("{\"offset\":\"" + offset + "\"," + "\"partition\":\"" + partition + "\","
                                 + "\"topic\":\"" + topic + "\","
                                    + "\"message\":\"" + message + "\"}");
                            }
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

    public static void electNewleader(){
        System.out.println("\nLeader crashed!");
        for(String brokerName:brokers.keySet()){
            BrokerNode broker = brokers.get(brokerName);
            if(broker.role.equals("replica")){
                activeLeader = brokerName;
                broker.role = "leader";
                for(String topics:topicISR.keySet()){
                    topicISR.get(topics).remove("broker1");
                }
                System.out.println("Updated ISR : " + topicISR);
                System.out.println("New leader elected: " + activeLeader);
                break;
            }
        }
    }

    public static void recoverBroker(String brokerName){
        BrokerNode recoveringBroker = brokers.get(brokerName);
        BrokerNode leaderBroker = brokers.get(activeLeader);
        recoveringBroker.messages.clear();

        for(String topic : leaderBroker.messages.keySet()){
            ArrayList<ArrayList<String>> leaderPartitions = leaderBroker.messages.get(topic);
            ArrayList<ArrayList<String>> copiedPartitions = new ArrayList<>();
            for(ArrayList<String> partitions:leaderPartitions){
                copiedPartitions.add(new ArrayList<>(partitions));
            }
            recoveringBroker.messages.put(topic,copiedPartitions);
        }
        recoveringBroker.role = "replica";
        for(String topic:topicISR.keySet()){
            if(!topicISR.get(topic).contains(brokerName)){
                topicISR.get(topic).add(brokerName);
            }
        }
        System.out.println("Updated ISR : " + topicISR);
        System.out.println(brokerName + "synchronized with leader");
        System.out.println(
        "Broker1 Data : "
            + brokers.get("broker1").messages
        );

        System.out.println(
            "Broker2 Data : "
            + brokers.get("broker2").messages
        );
        
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
            BrokerNode leaderBroker = brokers.get(activeLeader);
            ArrayList<String> messages = leaderBroker.messages.get(topic).get(partition);
            while(messages.size()>MAX_MESSAGES_PER_PARTITION){
                messages.remove(0);
            }
            rewritePartitionLog(topic,partition);
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void rewritePartitionLog(String topic,int partition){
        try{
            BrokerNode leaderBroker = brokers.get(activeLeader);
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
            
            BrokerNode leaderBroker = brokers.get(activeLeader);
            int lastestOffset = leaderBroker.messages.get(topic).get(partition).size()-1;
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
                    leaderBroker.messages.get(topic).get(partition).add(line);
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
            String key = consumer.groupId + ":" + consumer.topic + ":" + partition;
            int offset = consumerOffsets.getOrDefault(key, -1)+1;
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
                messages.add(line);
            } 
            fileReader.close();
            for(int i = offset;i<messages.size();i++){
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

    static class ConsumerHandler{
        String consumerId;
        String groupId;
        String topic;
        ArrayList<Integer> partitions = new ArrayList<>();

        PrintWriter writer;
        ConsumerHandler(String consumerId,String groupId,String topic,ArrayList<Integer> partitions,PrintWriter writer){
            this.consumerId = consumerId;
            this.groupId = groupId;
            this.topic = topic;
            this.partitions = partitions;
            this.writer = writer;
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
