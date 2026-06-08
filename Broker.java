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
    static HashMap<String,Integer> consumerOffsets = new HashMap<>();
    static HashMap<String,BrokerNode> brokers = new HashMap<>();
    static String activeLeader = "broker1";

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

            System.out.println("Recovered Topics : " + brokers.get(activeLeader).messages);

            // new Thread(()->{
            //     try{
            //         Thread.sleep(20000);
            //         brokers.get("broker1").role = "dead";
            //         electNewleader();
            //         Thread.sleep(15000);
            //         System.out.println("\nBroker1 recovered!");
            //         recoverBroker("broker1");
            //     }
            //     catch(Exception e){
            //         e.printStackTrace();
            //     }
            // }).start();

            ServerSocket serverSocket = new ServerSocket(9092);
            System.out.println("Broker is running on port 9092");

            /*new Thread(()->{
                try{
                    Thread.sleep(20000);
                    brokers.get("broker1").role = "dead";
                    electNewleader();
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }).start();*/

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
        PrintWriter writer = null;
            try{
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream())
                );

                String data;
                while ((data = reader.readLine()) != null) {

                    System.out.println("Received Message: " + data);

                    if(data.contains("\"type\":\"produce\"")){

                        String topic = extractValue(data,"topic");

                        BrokerNode leaderBroker = brokers.get(activeLeader);

                        System.out.println("Current Leader : " + leaderBroker.brokerId);

                        String message = extractValue(data,"message");

                        if(!leaderBroker.messages.containsKey(topic)){
                            ArrayList<ArrayList<String>> partitions = new ArrayList<>();
                            partitions.add(new ArrayList<>());
                            partitions.add(new ArrayList<>());

                            leaderBroker.messages.put(
                                topic,
                                partitions
                            );
                        }
                        if(!partitionCounters.containsKey(topic)){
                            partitionCounters.put(topic,0);
                        }
                        int partition = partitionCounters.get(topic) % 2;
                        partitionCounters.put(topic,partitionCounters.get(topic)+1);
                        leaderBroker.messages.get(topic).get(partition).add(message);

                        for(String brokerName : brokers.keySet()){
                            BrokerNode broker = brokers.get(brokerName);

                            if(broker.role.equals("leader")||broker.role.equals("dead")){
                                continue;
                            }
                            if(!broker.messages.containsKey(topic)){
                                ArrayList<ArrayList<String>> partitions = new ArrayList<>();
                                new ArrayList<>();
                                partitions.add(new ArrayList<>());
                                partitions.add(new ArrayList<>());

                                broker.messages.put(topic,partitions);
                            }
                            broker.messages.get(topic).get(partition).add(message);
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
                        writer = new PrintWriter(
                            socket.getOutputStream(),
                            true
                        );
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

    public static void loadTopicsFromLogs(){
        try{
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

                BrokerNode leaderBroker = brokers.get(activeLeader);
                if(!leaderBroker.messages.containsKey(topic)){
                    ArrayList<ArrayList<String>> partitions = new ArrayList<>();
                    partitions.add(new ArrayList<>());
                    partitions.add(new ArrayList<>());

                    leaderBroker.messages.put(topic,partitions);
                }

                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                while((line=reader.readLine())!=null){
                    leaderBroker.messages.get(topic).get(partition).add(line);
                }
                reader.close();

                System.out.println("Topic = " + topic + ", Partition = " + partition);
            }
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
        int partitioncount = 2;
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
        }
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
