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

            new Thread(()->{
                try{
                    Thread.sleep(20000);
                    brokers.get("broker1").role = "dead";
                    electNewleader();
                    Thread.sleep(15000);
                    System.out.println("\nBroker1 recovered!");
                    recoverBroker("broker1");
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }).start();

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
                            if(consumer.topic.equals(topic)&&consumer.partition==partition){
                                System.out.println(
                                "Sending message to consumer"
                            );
                                consumer.writer.println("{\"topic\":\""+topic+"\","+"\"message\":\""+message+"\"}");
                            }
                        }

                    }
                    else if(data.contains("\"type\":\"consume\"")){
                        String consumerId = extractValue(data, "consumerId");
                        String groupId = extractValue(data,"groupId");
                        String topic = extractValue(data, "topic");
                        int offset = Integer.parseInt(extractValue(data, "offset"));
                        PrintWriter writer = new PrintWriter(
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
                        int partition = consumerGroups.get(groupId).size()% 2;
                        ConsumerHandler consumer = new ConsumerHandler(consumerId,groupId,topic,partition,writer);
                        consumerGroups.get(groupId).add(consumer);
                        consumers.add(consumer);
                        System.out.println("Consumer assigned partition " + partition);
                        
                        String logFilePath = "logs/" + topic + "-" + partition + ".log";
                        File file = new File(logFilePath);

                        System.out.println(
                            "Consumer "
                            + consumerId
                            + " reading "
                            + logFilePath
                        );

                        if(file.exists()){
                            BufferedReader fileReader = new BufferedReader(new FileReader(file));
                            ArrayList<String> messages = new ArrayList<>();
                            String line;
                            while((line = fileReader.readLine())!=null){
                                messages.add(line);
                            }
                            fileReader.close();

                            for(int i = offset;i<messages.size();i++){
                                writer.println("{\"offset\":\"" + i + "\"," + "\"topic\":\"" + topic + "\"," + "\"message\":\"" + messages.get(i) + "\"}");
                            }
                        }
                    }

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
            }
            break;
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

    static class ConsumerHandler{
        String consumerId;
        String groupId;
        String topic;
        int partition;

        PrintWriter writer;
        ConsumerHandler(String consumerId,String groupId,String topic,int partition,PrintWriter writer){
            this.consumerId = consumerId;
            this.groupId = groupId;
            this.topic = topic;
            this.partition = partition;
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
