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

    static ArrayList<ConsumerHandler> consumers = new ArrayList<>();
    static HashMap<String,ArrayList<ConsumerHandler>> consumerGroups = new HashMap<>();
    public static void main(String[] args){
        try{
            ServerSocket serverSocket = new ServerSocket(9092);
            System.out.println("Broker is running on port 9092");

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

                        String message = extractValue(data,"message");

                        if(!topics.containsKey(topic)){
                            ArrayList<ArrayList<String>> partitions = new ArrayList<>();
                            partitions.add(new ArrayList<>());
                            partitions.add(new ArrayList<>());

                            topics.put(
                                topic,
                                partitions
                            );
                        }
                        if(!partitionCounters.containsKey(topic)){
                            partitionCounters.put(topic,0);
                        }
                        int partition = partitionCounters.get(topic) % 2;
                        partitionCounters.put(topic,partitionCounters.get(topic)+1);
                        topics.get(topic).get(partition).add(message);
                        System.out.println(
                                "Stored in partition "
                                + partition
                        );
                        String logFilePath = "logs/" + topic + "-" + partition + ".log";

                        FileWriter fileWriter = new FileWriter(logFilePath,true);

                        fileWriter.write(message + "\n");

                        fileWriter.close();

                        System.out.println("stored in topic" + topic);
                        System.out.println("\nCurrent topic : ");
                        System.out.println(topics);

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
}
