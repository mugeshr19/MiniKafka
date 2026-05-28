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
    static HashMap<String,ArrayList<String>> topics = new HashMap<>();

    static ArrayList<ConsumerHandler> consumers = new ArrayList<>();
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
                        topics.put(
                            topic,
                            new ArrayList<>()
                        );
                        }
                        topics.get(topic).add(message);

                        String logFilePath = "logs/" + topic + ".log";

                        FileWriter fileWriter = new FileWriter(logFilePath,true);

                        fileWriter.write(message + "\n");

                        fileWriter.close();

                        System.out.println("stored in topic" + topic);
                        System.out.println("\nCurrent topic : ");
                        System.out.println(topics);

                        for(ConsumerHandler consumer : consumers){
                            if(consumer.topic.equals(topic)){
                                System.out.println(
            "Sending message to consumer"
        );
                                consumer.writer.println("{\"topic\":\""+topic+"\","+"\"message\":\""+message+"\"}");
                            }
                        }

                    }
                    else if(data.contains("\"type\":\"consume\"")){
                        String consumerId = extractValue(data, "consumerId");
                        String topic = extractValue(data, "topic");
                        int offset = Integer.parseInt(extractValue(data, "offsets"));
                        PrintWriter writer = new PrintWriter(
                            socket.getOutputStream(),
                            true
                        );
                        consumers.add(new ConsumerHandler(topic,writer));
                        System.out.println("Consumer subscribed to topic " + topic);
                        String logFilePath = "logs/" + topic + ".log";
                        File file = new File(logFilePath);

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
        String topic;
        PrintWriter writer;
        ConsumerHandler(String topic,PrintWriter writer){
            this.topic = topic;
            this.writer = writer;
        }
    }
}
