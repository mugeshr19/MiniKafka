import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import java.util.ArrayList;
import java.util.HashMap;

public class Broker{
    static HashMap<String,ArrayList<String>> topics = new HashMap<>();
    public static void main(String[] args){
        try{
            ServerSocket serverSocket = new ServerSocket(9092);
            System.out.println("Broker is running on port 9092");

            while(true){

                Socket socket = serverSocket.accept();
                System.out.println("Producer connected");

                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream())
                );

                String data;
                while ((data = reader.readLine()) != null) {

                    System.out.println("Received Message: " + data);

                    if(data.contains("\"topic\"")&&data.contains("\"message\"")){

                        String topic = extractValue(data,"topic");

                        String message = extractValue(data,"message");

                        if(!topics.containsKey(topic)){
                        topics.put(
                            topic,
                            new ArrayList<>()
                        );
                        }
                        topics.get(topic).add(message);
                    }

                    System.out.println("\nCurrent topic : ");
                    System.out.println(topics);
                }

                System.out.println("Producer disconnected");

                socket.close();
            }

        }catch (Exception e) {
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
}
