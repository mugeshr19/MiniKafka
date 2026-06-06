import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.net.Socket;

public class Consumer1 {
    public static void main(String[] args){
        try{
            Socket socket = new Socket("localhost",9092);
            System.out.println("connected to broker");
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            //String subscribeMessage = "{\"type\":\"consume\",\"topic\":\"user-events\"}";

            String subscribeMessage = "{\"type\":\"consume\"," + "\"consumerId\":\"consumer1\"," + "\"groupId\":\"groupA\"," 
            + "\"topic\":\"user-events\"," + "\"offset\":\"0\"}";

            out.println(subscribeMessage);

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String data;

            while((data = reader.readLine())!=null){
                System.out.println("Received Event from 1 : " + data);

                String topic = extractValue(data,"topic");
                String offsetStr = extractValue(data,"offset");
                String partitionStr = extractValue(data, "partition");
                if(!offsetStr.equals("")){
                    String commitMessage = "{\"type\":\"commit\"," + "\"consumerId\":\"consumer1\","
                    + "\"topic\":\"" + topic + "\"," + "\"partition\":\"" + partitionStr +"\"," + "\"offset\":\"" + offsetStr + "\"}";

                    out.println(commitMessage);

                    System.out.println("Commit offset " + offsetStr);
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    public static String extractValue(
        String json,
        String key
    ){
        String search =
            "\"" + key + "\":\"";

        int start =
            json.indexOf(search);

        if(start == -1){
            return "";
        }

        start += search.length();

        int end =
            json.indexOf("\"", start);

        return json.substring(start,end);
    }
}
