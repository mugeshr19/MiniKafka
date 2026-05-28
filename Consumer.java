import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.net.Socket;

public class Consumer {
    public static void main(String[] args){
        try{
            Socket socket = new Socket("localhost",9092);
            System.out.println("connected to broker");
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            //String subscribeMessage = "{\"type\":\"consume\",\"topic\":\"user-events\"}";

            String subscribeMessage = "{\"type\":\"consume\"," + "\"consumerId\":\"consumer1\"," + "\"topic\":\"user-events\","
            + "\"offsets\":\"1\"}";

            out.println(subscribeMessage);

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String data;

            while((data = reader.readLine())!=null){
                System.out.println("Received Event : " + data);
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
