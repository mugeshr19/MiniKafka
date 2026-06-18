import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Producer {
    public static void main(String[] args){
        try{
            Socket socket = new Socket("localhost",9092);
            System.out.println("Connected to broker");

            PrintWriter writer = new PrintWriter(
                socket.getOutputStream(),true   
            );

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            //writer.println("Hello kafka this is mugesh");
            /*String message =
                    "{\"topic\":\"user-events\","
                    + "\"message\":\"USER_CREATED\"}";*/

            /*String message =
                "{\"topic\":\"payments\","
                + "\"message\":\"PAYMENT_SUCCESS\"}";*/
            
            String message =
                    "{\"type\":\"produce\","
                    + "\"topic\":\"user-events\","
                    + "\"message\":\"mugesh\"}";  

            writer.println(message);
            String response = reader.readLine();
            System.out.println("Broker Response : "+response);
            socket.close();
            
        }catch (Exception e){
            e.printStackTrace();    
        }
    }
}
