import java.io.PrintWriter;
import java.net.Socket;

public class LagClient {
    public static void main(String[] args){
        try{
            Socket socket = new Socket("localhost",9092);
            PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
            out.println("{\"type\":\"show-lag\"}");
            socket.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
