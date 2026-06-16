import java.io.PrintWriter;
import java.net.Socket;

public class TopicTest {

    public static void main(String[] args) {

        try {

            Socket socket =
                new Socket("localhost", 9092);

            PrintWriter out =
                new PrintWriter(
                    socket.getOutputStream(),
                    true
                );

            out.println(
                "{\"type\":\"create-topic\","
                + "\"topic\":\"orders\","
                + "\"partitions\":\"3\"}"
            );

            System.out.println(
                "Create topic request sent."
            );

            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}