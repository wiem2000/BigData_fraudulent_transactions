import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class CsvSocketServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 9999;
        String csvFilePath = "db fraud/fraudTest.csv"; // until 1000 is random

        try (ServerSocket serverSocket = new ServerSocket(port);
             FileReader fileReader = new FileReader(csvFilePath);
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {

            System.out.println("Server is running on port " + port);

            Socket clientSocket = serverSocket.accept();
            System.out.println("Client connected: " + clientSocket.getInetAddress().getHostAddress());

            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println("Sending: " + line);
                out.println(line);
                Thread.sleep(1000);  // Simulate streaming delay
            }
        }
    }
}
