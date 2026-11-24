import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Runs as its OWN Java process (its own memory). It never sees the Master's array directly:
 * the numbers arrive through a socket, which is what makes this distributed-memory processing.
 *
 * Protocol (all values via DataInputStream / DataOutputStream):
 *   Master -> Worker : int n, then n doubles
 *   Worker -> Master : long count, double sum, double mean, double m2, double min, double max
 *
 * Start with:  java -cp out DistributedWorker 5001
 */
public class DistributedWorker {

    public static void main(String[] args) throws IOException {
        int port = 5001;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("[Worker] Invalid port '" + args[0] + "', using 5001.");
            }
        }

        try (ServerSocket server = new ServerSocket(port, 50, InetAddress.getByName("127.0.0.1"))) {
            System.out.println("[Worker] Listening on 127.0.0.1:" + port + "  (press Ctrl+C to stop)");
            while (true) {
                try (Socket client = server.accept()) {
                    handle(client, port);
                } catch (IOException e) {
                    System.out.println("[Worker " + port + "] Connection error: " + e.getMessage());
                }
            }
        }
    }

    private static void handle(Socket client, int port) throws IOException {
        client.setSoTimeout(30000);
        DataInputStream in = new DataInputStream(new BufferedInputStream(client.getInputStream()));
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(client.getOutputStream()));

        int n = in.readInt();
        if (n < 0) {
            throw new IOException("Received a negative partition size.");
        }
        double[] partition = new double[n];
        for (int i = 0; i < n; i++) {
            partition[i] = in.readDouble();
        }
        System.out.println("[Worker " + port + "] Received partition of " + n + " values. Calculating...");

        Statistics.Partial p = Statistics.computeRange(partition, 0, n);

        out.writeLong(p.count);
        out.writeDouble(p.sum);
        out.writeDouble(p.mean);
        out.writeDouble(p.m2);
        out.writeDouble(p.min);
        out.writeDouble(p.max);
        out.flush();
        System.out.println("[Worker " + port + "] Partial result sent (count=" + p.count + ", sum=" + p.sum + ").");
    }
}