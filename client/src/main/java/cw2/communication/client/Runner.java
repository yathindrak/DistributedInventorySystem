package cw2.communication.client;

import java.io.IOException;

/**
 * Boostrap the client application
 */
public class Runner {
    public static void main(String[] args) {
        int port = Integer.parseInt(args[0].trim());
        String operation = args[1];

        if (args.length != 2) {
            System.out.println("Usage Inventory manager client app <host> <port> <admin|customer");
            System.exit(1);
        }

        if ("admin".equals(operation)) {
            AdminServiceClient client = null;
            try {
                client = new AdminServiceClient("localhost", port);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            client.processUserRequests();
            client.closeConnection();
        } else if ("customer".equals(operation)) {
            ShopperServiceClient client = null;
            try {
                client = new ShopperServiceClient("localhost", port);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            client.processUserRequests();
            client.closeConnection();
        }

    }
}
