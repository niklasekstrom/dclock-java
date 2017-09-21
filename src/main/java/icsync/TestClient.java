package icsync;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

public class TestClient {

	void run() {
		InetSocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 0);
		InetSocketAddress coordinatorAddress = new InetSocketAddress("127.0.0.1", 12000);

		ClientLibrary client = new ClientLibrary();
		client.init(clientAddress, coordinatorAddress);

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		boolean done = false;
		while (!done) {
			System.out.print("> ");
			try {
				String x = br.readLine();
				if (x.equals("exit")) {
					done = true;
				} else if (x.equals("drop-on")) {
					System.out.println("Starting to drop messages");
					client.dropMessages = true;
				} else if (x.equals("drop-off")) {
					System.out.println("Stopped dropping messages");
					client.dropMessages = false;
				} else {
					System.out.println("Unknown command: " + x);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		client.shutdown();
	}

	public static void main(String[] args) {
		new TestClient().run();
	}
}
