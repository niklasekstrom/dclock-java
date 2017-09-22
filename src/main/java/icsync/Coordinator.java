package icsync;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator {

	static class ClockServerProxy {
		final Address address;

		ScheduledFuture<?> communicateFuture = null;

		int nextUnknownCfg = 0;

		RoundId mrho = RoundId.ZERO;
		RoundId arbb = RoundId.ZERO;

		long lastResponseReceived = 0;
		boolean reachable = false;

		boolean isReachable(long now) {
			return now - lastResponseReceived < TimeUnit.SECONDS.toNanos(2);
		}

		public ClockServerProxy(Address address) {
			this.address = address;
		}
	}

	final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    Address myAddress;
	ScheduledThreadPoolExecutor executor;
	ThreadPoolExecutor addressLookupExecutor;
	DatagramChannel ch;
	Selector sel;
	Thread selectThread;
	volatile boolean shuttingDown = false;
	volatile boolean dropMessages = false;

	final HashMap<String, InetSocketAddress> hostPortToAddress = new HashMap<>();

	final ArrayList<ClockServerProxy> clockServersList = new ArrayList<>();
	final HashMap<InetSocketAddress, ClockServerProxy> clockServersMap = new HashMap<>();

	final ArrayList<Configuration> configurations = new ArrayList<>();

	RoundId mrho = RoundId.ZERO;
	RoundId arbb = RoundId.ZERO;

	void init(InetSocketAddress address) {
		myAddress = new Address(address);

		logger.info("Initializing cordinator using address {}.", myAddress);

		executor = new ScheduledThreadPoolExecutor(1);
		addressLookupExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

		try {
			sel = Selector.open();

			ch = DatagramChannel.open();
			ch.configureBlocking(false);
			ch.bind(myAddress.address);

			ch.register(sel, SelectionKey.OP_READ);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}

		executor.submit(this::initOnExecutor);
	}

	void initOnExecutor() {
		selectThread = new Thread(this::selectLoop);
		selectThread.start();

		//addClockServers("127.0.0.1:12001 127.0.0.1:12002 127.0.0.1:12003");
		//executor.schedule(() -> addConf("0 127.0.0.1:12001"), 500, TimeUnit.MILLISECONDS);
	}

	void shutdown() {
		try {
			executor.submit(this::shutdownOnExecutor).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		sel.wakeup();

		try {
			selectThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		try {
			ch.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			sel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		addressLookupExecutor.shutdown();
		try {
			addressLookupExecutor.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		executor.shutdown();
		try {
			executor.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		logger.info("Shutdown complete.");
	}

	void shutdownOnExecutor() {
		shuttingDown = true;

		for (ClockServerProxy cs : clockServersList) {
			if (cs.communicateFuture != null) {
				cs.communicateFuture.cancel(false);
				cs.communicateFuture = null;
			}
		}
	}

	void selectLoop() {
		boolean done = false;
		while (!done && !shuttingDown) {
			try {
				sel.select();
				Iterator<SelectionKey> it = sel.selectedKeys().iterator();
				while (it.hasNext()) {
					SelectionKey k = it.next();
					if (k.isReadable()) {
						try {
							ByteBuffer dst = ByteBuffer.allocate(1024);
							InetSocketAddress address = (InetSocketAddress) ch.receive(dst);
							dst.flip();
							executor.submit(() -> processMessage(address, dst));
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					it.remove();
				}
			} catch (ClosedSelectorException e) {
				done = true;
			} catch (IOException e) {
				e.printStackTrace();
				done = true;
			}
		}
	}

	void processMessage(InetSocketAddress address, ByteBuffer msg) {
		if (dropMessages || shuttingDown) {
			return;
		}

		byte kind = msg.get();
		if (kind == Message.MSG_MULTIPART_RES) {
			processMultipartResponse(address, msg);
		} else if (kind == Message.MSG_CLIENT_COORDINATOR_REQ) {
			processClientCoordinatorRequest(address, msg);
		}
	}

	void sendDatagram(InetSocketAddress address, ByteBuffer src) {
		if (dropMessages || shuttingDown) {
			return;
		}
		actuallySendDatagram(address, src);
		//executor.schedule(() -> actuallySendDatagram(address, src), 90, TimeUnit.MILLISECONDS);
	}

	void actuallySendDatagram(InetSocketAddress address, ByteBuffer src) {
		if (dropMessages || shuttingDown) {
			return;
		}

		try {
			int n = ch.send(src, address);
			if (n == 0) {
				throw new RuntimeException("DatagramChannel.send() returned zero!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	void addClockServers(String hostPorts) {
		String[] arr = hostPorts.split("\\s+");
		for (String hostPort : arr) {
			addClockServer(hostPort);
		}
	}

	void addClockServer(String hostPort) {
		String[] arr = hostPort.split(":");
		if (arr.length != 2) {
			logger.warn("Invalid address " + hostPort + ". Must be <hostname:port>");
		} else {
			CompletableFuture.supplyAsync(() -> {
				InetSocketAddress address = null;
				try {
					String hostname = arr[0];
					int port = Integer.parseInt(arr[1]);
					address = new InetSocketAddress(hostname, port); 
				} catch (Exception e) {
					e.printStackTrace();
				}
				return address;
			}, addressLookupExecutor).thenAcceptAsync(address -> {
				if (address == null || address.isUnresolved()) {
					logger.warn("Couldn't resolve address " + hostPort);
				} else if (clockServersMap.containsKey(address)) {
					logger.warn("The clock server was previously added");
				} else {
					hostPortToAddress.put(hostPort, address);

					ClockServerProxy cs = new ClockServerProxy(new Address(address));
					clockServersList.add(cs);
					clockServersMap.put(address, cs);

					cs.communicateFuture = executor.schedule(() -> communicateClockServer(cs), 0, TimeUnit.SECONDS);

					logger.info("Added clock server " + cs.address);
				}
			}, executor);
		}
	}

	void addConf(String arg) {
		String[] arr = arg.split("\\s+");
		if (arr.length < 2) {
			logger.warn("Invalid format, usage: conf confId clockServer1 clockServer2 ...");
			return;
		}

		int cfgId = Integer.parseInt(arr[0]);
		if (cfgId != configurations.size()) {
			logger.warn("Tried to add configuration " + cfgId + " but configuration " + configurations.size() + " is the next free configuration");
			return;
		}

		if (arbb.cfgId < configurations.size() - 1) {
			logger.warn("The configuration before the current is not blocked, so cannot start another configuration");
			return;
		}

		Address[] members = new Address[arr.length - 1];
		for (int i = 0; i < members.length; i++) {
			String hostPort = arr[i + 1];
			InetSocketAddress address = hostPortToAddress.get(hostPort);
			ClockServerProxy match = address == null ? null : clockServersMap.get(address);

			if (match == null) {
				logger.warn("ClockServer " + hostPort + " had not been previously added");
				return;
			}

			if (Util.arrayContains(members, match.address)) {
				logger.warn("ClockServer " + hostPort + " was mentioned more than once");
				return;
			}

			if (!match.reachable) {
				logger.warn("ClockServer " + hostPort + " is not reachable");
				return;
			}

			members[i] = match.address;
		}

		if (cfgId == 0 && members.length != 1) {
			logger.warn("Configuration zero must contain exactly one member");
			return;
		}

		if (cfgId > 0) {
			Configuration prev = configurations.get(cfgId - 1);
			if (members.length == prev.members.length) {
				boolean same = true;
				for (int i = 0; i < members.length; i++) {
					if (!members[i].equals(prev.members[i])) {
						same = false;
						break;
					}
				}

				if (same) {
					logger.warn("The given configuration is exactly the same as the most recent configuration");
					return;
				}
			}
		}

		Configuration cfg = new Configuration(cfgId, members);
		configurations.add(cfg);

		logger.info("Configuration {} added", cfg);

		rescheduleCommunications();
	}

	void rescheduleCommunications() {
		for (ClockServerProxy cs : clockServersList) {
			if (cs.communicateFuture != null) {
				cs.communicateFuture.cancel(false);
				cs.communicateFuture = executor.schedule(() -> communicateClockServer(cs), 0, TimeUnit.SECONDS);
			}
		}
	}

	void communicateClockServer(ClockServerProxy cs) {
		cs.communicateFuture = null;

		long now = System.nanoTime();

		boolean reachable = cs.isReachable(now);

		if (cs.reachable && !reachable) {
			cs.reachable = false;
			logger.info("ClockServer " + cs.address + " became unreachable");
		}

		MultipartRequest mreq = new MultipartRequest();

		if (cs.mrho.compareTo(mrho) < 0) {
			mreq.mrho = mrho;
		}

		if (cs.arbb.compareTo(arbb) < 0) {
			mreq.arbb = arbb;
		}

		mreq.getMRHO = true;
		mreq.getARBB = true;

		if (cs.reachable) {
			if (cs.nextUnknownCfg < configurations.size()) {
				Configuration cfg = configurations.get(cs.nextUnknownCfg);
				mreq.nextUnknownCfg = cfg;
			}
		}

		ByteBuffer req = ByteBuffer.allocate(1000);
		req.put(Message.MSG_MULTIPART_REQ);
		MultipartRequest.serialize(mreq, req);
		req.flip();
		sendDatagram(cs.address.address, req);

		cs.communicateFuture = executor.schedule(() -> communicateClockServer(cs), 1, TimeUnit.SECONDS);
	}

	void processMultipartResponse(InetSocketAddress address, ByteBuffer res) {
		ClockServerProxy cs = clockServersMap.get(address);
		if (cs == null) {
			logger.info("Received multipart response from unknown address " + address);
			return;
		}

		long now = System.nanoTime();

		MultipartResponse mres = MultipartResponse.deserialize(res);

		cs.lastResponseReceived = now;

		if (!cs.reachable) {
			cs.reachable = true;
			logger.info("ClockServer " + cs.address + " became reachable");
		}

		if (mres.mrho != null) {
			if (cs.mrho.compareTo(mres.mrho) < 0) {
				cs.mrho = mres.mrho;
			}

			if (mrho.compareTo(mres.mrho) < 0) {
				logger.info("Max round heard of updated from " + mrho + " to " + mres.mrho + " because multipart response from " + cs.address);
				mrho = mres.mrho;
			}
		}

		if (mres.arbb != null) {
			if (cs.arbb.compareTo(mres.arbb) < 0) {
				cs.arbb = mres.arbb;
			}

			if (arbb.compareTo(mres.arbb) < 0) {
				logger.info("ARBB updated from " + arbb + " to " + mres.arbb + " because multipart response from " + cs.address);
				arbb = mres.arbb;
			}
		}

		if (mres.containsNextUnknownCfgId) {
			int nextUnknownCfg = mres.nextUnknownCfgId;
			if (cs.nextUnknownCfg < nextUnknownCfg) {
				cs.nextUnknownCfg = nextUnknownCfg;
			}
		}

		if (cs.nextUnknownCfg < configurations.size()) {
			cs.communicateFuture.cancel(false);
			cs.communicateFuture = executor.schedule(() -> communicateClockServer(cs), 0, TimeUnit.SECONDS);
		}
	}

	void processClientCoordinatorRequest(InetSocketAddress address, ByteBuffer req) {
		ByteBuffer res = ByteBuffer.allocate(1000);
		res.put(Message.MSG_CLIENT_COORDINATOR_RES);

		int nextUnknownCfgId = req.getShort();
		int maxCfgId = configurations.size() - 1;

		if (nextUnknownCfgId <= maxCfgId) {
			res.put((byte) 1);
			Configuration cfg = configurations.get(maxCfgId);
			Configuration.serialize(cfg, res);
		} else {
			res.put((byte) 0);
		}

		RoundId.serialize(mrho, res);

		res.flip();
		sendDatagram(address, res);
	}

	void repl() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
		while (!done) {
			System.out.print("> ");
			try {
				String x = br.readLine();
				if (x.equals("")) {
				} else if (x.equals("exit")) {
					done = true;
				} else if (x.startsWith("add ")) {
					executor.submit(() -> addClockServers(x.substring(4)));
				} else if (x.startsWith("conf ")) {
					executor.submit(() -> addConf(x.substring(5)));
				} else if (x.equals("drop-on")) {
					dropMessages = true;
				} else if (x.equals("drop-off")) {
					dropMessages = false;
				} else {
					System.out.println("Unknown command: " + x);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	void run(String[] args) {
		InetSocketAddress address = null;

		if (args.length < 1) {
			println("Usage: coordinator <my-address>\n   where address has the format host:port");
			return;
		}

		String[] arr = args[0].split(":");
		if (arr.length != 2) {
			println("Address format is host:port");
		}

		int port = 0;
		try {
			port = Integer.parseInt(arr[1]);
		} catch (NumberFormatException e) {
			println("Unable to parse port number");
			return;
		}

		try {
			address = new InetSocketAddress(arr[0], port);
		} catch (IllegalArgumentException e) {
			println("Port is outside of valid range");
			return;
		}

		if (address.isUnresolved()) {
			println("Unable to resolve address " + args);
			return;
		}

		init(address);
		repl();
		shutdown();
	}

	static void println(String x) {
		System.out.println(x);
	}

	public static void main(String[] args) {
		new Coordinator().run(args);
	}
}
