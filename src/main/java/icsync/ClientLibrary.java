package icsync;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientLibrary {

	static class CoordinatorProxy {
		final Address address;

		boolean isCommunicating = false;
		long lastCommunicationStarted = 0;
		long lastCommunicationEnded = 0;
		long nextRetransmission = 0;
		int rto = 1;

		public CoordinatorProxy(Address address) {
			this.address = address;
		}
	}

	static class ClockServerProxy {
		final Address address;

		boolean isCommunicating = false;
		long lastCommunicationStarted = 0;
		long nextRetransmission = 0;
		int rto = 1;

		public ClockServerProxy(Address address) {
			this.address = address;
		}
	}

    final Logger logger = LoggerFactory.getLogger(ClientLibrary.class);

	ScheduledThreadPoolExecutor executor;
	Address myAddress;
	DatagramChannel ch;
	Selector selector;
	Thread selectThread;
	volatile boolean shuttingDown = false;
	volatile boolean dropMessages = false;

	CoordinatorProxy coordinator = null;

	Configuration maxCfg = null;
	final HashMap<InetSocketAddress, ClockServerProxy> serversMap = new HashMap<>();
	final ArrayList<ClockServerProxy> priority = new ArrayList<>(); // Includes only members in maxCfg.

	RoundId mrho = RoundId.ZERO;

	ScheduledFuture<?> periodicTimer = null;

	ClockTriple certainReading = null;
	long lastSynced = 0;

	void init(InetSocketAddress clientAddress, InetSocketAddress coordinatorAddress) {
		// Both addresses must be resolved, otherwise throws run-time exception.
		myAddress = new Address(clientAddress);
		coordinator = new CoordinatorProxy(new Address(coordinatorAddress));

		logger.info("Initializing using address {} and coordinator address {}.", myAddress, coordinator.address);

		executor = new ScheduledThreadPoolExecutor(1);

		try {
			selector = Selector.open();

			ch = DatagramChannel.open();
			ch.configureBlocking(false);
			ch.bind(myAddress.address);

			ch.register(selector, SelectionKey.OP_READ);
		} catch (IOException e) {
			logger.error("Unable to initialize client library", e);
			throw new UncheckedIOException(e);
		}

		executor.submit(this::initOnExecutor);
	}

	void initOnExecutor() {
		selectThread = new Thread(this::selectLoop);
		selectThread.start();

		periodicTimer = executor.schedule(this::periodicTimeout, 0, TimeUnit.SECONDS);
	}

	void shutdown() {
		try {
			executor.submit(this::shutdownOnExecutor).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		selector.wakeup();

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
			selector.close();
		} catch (IOException e) {
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

		// Cancel timers.
		if (periodicTimer != null) {
			periodicTimer.cancel(false);
			periodicTimer = null;
		}
	}

	void selectLoop() {
		boolean done = false;
		while (!done && !shuttingDown) {
			try {
				selector.select();
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
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

	void logStatusMessage(long now) {
		StringBuilder sb = new StringBuilder();

		sb.append("Status: mrho = " + mrho + ", clock ");
		sb.append(certainReading == null ? "not started" : ("= " + readClock(now)));

		logger.info(sb.toString());
	}

	void periodicTimeout() {
		periodicTimer = null;

		long now = System.nanoTime();
		logStatusMessage(now);

		if (maxCfg == null || maxCfg.cfgId < mrho.cfgId) {
			if (!coordinator.isCommunicating && now - coordinator.lastCommunicationEnded >= TimeUnit.SECONDS.toNanos(30)) {
				coordinator.isCommunicating = true;
				coordinator.lastCommunicationStarted = now;
				sendClientCoordinatorRequest();
				coordinator.rto = 1;
				coordinator.nextRetransmission = now + TimeUnit.SECONDS.toNanos(coordinator.rto);
			}
		} else if (now - lastSynced >= TimeUnit.SECONDS.toNanos(4)) {
			boolean allAreUnreachable = true;

			for (ClockServerProxy cs : priority) {
				if (cs.isCommunicating) {
					boolean isReachable = now - cs.lastCommunicationStarted < TimeUnit.SECONDS.toNanos(5);
					if (isReachable) {
						allAreUnreachable = false;
						break;
					}
				} else {
					cs.isCommunicating = true;
					cs.lastCommunicationStarted = now;
					sendClockServerRequest(cs);
					cs.rto = 1;
					cs.nextRetransmission = now + TimeUnit.SECONDS.toNanos(cs.rto);
					allAreUnreachable = false;
					break;
				}
			}

			if (allAreUnreachable) {
				if (!coordinator.isCommunicating && now - coordinator.lastCommunicationEnded >= TimeUnit.SECONDS.toNanos(30)) {
					coordinator.isCommunicating = true;
					coordinator.lastCommunicationStarted = now;
					sendClientCoordinatorRequest();
					coordinator.rto = 1;
					coordinator.nextRetransmission = now + TimeUnit.SECONDS.toNanos(coordinator.rto);
				}
			}
		}

		if (coordinator.isCommunicating && now - coordinator.nextRetransmission >= 0) {
			sendClientCoordinatorRequest();
			coordinator.rto = Math.min(coordinator.rto * 2, 10);
			coordinator.nextRetransmission = now + TimeUnit.SECONDS.toNanos(coordinator.rto);
		}

		for (ClockServerProxy cs : priority) {
			if (cs.isCommunicating && now - cs.nextRetransmission >= 0) {
				sendClockServerRequest(cs);
				cs.rto = Math.min(cs.rto * 2, 10);
				cs.nextRetransmission = now + TimeUnit.SECONDS.toNanos(cs.rto);
			}
		}

		periodicTimer = executor.schedule(this::periodicTimeout, 1, TimeUnit.SECONDS);
	}

	void sendClientCoordinatorRequest() {
		ByteBuffer req = ByteBuffer.allocate(3);
		req.put(Message.MSG_CLIENT_COORDINATOR_REQ);

		int nextUnknownCfgId = maxCfg == null ? 0 : (maxCfg.cfgId + 1);
		req.putShort((short) nextUnknownCfgId);

		req.flip();
		sendDatagram(coordinator.address.address, req);
	}

	void sendClockServerRequest(ClockServerProxy cs) {
		MultipartRequest mreq = new MultipartRequest();

		long now = System.nanoTime();

		mreq.getMRHO = true;
		mreq.readClock = true;
		mreq.sent = now;

		ByteBuffer req = ByteBuffer.allocate(11);
		req.put(Message.MSG_MULTIPART_REQ);
		MultipartRequest.serialize(mreq, req);
		req.flip();
		sendDatagram(cs.address.address, req);
	}

	void processMessage(InetSocketAddress address, ByteBuffer msg) {
		if (dropMessages || shuttingDown) {
			return;
		}

		byte kind = msg.get();
		if (kind == Message.MSG_CLIENT_COORDINATOR_RES) {
			processClientCoordinatorResponse(address, msg);
		} else if (kind == Message.MSG_MULTIPART_RES) {
			processMultipartResponse(address, msg);
		}
	}

	void processClientCoordinatorResponse(InetSocketAddress address, ByteBuffer res) {
		long now = System.nanoTime();

		if (coordinator.isCommunicating) {
			coordinator.lastCommunicationEnded = now;
			coordinator.isCommunicating = false;
		}

		boolean shouldReprioritize = false;

		boolean hasCfg = res.get() != 0;
		if (hasCfg) {
			Configuration cfg = Configuration.deserialize(res);
			if (maxCfg == null || maxCfg.cfgId < cfg.cfgId) {

				List<Address> prevMembers = Arrays.asList(maxCfg == null ? new Address[0] : maxCfg.members);
				List<Address> nextMembers = Arrays.asList(cfg.members);

				HashSet<Address> toAdd = new HashSet<>(nextMembers);
				toAdd.removeAll(prevMembers);

				HashSet<Address> toRemove = new HashSet<>(prevMembers);
				toRemove.removeAll(nextMembers);

				for (Address member : toRemove) {
					serversMap.remove(member.address);
				}

				for (Address member : toAdd) {
					ClockServerProxy cs = new ClockServerProxy(member);
					serversMap.put(cs.address.address, cs);
				}

				maxCfg = cfg;
				shouldReprioritize = true;
			}
		}

		RoundId r = RoundId.deserialize(res);
		if (mrho.compareTo(r) < 0) {
			mrho = r;
			shouldReprioritize = true;
		}

		if (shouldReprioritize) {
			reprioritize();
		}
	}

	void processMultipartResponse(InetSocketAddress address, ByteBuffer res) {
		long now = System.nanoTime();

		ClockServerProxy cs = serversMap.get(address);
		if (cs == null) {
			return;
		}

		if (cs.isCommunicating) {
			cs.isCommunicating = false;

			if (maxCfg.cfgId >= mrho.cfgId) {
				int index = priority.indexOf(cs);

				for (int i = index + 1; i < priority.size(); i++) {
					priority.get(i).isCommunicating = false;
				}

				if (coordinator.isCommunicating) {
					coordinator.isCommunicating = false;
					coordinator.lastCommunicationEnded = now;
				}
			}
		}

		MultipartResponse mres = MultipartResponse.deserialize(res);

		if (mres.containsClock) {
			processSyncResponse(mres.reqSent, now, mres.c, mres.e);
			lastSynced = now;
		}

		if (mres.mrho != null && mrho.compareTo(mres.mrho) < 0) {
			mrho = mres.mrho;

			if (maxCfg.cfgId >= mrho.cfgId) {
				reprioritize();
			}
		}
	}

	void reprioritize() {
		assert maxCfg.cfgId >= mrho.cfgId;

		priority.clear();

		Address[] members = maxCfg.members;

		if (mrho.cfgId == maxCfg.cfgId) {
			priority.add(serversMap.get(members[mrho.index].address));
			for (int i = members.length - 1; i >= 0; i--) {
				if (i != mrho.index) {
					priority.add(serversMap.get(members[i].address));
				}
			}
		} else {
			for (int i = members.length - 1; i >= 0; i--) {
				priority.add(serversMap.get(members[i].address));
			}
		}

		StringBuilder sb = new StringBuilder();
		sb.append("Priority order: ");
		boolean first = true;
		for (ClockServerProxy cs : priority) {
			if (first) {
				first = false;
			} else {
				sb.append(", ");
			}
			sb.append(cs.address);
		}

		logger.info(sb.toString());
	}

	void processSyncResponse(long sent, long now, long c, long w) {
		ClockTriple s = ClockTriple.sync(sent, now, c, w);
		if (certainReading == null) {
			logger.info("Started clock at " + s);
			certainReading = s;
		} else {
			ClockTriple ct = certainReading.advanceGrowing(now);
			if (ct.isImprovedBy(s)) {
				certainReading = ct.intersection(s);
			}
		}
	}

	ClockTriple readClock(long now) {
		ClockTriple ct = certainReading;
		return ct == null ? null : ct.advanceGrowing(now);
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
}
