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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClockServer {

    static class RelinquishLease {
        final Address address; // Address of clock server that gave the lease to us.
        final RoundId round; // The round that the lease was given for.
        final long expires; // At any rate the lease will have expired by this local time.

        public RelinquishLease(Address address, RoundId round, long expires) {
            this.address = address;
            this.round = round;
            this.expires = expires;
        }
    }

    static class Neighbor {
        final Address address;

        // A lower bound (e.g. speed of light) for the time it takes to send a message to/from this neighbor.
        long minLatency = 0;

        // Invariant: communicateFuture != null <=> neighbor is in currentCommunicationSet.
        ScheduledFuture<?> communicateFuture = null;

        int restarts = 0;
        int nextUnknownCfg = 0;

        RoundId mrho = RoundId.ZERO;
        RoundId arbb = RoundId.ZERO;
        RoundId currentRound = RoundId.ZERO;

        long leaseExpiresEarliest = 0;

        long lastResponseReceived = 0;
        boolean reachable = false;

        boolean isReachable(long now) {
            return now - lastResponseReceived < TimeUnit.SECONDS.toNanos(2);
        }

        public Neighbor(Address address) {
            this.address = address;
        }
    }

    // The lease length should be a configuration parameter.
    static final long LEASE_LENGTH = TimeUnit.SECONDS.toNanos(10);

    final Logger logger = LoggerFactory.getLogger(ClockServer.class);

    Address myAddress;
    PersistentStorage persistentStorage;
    LoggingScheduledThreadPoolExecutor executor;
    DatagramChannel ch;
    Selector sel;
    Thread selectThread;
    volatile boolean shuttingDown = false;
    volatile boolean dropMessages = false;

    final HashMap<InetSocketAddress, Neighbor> neighbors = new HashMap<>();
    final HashSet<Address> currentCommunicationSet = new HashSet<>();
    final HashMap<Address, RelinquishLease> relinquishMap = new HashMap<>();

    int restarts = 0;

    int maxCfgHeardOf = 0;
    final ArrayList<Configuration> configurations = new ArrayList<>();

    RoundId mrho = RoundId.ZERO; // Set this as current round when lease expires.
    RoundId arbb = RoundId.ZERO; // All rounds below this round are blocked.
    RoundId currentRound = RoundId.ZERO;
    RoundId maxCurrentRoundWriteStarted = RoundId.ZERO;

    long leaseExpires = 0;
    long unstablePeriodEnds = 0;

    boolean myMRHO = false; // True if I know that MRHO is my round (I started it).

    ScheduledFuture<?> leaseExpiresTimer = null;
    ScheduledFuture<?> startNewRoundTimer = null;

    ClockTriple certainReading = null;
    ClockTriple latentSynchronization = null;

    MinLatencyMap latencyMap = null;

    void init(InetSocketAddress address, String filename) {

        myAddress = new Address(address); // Throws exception if address is unresolved.

        logger.info("Starting clock server using address {}.", myAddress);

        executor = new LoggingScheduledThreadPoolExecutor(1, logger);

        persistentStorage = new PersistentStorage();
        persistentStorage.init(filename, myAddress);

        if (persistentStorage.restarts != 0) {
            restarts = persistentStorage.restarts;
            currentRound = persistentStorage.currentRound;

            mrho = currentRound;
            maxCurrentRoundWriteStarted = currentRound;
            maxCfgHeardOf = currentRound.cfgId;

            // This prevents me from updating current round while I had, during last restart, given a lease.
            long now = System.nanoTime();
            leaseExpires = now + ClockTriple.skewMax(LEASE_LENGTH);
        }

        latencyMap = new MinLatencyMap(myAddress);

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

        persistentStorage.shutdown();

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
        for (Neighbor neighbor : neighbors.values()) {
            if (neighbor.communicateFuture != null) {
                neighbor.communicateFuture.cancel(false);
                neighbor.communicateFuture = null;
            }
        }

        if (leaseExpiresTimer != null) {
            leaseExpiresTimer.cancel(false);
            leaseExpiresTimer = null;
        }

        if (startNewRoundTimer != null) {
            startNewRoundTimer.cancel(false);
            startNewRoundTimer = null;
        }
    }

    void rescheduleCommunications() {
        for (Address address : currentCommunicationSet) {
            Neighbor neighbor = neighbors.get(address.address);
            neighbor.communicateFuture.cancel(false);
            neighbor.communicateFuture = executor.schedule(() -> communicateTimeout(neighbor), 0, TimeUnit.SECONDS);
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

    void communicateTimeout(Neighbor neighbor) {
        long now = System.nanoTime();

        RelinquishLease rl = relinquishMap.get(neighbor.address);
        if (rl != null) {
            if (now - rl.expires >= 0) {
                relinquishMap.remove(rl.address);
                rl = null;
            }

            updateCommunicationSet(now);

            if (neighbor.communicateFuture == null) {
                return;
            }
        }

        boolean reachable = neighbor.isReachable(now);
        if (neighbor.reachable && !reachable) {
            neighbor.reachable = false;
            logger.info("ClockServer " + neighbor.address + " became unreachable");
            updateUnstablePeriod(now);
        }

        MultipartRequest mreq = new MultipartRequest();

        if (neighbor.mrho.compareTo(mrho) < 0) {
            mreq.mrho = mrho;
        }

        if (neighbor.arbb.compareTo(arbb) < 0) {
            mreq.arbb = arbb;
        }

        mreq.getRestarts = true;
        mreq.getMRHO = true;
        mreq.getARBB = true;
        mreq.getNextUnknownCfg = true;

        // Always wants to synchronize clock, even when I'm the leader.
        // To add: don't ask to synchronize if I'm the leader and my error is zero; my error can never improve from that.

        mreq.readClock = true;
        mreq.sent = now;

        if (reachable) {
            if (rl != null) {
                mreq.relinquishLease = rl.round;
            }

            if (neighbor.nextUnknownCfg < configurations.size()) {
                Configuration cfg = configurations.get(neighbor.nextUnknownCfg);
                mreq.nextUnknownCfg = cfg;
            }

            if (myMRHO) {
                if (arbb.equals(mrho)) {
                    Configuration cfg = configurations.get(mrho.cfgId);
                    if (cfg.contains(neighbor.address)) {
                        // If this neighbor is in r then ask neighbor to set CR=r and give me a lease for 10 seconds.
                        mreq.getLease = mrho;
                        mreq.sent = now;
                    }
                } else {
                    // Send to every neighbor in my interest set.
                    mreq.getCR = true;
                }
            }
        }

        ByteBuffer req = ByteBuffer.allocate(1000);
        req.put(Message.MSG_MULTIPART_REQ);
        MultipartRequest.serialize(mreq, req);
        req.flip();
        sendDatagram(neighbor.address.address, req);

        // Reschedule communication.
        neighbor.communicateFuture = executor.schedule(() -> communicateTimeout(neighbor), 1, TimeUnit.SECONDS);
    }

    void processMessage(InetSocketAddress address, ByteBuffer msg) {
        if (dropMessages || shuttingDown) {
            return;
        }

        byte kind = msg.get();
        if (kind == Message.MSG_MULTIPART_REQ) {
            processMultipartRequest(address, msg);
        } else if (kind == Message.MSG_MULTIPART_RES) {
            processMultipartResponse(address, msg);
        }
    }

    void processMultipartRequest(InetSocketAddress address, ByteBuffer req) {
        long now = System.nanoTime();

        MultipartRequest mreq = MultipartRequest.deserialize(req);

        boolean checkCommunicationSetUpdated = false;
        boolean rescheduleAll = false;

        MultipartResponse mres = new MultipartResponse();

        // Update max configuration heard of.
        if (mreq.mrho != null && maxCfgHeardOf < mreq.mrho.cfgId) {
            maxCfgHeardOf = mreq.mrho.cfgId;
            checkCommunicationSetUpdated = true;
        }

        if (mreq.arbb != null && maxCfgHeardOf < mreq.arbb.cfgId) {
            maxCfgHeardOf = mreq.arbb.cfgId;
            checkCommunicationSetUpdated = true;
        }

        if (mreq.nextUnknownCfg != null && maxCfgHeardOf < mreq.nextUnknownCfg.cfgId) {
            maxCfgHeardOf = mreq.nextUnknownCfg.cfgId;
            checkCommunicationSetUpdated = true;
        }

        // Update max round heard of.
        if (mreq.mrho != null && mrho.compareTo(mreq.mrho) < 0) {
            logger.info("Max round heard of updated from " + mrho + " to " + mreq.mrho + " because multipart request from " + address);

            if (myMRHO) {
                logger.info("Abandoning my round " + mrho);
                if (arbb.equals(mrho)) {
                    certainReading = readClock(now);
                    latentSynchronization = null;
                }
                updateRelinquishMap(now);
                checkCommunicationSetUpdated = true;
                myMRHO = false;
            }

            mrho = mreq.mrho;

            // I heard about a round that I started from someone else.
            // This can only happen after a restart.
            if (mrho.cfgId < configurations.size() && mrho.index == configurations.get(mrho.cfgId).indexOf(myAddress)) {
                myMRHO = true;
            }

            updateCurrentRoundOrSetLeaseTimer(now);
        }

        // Update max round such that all rounds below are blocked.
        if (mreq.arbb != null && arbb.compareTo(mreq.arbb) < 0) {
            logger.info("ARBB updated from " + arbb + " to " + mreq.arbb + " because multipart request from " + address);

            // I found out from someone else that this round that I started is ARBB.
            // This can only happen after a restart.
            if (myMRHO && mreq.arbb.equals(mrho) && certainReading != null) {
                certainReading = readClock(now);
            }

            arbb = mreq.arbb;
            checkCommunicationSetUpdated = true;
            rescheduleAll = true;
        }

        // Update configurations with known members.
        if (mreq.nextUnknownCfg != null && mreq.nextUnknownCfg.cfgId == configurations.size()) {
            Configuration cfg = mreq.nextUnknownCfg;

            logger.info("Adding configuration " + cfg);

            configurations.add(cfg);

            for (Address m : cfg.members) {
                if (!m.equals(myAddress) && !neighbors.containsKey(m.address)) {
                    Neighbor neighbor = new Neighbor(m);
                    neighbor.minLatency = latencyMap.getMinLatency(m);
                    neighbors.put(m.address, neighbor);
                }
            }

            if (cfg.cfgId == currentRound.cfgId && cfg.indexOf(myAddress) == currentRound.index) {
                // I didn't know that the current round was started by me.
                // This can only happen after a restart.
                leaseExpires = now;

                if (leaseExpiresTimer != null) {
                    leaseExpiresTimer.cancel(false);
                    leaseExpiresTimer = null;
                }

                if (currentRound.compareTo(mrho) < 0) {
                    updateCurrentRoundLeaseHasExpired();
                }
            }

            if (cfg.cfgId == mrho.cfgId && cfg.indexOf(myAddress) == mrho.index) {
                // I just found out that MRHO was started by me.
                // This can only happen after a restart.
                if (arbb.equals(mrho) && certainReading != null) {
                    certainReading = readClock(now);
                }

                myMRHO = true;

                if (cfg.cfgId == 0 && restarts == 0) {
                    certainReading = new ClockTriple(now, 0, 0); // Could start clock at something else than c = 0.
                    logger.info("Started clock at {}", certainReading);
                }
            }

            if (startNewRoundTimer == null) {
                startNewRoundTimer = executor.schedule(this::maybeStartNewRoundTimeout, 0, TimeUnit.SECONDS);
            }

            checkCommunicationSetUpdated = true;
            rescheduleAll = true;
        }

        if (mreq.relinquishLease != null) {
            logger.info("Received reqlinquish lease request for round {} from {}", mreq.relinquishLease, address);

            if (currentRound.equals(mreq.relinquishLease)) {
                leaseExpires = now;

                if (leaseExpiresTimer != null) {
                    leaseExpiresTimer.cancel(false);
                    leaseExpiresTimer = null;
                }

                // Invariant: at this point, current round < mrho, since we got this request
                // from the leader of current round, and it has abandoned its round for a higher mrho.
                assert currentRound.compareTo(mrho) < 0;

                updateCurrentRoundLeaseHasExpired();
            }

            mres.relinquishedLease = mreq.relinquishLease;
        }

        if (mreq.getLease != null) {
            // The max round heard of is always set before we get here.
            // If the previous lease had expired then the current round would have been updated already.
            // Otherwise the current round is less than this, and we cannot give a lease.

            if (currentRound.equals(mreq.getLease)) {
                leaseExpires = now + ClockTriple.skewMax(LEASE_LENGTH);

                mres.gaveLease = mreq.getLease;
                mres.reqSent = mreq.sent;
            }
        }

        if (mreq.getMRHO) {
            mres.mrho = mrho;
        }

        if (mreq.getCR) {
            mres.cr = currentRound;
        }

        if (mreq.getARBB) {
            mres.arbb = arbb;
        }

        if (mreq.getRestarts) {
            mres.containsRestarts = true;
            mres.restarts = restarts;
        }

        if (mreq.getNextUnknownCfg) {
            mres.containsNextUnknownCfgId = true;
            mres.nextUnknownCfgId = configurations.size();
        }

        if (mreq.readClock && certainReading != null) {
            ClockTriple ct = readClock(now);

            mres.containsClock = true;
            mres.reqSent = mreq.sent;
            mres.c = ct.c;
            mres.e = ct.e;
        }

        ByteBuffer res = ByteBuffer.allocate(1000);
        res.put(Message.MSG_MULTIPART_RES);
        MultipartResponse.serialize(mres, res);
        res.flip();
        sendDatagram(address, res);

        if (checkCommunicationSetUpdated) {
            updateCommunicationSet(now);
        }

        if (rescheduleAll) {
            rescheduleCommunications();
        }
    }

    void processMultipartResponse(InetSocketAddress address, ByteBuffer res) {
        Neighbor neighbor = neighbors.get(address);
        if (neighbor == null) {
            return;
        }

        long now = System.nanoTime();

        MultipartResponse mres = MultipartResponse.deserialize(res);

        boolean checkCommunicationSetUpdated = false;
        boolean rescheduleAll = false;

        neighbor.lastResponseReceived = now;

        if (!neighbor.reachable) {
            logger.info("ClockServer " + neighbor.address + " became reachable");
            neighbor.reachable = true;
            updateUnstablePeriod(now);
        }

        // Update my neighbor state information.
        if (mres.containsRestarts && neighbor.restarts < mres.restarts) {
            neighbor.restarts = mres.restarts;

            neighbor.mrho = RoundId.ZERO;
            neighbor.arbb = RoundId.ZERO;
            neighbor.nextUnknownCfg = 0;
        }

        if (mres.mrho != null && neighbor.mrho.compareTo(mres.mrho) < 0) {
            neighbor.mrho = mres.mrho;
        }

        if (mres.arbb != null && neighbor.arbb.compareTo(mres.arbb) < 0) {
            neighbor.arbb = mres.arbb;
        }

        if (mres.containsNextUnknownCfgId && neighbor.nextUnknownCfg < mres.nextUnknownCfgId) {
            neighbor.nextUnknownCfg = mres.nextUnknownCfgId;
        }

        boolean neighborCrUpdated = false;
        if (mres.cr != null && neighbor.currentRound.compareTo(mres.cr) < 0) {
            neighbor.currentRound = mres.cr;
            neighborCrUpdated = true;
        }

        if (mres.gaveLease != null && neighbor.currentRound.compareTo(mres.gaveLease) < 0) {
            neighbor.currentRound = mres.gaveLease;
            neighborCrUpdated = true;
        }

        // Update max configuration heard of.
        if (mres.mrho != null && maxCfgHeardOf < mres.mrho.cfgId) {
            maxCfgHeardOf = mres.mrho.cfgId;
            checkCommunicationSetUpdated = true;
        }

        if (mres.arbb != null && maxCfgHeardOf < mres.arbb.cfgId) {
            maxCfgHeardOf = mres.arbb.cfgId;
            checkCommunicationSetUpdated = true;
        }

        // Update max round heard of.
        if (mres.mrho != null && mrho.compareTo(mres.mrho) < 0) {
            logger.info("Max round heard of updated from " + mrho + " to " + mres.mrho + " because multipart response from " + address);

            if (myMRHO) {
                logger.info("Abandoning my round " + mrho);
                if (arbb.equals(mrho)) {
                    certainReading = readClock(now);
                    latentSynchronization = null;
                }
                updateRelinquishMap(now);
                checkCommunicationSetUpdated = true;
                myMRHO = false;
            }

            mrho = mres.mrho;

            if (mrho.cfgId < configurations.size() && mrho.index == configurations.get(mrho.cfgId).indexOf(myAddress)) {
                myMRHO = true;
            }

            updateCurrentRoundOrSetLeaseTimer(now);
        }

        if (mres.arbb != null && arbb.compareTo(mres.arbb) < 0) {
            logger.info("ARBB updated from " + arbb + " to " + mres.arbb + " because multipart response from " + address);

            // I found out from someone else that this round that I started is ARBB.
            // This can only happen after a restart.
            if (myMRHO && mres.arbb.equals(mrho) && certainReading != null) {
                certainReading = readClock(now);
            }

            arbb = mres.arbb;
            checkCommunicationSetUpdated = true;
            rescheduleAll = true;
        }

        if (mres.containsClock) {
            processSyncResponse(mres.reqSent, now, mres.c, mres.e, neighbor.minLatency);
        }

        if (neighborCrUpdated && myMRHO && !arbb.equals(mrho) && checkArbb()) {
            logger.info("ARBB updated from " + arbb + " to " + mrho + " because a block quorum have set the round as the current round");
            certainReading = readClock(now);
            arbb = mrho;
            checkCommunicationSetUpdated = true;
            rescheduleAll = true;
        }

        if (mres.gaveLease != null && myMRHO && mres.gaveLease.equals(mrho)) {
            long expires = mres.reqSent + ClockTriple.skewMin(neighbor.minLatency) + ClockTriple.skewMin(LEASE_LENGTH);
            if (neighbor.leaseExpiresEarliest < expires) {
                neighbor.leaseExpiresEarliest = expires;
            }

            // This could trigger that NQB(r) is updated, but there is no need to do
            // anything about that for now.
        }

        if (mres.relinquishedLease != null) {
            RelinquishLease rl = relinquishMap.get(neighbor.address);
            if (rl != null && rl.round.equals(mres.relinquishedLease)) {
                relinquishMap.remove(neighbor.address);
                checkCommunicationSetUpdated = true;
            }
        }

        if (checkCommunicationSetUpdated) {
            updateCommunicationSet(now);
        }

        if (rescheduleAll) {
            rescheduleCommunications();
        }

        if (!rescheduleAll && neighbor.communicateFuture != null) {
            boolean reschedule = neighbor.nextUnknownCfg < configurations.size()
                    || neighbor.mrho.compareTo(mrho) < 0
                    || neighbor.arbb.compareTo(arbb) < 0;

            if (reschedule) {
                neighbor.communicateFuture.cancel(false);
                neighbor.communicateFuture = executor.schedule(() -> communicateTimeout(neighbor), 0, TimeUnit.SECONDS);
            }
        }
    }

    boolean checkArbb() {
        assert myMRHO && !arbb.equals(mrho);

        // The coordinator enforces an invariant that before a new configuration is
        // started
        // the previous (the most recent) configuration must have been blocked.
        // This CS will know about this, because the ARBB information is always sent
        // together
        // with the configuration, so the following assertion is guaranteed to hold.

        assert arbb.cfgId >= mrho.cfgId - 1;

        if (arbb.cfgId == mrho.cfgId - 1) {
            int count = 0;

            Configuration prevCfg = configurations.get(mrho.cfgId - 1);
            for (Address address : prevCfg.members) {
                if (address.equals(myAddress)) {
                    if (currentRound.equals(mrho)) {
                        count++;
                    }
                } else {
                    Neighbor neighbor = neighbors.get(address.address);
                    if (neighbor.currentRound.equals(mrho)) {
                        count++;
                    }
                }
            }

            if (count < prevCfg.members.length / 2 + 1) {
                return false;
            }
        }

        int count = 0;

        Address[] members = configurations.get(mrho.cfgId).members;
        for (Address address : members) {
            if (address.equals(myAddress)) {
                if (currentRound.equals(mrho)) {
                    count++;
                }
            } else {
                Neighbor neighbor = neighbors.get(address.address);
                if (neighbor.currentRound.equals(mrho)) {
                    count++;
                }
            }
        }

        if (count < members.length / 2 + 1) {
            return false;
        }

        return true;
    }

    void updateRelinquishMap(long now) {
        relinquishMap.clear();

        Configuration cfg = configurations.get(mrho.cfgId);
        for (Address address : cfg.members) {
            if (!address.equals(myAddress)) {
                Neighbor neighbor = neighbors.get(address.address);
                if (neighbor.currentRound.equals(mrho) && now - neighbor.leaseExpiresEarliest < 0) {
                    RelinquishLease rl = new RelinquishLease(neighbor.address, mrho, neighbor.leaseExpiresEarliest);
                    relinquishMap.put(rl.address, rl);
                }
                neighbor.leaseExpiresEarliest = 0;
            }
        }
    }

    void updateCurrentRoundOrSetLeaseTimer(long now) {
        if (leaseExpiresTimer == null) {
            if (now - leaseExpires >= 0) {
                updateCurrentRoundLeaseHasExpired();
            } else {
                leaseExpiresTimer = executor.schedule(this::leaseExpiredTimeout, leaseExpires - now, TimeUnit.NANOSECONDS);
            }
        }
    }

    void leaseExpiredTimeout() {
        leaseExpiresTimer = null;

        long now = System.nanoTime();

        // The lease timer is only started if currentRound < MRHO.
        // currentRound cannot increase until the lease has expired.
        // If the lease timer is set then currentRound is not updated until the lease
        // timer times out.
        // Hence, when we get here the following invariant must hold.

        assert currentRound.compareTo(mrho) < 0;

        // A lease is not given if current round is not equal to MRHO.
        // Current round is not updated to MRHO until the previous lease expires.
        // The timer is set to trigger at a time after the lease expires.
        // Hence, the following invariant must hold.

        assert now - leaseExpires >= 0;

        updateCurrentRoundLeaseHasExpired();
    }

    void updateCurrentRoundLeaseHasExpired() {
        if (maxCurrentRoundWriteStarted.compareTo(mrho) < 0) {
            persistentStorage.updateCurrentRound(mrho, round -> {
                executor.submit(() -> currentRoundWriteCompleted(round));
            });
            maxCurrentRoundWriteStarted = mrho;
        }
    }

    void currentRoundWriteCompleted(RoundId round) {
        if (shuttingDown) {
            return;
        }

        logger.info("Current round {} write completed", round);

        long now = System.nanoTime();

        assert currentRound.compareTo(round) < 0;

        currentRound = round;

        // This could trigger ARBB for my round.
        if (myMRHO) {
            if (!arbb.equals(mrho) && checkArbb()) {
                logger.info("ARBB updated from " + arbb + " to " + mrho + " because a block quorum have set the round as the current round");
                certainReading = readClock(now);
                arbb = mrho;
                updateCommunicationSet(now);
                rescheduleCommunications();
            }
        } else {
            // Maybe send a message to the leader of the round if it isn't me?
            // This is probably not worth it though; the leader will try to contact us again
            // within one second.
        }
    }

    void updateUnstablePeriod(long now) {
        logger.info("Updated unstable period ends in 3 seconds");
        unstablePeriodEnds = Math.max(unstablePeriodEnds, now + TimeUnit.SECONDS.toNanos(3));
    }

    void processSyncResponse(long sent, long now, long c, long e, long latency) {
        ClockTriple s = ClockTriple.sync(sent, now, c, e, latency);

        if (certainReading == null) {
            logger.info("Started clock at " + s);
            certainReading = s;
        } else {
            if (myMRHO && arbb.equals(mrho)) {
                long nqb = getHighestNqb(now);
    
                if (latentSynchronization != null && latentSynchronization.h <= nqb) {
                    ClockTriple ct = certainReading.advanceShrinking(latentSynchronization.h);
                    if (ct.isImprovedBy(latentSynchronization)) {
                        certainReading = ct.intersection(latentSynchronization);
                    }
                    latentSynchronization = null;
                }
    
                if (nqb >= now) {
                    ClockTriple ct = certainReading.advanceShrinking(now);
                    if (ct.isImprovedBy(s)) {
                        certainReading = ct.intersection(s);
                    }
                } else {
                    if (latentSynchronization != null) {
                        ClockTriple ct = latentSynchronization.advanceGrowing(now);
                        if (ct.isImprovedBy(s)) {
                            latentSynchronization = ct.intersection(s);
                        }
                    } else {
                        latentSynchronization = s;
                    }
                }
            } else {
                ClockTriple ct = certainReading.advanceGrowing(now);
                if (ct.isImprovedBy(s)) {
                    certainReading = ct.intersection(s);
                }
            }
        }
    }

    ClockTriple readClock(long now) {
        if (now < certainReading.h) {
            throw new RuntimeException("The clock has been adjusted to a time in the future");
        }

        if (!(myMRHO && arbb.equals(mrho))) {
            return certainReading.advanceGrowing(now);
        }

        long nqb = getHighestNqb(now);

        if (latentSynchronization != null && latentSynchronization.h <= nqb) {
            ClockTriple ct = certainReading.advanceShrinking(latentSynchronization.h);
            if (ct.isImprovedBy(latentSynchronization)) {
                certainReading = ct.intersection(latentSynchronization);
            }
            latentSynchronization = null;
        }

        if (nqb >= now) {
            return certainReading.advanceShrinking(now);
        } else if (nqb <= certainReading.h) {
            ClockTriple ct = certainReading.advanceGrowing(now);
            if (latentSynchronization != null) {
                ct = ct.intersection(latentSynchronization.advanceGrowing(now));
            }
            return ct;
        } else {
            ClockTriple ct = certainReading.advanceShrinking(nqb).advanceGrowing(now);
            if (latentSynchronization != null) {
                ct = ct.intersection(latentSynchronization.advanceGrowing(now));
            }
            return ct;
        }
    }

    long getHighestNqb(long now) {
        Address[] members = configurations.get(mrho.cfgId).members;

        long[] nb = new long[members.length];

        for (int i = 0; i < members.length; i++) {
            Address address = members[i];
            if (address.equals(myAddress)) {
                nb[i] = currentRound.equals(mrho) ? now : 0L; // Should be infinity instead of now.
            } else {
                Neighbor neighbor = neighbors.get(address.address);
                nb[i] = neighbor.currentRound.equals(mrho) ? neighbor.leaseExpiresEarliest : 0L;
            }
        }

        Arrays.sort(nb);
        return nb[members.length / 2];
    }

    HashSet<Address> getCommunicationSet() {
        HashSet<Address> communicationSet = new HashSet<>();

        boolean inLatest = false;
        if (maxCfgHeardOf < configurations.size()) {
            Configuration cfg = configurations.get(maxCfgHeardOf);
            inLatest = cfg.contains(myAddress);
            communicationSet.addAll(Arrays.asList(cfg.members));
        }

        if (arbb.cfgId < maxCfgHeardOf && maxCfgHeardOf - 1 < configurations.size()) {
            Configuration penultimateCfg = configurations.get(maxCfgHeardOf - 1);

            boolean inPenultimate = penultimateCfg.contains(myAddress);

            if (inLatest || inPenultimate) {
                communicationSet.addAll(Arrays.asList(penultimateCfg.members));
            }
        }

        communicationSet.addAll(relinquishMap.keySet());
        communicationSet.remove(myAddress);
        return communicationSet;
    }

    void updateCommunicationSet(long now) {
        HashSet<Address> communicationSet = getCommunicationSet();

        if (!communicationSet.equals(currentCommunicationSet)) {
            HashSet<Address> toAdd = new HashSet<>(communicationSet);
            toAdd.removeAll(currentCommunicationSet);

            HashSet<Address> toRemove = new HashSet<>(currentCommunicationSet);
            toRemove.removeAll(communicationSet);

            if (!toAdd.isEmpty()) {
                updateUnstablePeriod(now);
            }

            for (Address address : toRemove) {
                logger.info("Removed {} from communication set", address);
                currentCommunicationSet.remove(address);

                Neighbor neighbor = neighbors.get(address.address);
                neighbor.communicateFuture.cancel(false);
                neighbor.communicateFuture = null;
            }

            for (Address address : toAdd) {
                // Update reachability to avoid an unnecessary reachable -> unreachable -> reachable triggering sequence.
                Neighbor neighbor = neighbors.get(address.address);
                neighbor.reachable = neighbor.isReachable(now);

                logger.info("Added {} to communication set", address);
                currentCommunicationSet.add(address);

                neighbor.communicateFuture = executor.schedule(() -> communicateTimeout(neighbor), 0, TimeUnit.SECONDS);
            }
        }
    }

    boolean shouldStartNewRound(long now) {
        // Is my clock started?
        if (certainReading == null) {
            return false;
        }

        // Am I in a stable period?
        if (now - unstablePeriodEnds < 0) {
            return false;
        }

        // Is the max configuration that I know the members of also the max configuration that I heard of?
        int maxKnownCfgId = configurations.size() - 1;
        if (maxKnownCfgId < maxCfgHeardOf) {
            return false;
        }

        // Am I a member of the max heard of configuration?
        Configuration maxKnownCfg = configurations.get(maxKnownCfgId);
        if (!maxKnownCfg.contains(myAddress)) {
            return false;
        }

        // Is the leader of MRHO unreachable, or is MRHO in a previous configuration?
        Address leader = configurations.get(mrho.cfgId).members[mrho.index];
        boolean leaderReachable = true;
        if (!leader.equals(myAddress)) {
            Neighbor neighbor = neighbors.get(leader.address);
            leaderReachable = neighbor.isReachable(now);
        }

        if (!(!leaderReachable || mrho.cfgId < maxKnownCfgId)) {
            return false;
        }

        // Is lease expired, or, the lease has not expired but it is given to a round in a previous configuration and no round has been started in this configuration?
        if (!(now - leaseExpires >= 0 || mrho.cfgId < maxKnownCfgId)) {
            return false;
        }

        // Is ARBB in this configuration, or a block quorum of members in previous configuration are reachable?
        if (arbb.cfgId < maxKnownCfgId) {
            int count = 0;
            Configuration prevCfg = configurations.get(maxKnownCfgId - 1);
            for (Address address : prevCfg.members) {
                if (address.equals(myAddress)) {
                    count++;
                } else {
                    Neighbor neighbor = neighbors.get(address.address);
                    if (neighbor.isReachable(now)) {
                        count++;
                    }
                }
            }

            if (count < prevCfg.members.length / 2 + 1) {
                return false;
            }
        }

        // Is a block quorum of members of this configuration reachable, and I have the best priority?
        int count = 0;
        boolean bestPriority = true;
        int myIndex = maxKnownCfg.indexOf(myAddress);
        for (int i = 0; i < maxKnownCfg.members.length; i++) {
            Address address = maxKnownCfg.members[i];
            if (address.equals(myAddress)) {
                count++;
            } else {
                Neighbor neighbor = neighbors.get(address.address);
                if (neighbor.isReachable(now)) {
                    count++;
                    if (myIndex < i) {
                        bestPriority = false;
                    }
                }
            }
        }

        if (count < maxKnownCfg.members.length / 2 + 1 || !bestPriority) {
            return false;
        }

        return true;
    }

    void maybeStartNewRoundTimeout() {
        startNewRoundTimer = null;

        long now = System.nanoTime();

        logStatusMessage(now);

        if (shouldStartNewRound(now)) {
            int cfgId = configurations.size() - 1;
            Configuration cfg = configurations.get(cfgId);

            RoundId startedRound = new RoundId(cfgId, mrho.lt + 1, cfg.indexOf(myAddress));

            logger.info("Starting new round " + startedRound);

            if (myMRHO) {
                logger.info("Abandoning my round " + mrho);
                if (arbb.equals(mrho)) {
                    certainReading = readClock(now);
                    latentSynchronization = null;
                }
                updateRelinquishMap(now);
                myMRHO = false;
            }

            mrho = startedRound;
            myMRHO = true;

            updateCurrentRoundOrSetLeaseTimer(now);

            updateCommunicationSet(now);
            rescheduleCommunications();
        }

        startNewRoundTimer = executor.schedule(this::maybeStartNewRoundTimeout, 1, TimeUnit.SECONDS);
    }

    void logStatusMessage(long now) {
        StringBuilder sb = new StringBuilder();
        sb.append("Status: MRHO = " + mrho + (myMRHO ? " (my round)" : "") + ", CR = " + currentRound + ", ARBB = " + arbb);

        boolean steering = false;
        if (myMRHO && arbb.equals(mrho)) {
            long nqb = getHighestNqb(now);
            if (nqb >= now) {
                steering = true;
            }
        }

        sb.append(steering ? ", steering" : ", not steering");
        sb.append(certainReading == null ? ", clock not started" : (", clock = " + readClock(now)));
        logger.info(sb.toString());
    }

    void sendDatagram(InetSocketAddress address, ByteBuffer src) {
        if (dropMessages || shuttingDown) {
            return;
        }
        actuallySendDatagram(address, src);
        // executor.schedule(() -> actuallySendDatagram(address, src), 90,
        // TimeUnit.MILLISECONDS);
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

    void repl() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
        while (!done) {
            System.out.print("> ");
            try {
                String x = br.readLine();
                if (x.equals("exit")) {
                    done = true;
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

        if (args.length < 2) {
            println("Usage: clockserver <my-address> <persistent-storage-filename>\n   where address has the format host:port");
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

        init(address, args[1]);
        repl();
        shutdown();
    }

    static void println(String x) {
        System.out.println(x);
    }

    public static void main(String[] args) {
        new ClockServer().run(args);
    }
}
