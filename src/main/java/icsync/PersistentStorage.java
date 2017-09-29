package icsync;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentStorage {

    final Logger logger = LoggerFactory.getLogger(PersistentStorage.class);

    static class Slot {
        final long lsn;
        final Address address;
        final int restartsCount;
        final RoundId round;

        public Slot(long lsn, Address address, int restartsCount, RoundId round) {
            this.lsn = lsn;
            this.address = address;
            this.restartsCount = restartsCount;
            this.round = round;
        }

        @Override
        public String toString() {
            return String.format("(%s, %s, %s, %s)", lsn, address, restartsCount, round);
        }

        static void serialize(Slot s, ByteBuffer dst) {
            byte[] b = new byte[46];
            ByteBuffer bb = ByteBuffer.wrap(b);

            bb.putInt(0);                           // @0
            bb.putLong(s.lsn);                      // @4
            Address.serialize(s.address, bb);       // @12
            int l = s.address.serializedLength();
            bb.position(bb.position() + (19 - l));
            bb.putInt(s.restartsCount);             // @31
            RoundId.serialize(s.round, bb);         // @35
            bb.putInt(0);                           // @42 -> 46 bytes

            CRC32 checksum = new CRC32();
            checksum.update(b, 4, 38);
            int value = (int) checksum.getValue();

            bb.putInt(0, value);
            bb.putInt(42, value);

            dst.put(b);
        }

        static Slot deserialize(ByteBuffer src) {
            byte[] b = new byte[46];
            src.get(b);

            CRC32 checksum = new CRC32();
            checksum.update(b, 4, 38);
            int value = (int) checksum.getValue();

            ByteBuffer bb = ByteBuffer.wrap(b);

            if (bb.getInt(42) == value && bb.getInt() == value) {
                long lsn = bb.getLong();
                Address address = Address.deserialize(bb);
                int l = address.serializedLength();
                bb.position(bb.position() + (19 - l));
                int restarts = bb.getInt();
                RoundId round = RoundId.deserialize(bb);
                return new Slot(lsn, address, restarts, round);
            }

            return null;
        }
    }

    LoggingScheduledThreadPoolExecutor executor;

    FileChannel fileChannel = null;

    // The max state that is know to be in file.
    Address myAddress = null;
    long nextLsn = 0;
    int restarts = 0;
    RoundId currentRound = RoundId.ZERO;

    void init(String filename, Address myAddress) {
        this.myAddress = myAddress;

        try {
            fileChannel = FileChannel.open(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            logger.error("Unable to open persistent storage file " + filename, e);
            throw new UncheckedIOException(e);
        }

        int size = 0;
        try {
            size = (int) fileChannel.size();
        } catch (IOException e) {
            logger.error("Unable to get persistent storage size", e);
            closeFile();
            throw new UncheckedIOException(e);
        }

        if (size == 0) {
            writeSlot(new Slot(nextLsn++, myAddress, restarts, currentRound));
            writeSlot(new Slot(nextLsn++, myAddress, restarts, currentRound));
        } else {
            ByteBuffer dst = ByteBuffer.allocateDirect(size);
            try {
                fileChannel.read(dst);
            } catch (IOException e) {
                logger.error("Unable to read persistent storage contents", e);
                closeFile();
                throw new UncheckedIOException(e);
            }

            dst.flip();

            Slot s0 = null;
            Slot s1 = null;

            if (dst.remaining() >= 46) {
                s0 = Slot.deserialize(dst);
            }

            if (dst.remaining() >= 46) {
                s1 = Slot.deserialize(dst);
            }

            if (s0 == null && s1 == null) {
                logger.error("Both persistent storage slot 0 and slot 1 were unreadable.\nProbably delete persistent storage file and restart.");
                closeFile();
                throw new RuntimeException("Persistent storage unreadable");
            }

            Slot maxSlot = (s0 != null && (s1 == null || s1.lsn < s0.lsn)) ? s0 : s1;

            if (!myAddress.equals(maxSlot.address)) {
                logger.error("My address {} is not the same as address {} in persistent storage", myAddress, maxSlot.address);
                closeFile();
                throw new RuntimeException("Address is not matching address in persistent storage.");
            }

            nextLsn = maxSlot.lsn + 1;
            restarts = maxSlot.restartsCount + 1;
            currentRound = maxSlot.round;

            logger.info("Clock server restarted, restarts = {}", restarts);

            writeSlot(new Slot(nextLsn++, myAddress, restarts, currentRound));
        }

        executor = new LoggingScheduledThreadPoolExecutor(1, logger);
    }

    void closeFile() {
        if (fileChannel != null) {
            try {
                fileChannel.close();
                fileChannel = null;
            } catch (IOException e1) {}
        }
    }

    void shutdown() {
        // Writes could be running concurrently on executor thread.
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}
        closeFile();
    }

    void writeSlot(Slot s) {
        int slotNum = ((int) s.lsn) & 1;
        ByteBuffer src = ByteBuffer.allocate(46);
        Slot.serialize(s, src);
        src.flip();

        try {
            fileChannel.position(slotNum * 46);
            fileChannel.write(src);
            fileChannel.force(true);
        } catch (IOException e) {
            logger.error("Unable to write to persistent storage file");
            throw new UncheckedIOException(e);
        }
    }

    void updateCurrentRound(RoundId round, Consumer<RoundId> callback) {
        executor.submit(() -> {
            if (currentRound.compareTo(round) < 0) {
                writeSlot(new Slot(nextLsn++, myAddress, restarts, round));
                currentRound = round;
            }
            callback.accept(round);
        });
    }
}
