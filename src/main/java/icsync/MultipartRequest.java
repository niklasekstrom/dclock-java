package icsync;

import java.nio.ByteBuffer;

public class MultipartRequest {

    boolean getMRHO = false;
    boolean getARBB = false;
    boolean getCR = false;
    boolean getRestarts = false;
    boolean getNextUnknownCfg = false;
    boolean readClock = false;

    RoundId mrho = null;
    RoundId arbb = null;
    RoundId getLease = null;
    RoundId relinquishLease = null;
    Configuration nextUnknownCfg = null;

    long sent;

    static final byte GET_FLAG_MRHO = 0x01;
    static final byte GET_FLAG_ARBB = 0x02;
    static final byte GET_FLAG_NEXT_UNKNOWN_CFG = 0x04;
    static final byte GET_FLAG_RESTARTS = 0x08;
    static final byte GET_FLAG_CR = 0x10;

    static final byte MULTIPART_UPDATE_CFG = 0;
    static final byte MULTIPART_UPDATE_ARBB = 1;
    static final byte MULTIPART_UPDATE_MRHO = 2;
    static final byte MULTIPART_GIVE_10S_LEASE = 3;
    static final byte MULTIPART_RELINQUISH_LEASE = 4;
    static final byte MULTIPART_READ_CLOCK = 5;

    static void serialize(MultipartRequest mreq, ByteBuffer dst) {
        // To do: improve the encoding of the message, there is a lot of overhead.

        byte getFlags = 0;
        getFlags |= mreq.getMRHO ? GET_FLAG_MRHO : 0;
        getFlags |= mreq.getARBB ? GET_FLAG_ARBB : 0;
        getFlags |= mreq.getNextUnknownCfg ? GET_FLAG_NEXT_UNKNOWN_CFG : 0;
        getFlags |= mreq.getRestarts ? GET_FLAG_RESTARTS : 0;
        getFlags |= mreq.getCR ? GET_FLAG_CR : 0;
        dst.put(getFlags);

        if (mreq.mrho != null) {
            dst.put(MULTIPART_UPDATE_MRHO);
            RoundId.serialize(mreq.mrho, dst);
        }

        if (mreq.arbb != null) {
            dst.put(MULTIPART_UPDATE_ARBB);
            RoundId.serialize(mreq.arbb, dst);
        }

        if (mreq.nextUnknownCfg != null) {
            dst.put(MULTIPART_UPDATE_CFG);
            Configuration.serialize(mreq.nextUnknownCfg, dst);
        }

        if (mreq.relinquishLease != null) {
            dst.put(MULTIPART_RELINQUISH_LEASE);
            RoundId.serialize(mreq.relinquishLease, dst);
        }

        if (mreq.getLease != null) {
            dst.put(MULTIPART_GIVE_10S_LEASE);
            RoundId.serialize(mreq.getLease, dst);
            dst.putLong(mreq.sent);
        }

        if (mreq.readClock) {
            dst.put(MULTIPART_READ_CLOCK);
            dst.putLong(mreq.sent);
        }
    }

    static MultipartRequest deserialize(ByteBuffer src) {
        MultipartRequest mreq = new MultipartRequest();

        byte getFlags = src.get();
        mreq.getMRHO = (getFlags & GET_FLAG_MRHO) != 0;
        mreq.getARBB = (getFlags & GET_FLAG_ARBB) != 0;
        mreq.getNextUnknownCfg = (getFlags & GET_FLAG_NEXT_UNKNOWN_CFG) != 0;
        mreq.getRestarts = (getFlags & GET_FLAG_RESTARTS) != 0;
        mreq.getCR = (getFlags & GET_FLAG_CR) != 0;

        while (src.hasRemaining()) {
            byte part = src.get();
            if (part == MULTIPART_UPDATE_MRHO) {
                mreq.mrho = RoundId.deserialize(src);
            } else if (part == MULTIPART_UPDATE_ARBB) {
                mreq.arbb = RoundId.deserialize(src);
            } else if (part == MULTIPART_READ_CLOCK) {
                mreq.readClock = true;
                mreq.sent = src.getLong();
            } else if (part == MULTIPART_RELINQUISH_LEASE) {
                mreq.relinquishLease = RoundId.deserialize(src);
            } else if (part == MULTIPART_GIVE_10S_LEASE) {
                mreq.getLease = RoundId.deserialize(src);
                mreq.sent = src.getLong();
            } else if (part == MULTIPART_UPDATE_CFG) {
                mreq.nextUnknownCfg = Configuration.deserialize(src);
            }
        }

        return mreq;
    }
}
