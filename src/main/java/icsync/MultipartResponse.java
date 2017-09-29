package icsync;

import java.nio.ByteBuffer;

public class MultipartResponse {

    RoundId mrho = null;
    RoundId cr = null;
    RoundId arbb = null;
    RoundId relinquishedLease = null;
    RoundId gaveLease = null;

    boolean containsNextUnknownCfgId = false;
    int nextUnknownCfgId;

    boolean containsRestarts = false;
    int restarts;

    long reqSent;

    boolean containsClock = false;
    long c;
    long e;

    static final byte MULTIPART_CONTAINS_NEXT_UNKNOWN_CFG = 0;
    static final byte MULTIPART_CONTAINS_MRHO = 1;
    static final byte MULTIPART_CONTAINS_ARBB = 2;
    static final byte MULTIPART_CONTAINS_CURRENT_ROUND = 3;
    static final byte MULTIPART_CONTAINS_RESTARTS = 4;
    static final byte MULTIPART_CONTAINS_CLOCK_READING = 5;
    static final byte MULTIPART_GAVE_10S_LEASE = 6;
    static final byte MULTIPART_RELINQUISHED_LEASE = 7;

    static void serialize(MultipartResponse mres, ByteBuffer dst) {
        // To do: improve the encoding of the message, there is a lot of overhead.

        if (mres.mrho != null) {
            dst.put(MULTIPART_CONTAINS_MRHO);
            RoundId.serialize(mres.mrho, dst);
        }

        if (mres.cr != null) {
            dst.put(MULTIPART_CONTAINS_CURRENT_ROUND);
            RoundId.serialize(mres.cr, dst);
        }

        if (mres.arbb != null) {
            dst.put(MULTIPART_CONTAINS_ARBB);
            RoundId.serialize(mres.arbb, dst);
        }

        if (mres.containsNextUnknownCfgId) {
            dst.put(MULTIPART_CONTAINS_NEXT_UNKNOWN_CFG);
            dst.putShort((short) mres.nextUnknownCfgId);
        }

        if (mres.containsRestarts) {
            dst.put(MULTIPART_CONTAINS_RESTARTS);
            dst.putShort((short) mres.restarts);
        }

        if (mres.containsClock) {
            dst.put(MULTIPART_CONTAINS_CLOCK_READING);
            dst.putLong(mres.reqSent);
            dst.putLong(mres.c);
            dst.putLong(mres.e);
        }

        if (mres.relinquishedLease != null) {
            dst.put(MULTIPART_RELINQUISHED_LEASE);
            RoundId.serialize(mres.relinquishedLease, dst);
        }

        if (mres.gaveLease != null) {
            dst.put(MULTIPART_GAVE_10S_LEASE);
            RoundId.serialize(mres.gaveLease, dst);
            dst.putLong(mres.reqSent);
        }
    }

    static MultipartResponse deserialize(ByteBuffer src) {
        MultipartResponse mres = new MultipartResponse();

        while (src.hasRemaining()) {
            byte part = src.get();
            if (part == MULTIPART_CONTAINS_MRHO) {
                mres.mrho = RoundId.deserialize(src);
            } else if (part == MULTIPART_CONTAINS_CURRENT_ROUND) {
                mres.cr = RoundId.deserialize(src);
            } else if (part == MULTIPART_CONTAINS_ARBB) {
                mres.arbb = RoundId.deserialize(src);
            } else if (part == MULTIPART_CONTAINS_RESTARTS) {
                mres.containsRestarts = true;
                mres.restarts = src.getShort();
            } else if (part == MULTIPART_CONTAINS_NEXT_UNKNOWN_CFG) {
                mres.containsNextUnknownCfgId = true;
                mres.nextUnknownCfgId = src.getShort();
            } else if (part == MULTIPART_CONTAINS_CLOCK_READING) {
                mres.containsClock = true;
                mres.reqSent = src.getLong();
                mres.c = src.getLong();
                mres.e = src.getLong();
            } else if (part == MULTIPART_RELINQUISHED_LEASE) {
                mres.relinquishedLease = RoundId.deserialize(src);
            } else if (part == MULTIPART_GAVE_10S_LEASE) {
                mres.gaveLease = RoundId.deserialize(src);
                mres.reqSent = src.getLong();
            }
        }

        return mres;
    }
}
