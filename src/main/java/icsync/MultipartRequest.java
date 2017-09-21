package icsync;

import java.nio.ByteBuffer;

public class MultipartRequest {

	boolean getMRHO = false;
	boolean getARBB = false;
	boolean getCR = false;
	boolean readClock = false;
	//boolean getNextUnknownConfiguration = false;

	RoundId mrho = null;
	RoundId arbb = null;
	RoundId getLease = null;
	RoundId relinquishLease = null;
	Configuration nextUnknownCfg = null;

	long sent;

	static final byte MULTIPART_UPDATE_CFG_REQ = 0;
	static final byte MULTIPART_UPDATE_ARBB_REQ = 2;
	static final byte MULTIPART_UPDATE_MRHO_REQ = 3;
	static final byte MULTIPART_GIVE_10S_LEASE_REQ = 4;
	static final byte MULTIPART_RELINQUISH_LEASE_REQ = 5;
	static final byte MULTIPART_READ_CLOCK_REQ = 1;
	static final byte MULTIPART_RETURN_MRHO_REQ = 6;
	static final byte MULTIPART_RETURN_CURRENT_ROUND_REQ = 7;
	static final byte MULTIPART_RETURN_ARBB_REQ = 8;

	static void serialize(MultipartRequest mreq, ByteBuffer dst) {
		// To do: improve the encoding of the message, there is a lot of overhead.

		if (mreq.mrho != null) {
			dst.put(MULTIPART_UPDATE_MRHO_REQ);
			RoundId.serialize(mreq.mrho, dst);
		}

		if (mreq.arbb != null) {
			dst.put(MULTIPART_UPDATE_ARBB_REQ);
			RoundId.serialize(mreq.arbb, dst);
		}

		if (mreq.nextUnknownCfg != null) {
			dst.put(MULTIPART_UPDATE_CFG_REQ);
			Configuration.serialize(mreq.nextUnknownCfg, dst);
		}

		if (mreq.relinquishLease != null) {
			dst.put(MULTIPART_RELINQUISH_LEASE_REQ);
			RoundId.serialize(mreq.relinquishLease, dst);
		}

		if (mreq.getLease != null) {
			dst.put(MULTIPART_GIVE_10S_LEASE_REQ);
			RoundId.serialize(mreq.getLease, dst);
			dst.putLong(mreq.sent);
		}

		if (mreq.getMRHO) {
			dst.put(MULTIPART_RETURN_MRHO_REQ);
		}

		if (mreq.getCR) {
			dst.put(MULTIPART_RETURN_CURRENT_ROUND_REQ);
		}

		if (mreq.getARBB) {
			dst.put(MULTIPART_RETURN_ARBB_REQ);
		}

		if (mreq.readClock) {
			dst.put(MULTIPART_READ_CLOCK_REQ);
			dst.putLong(mreq.sent);
		}
	}

	static MultipartRequest deserialize(ByteBuffer src) {
		MultipartRequest mreq = new MultipartRequest();

		while (src.hasRemaining()) {
			byte part = src.get();
			if (part == MULTIPART_UPDATE_MRHO_REQ) {
				mreq.mrho = RoundId.deserialize(src);
			} else if (part == MULTIPART_UPDATE_ARBB_REQ) {
				mreq.arbb = RoundId.deserialize(src);
			} else if (part == MULTIPART_RETURN_MRHO_REQ) {
				mreq.getMRHO = true;
			} else if (part == MULTIPART_RETURN_ARBB_REQ) {
				mreq.getARBB = true;
			} else if (part == MULTIPART_READ_CLOCK_REQ) {
				mreq.readClock = true;
				mreq.sent = src.getLong();
			} else if (part == MULTIPART_RELINQUISH_LEASE_REQ) {
				mreq.relinquishLease = RoundId.deserialize(src);
			} else if (part == MULTIPART_GIVE_10S_LEASE_REQ) {
				mreq.getLease = RoundId.deserialize(src);
				mreq.sent = src.getLong();
			} else if (part == MULTIPART_RETURN_CURRENT_ROUND_REQ) {
				mreq.getCR = true;
			} else if (part == MULTIPART_UPDATE_CFG_REQ) {
				mreq.nextUnknownCfg = Configuration.deserialize(src);
			}
		}

		return mreq;
	}
}
