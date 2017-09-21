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

	long reqSent;

	boolean containsClock = false;
	long c;
	long e;

	static final byte MULTIPART_UPDATE_CFG_RES = 0;
	static final byte MULTIPART_GIVE_10S_LEASE_RES = 2;
	static final byte MULTIPART_RELINQUISH_LEASE_RES = 3;
	static final byte MULTIPART_READ_CLOCK_RES = 1;
	static final byte MULTIPART_RETURN_MRHO_RES = 4;
	static final byte MULTIPART_RETURN_CURRENT_ROUND_RES = 5;
	static final byte MULTIPART_RETURN_ARBB_RES = 6;

	static void serialize(MultipartResponse mres, ByteBuffer dst) {
		// To do: improve the encoding of the message, there is a lot of overhead.

		if (mres.mrho != null) {
			dst.put(MULTIPART_RETURN_MRHO_RES);
			RoundId.serialize(mres.mrho, dst);
		}

		if (mres.cr != null) {
			dst.put(MULTIPART_RETURN_CURRENT_ROUND_RES);
			RoundId.serialize(mres.cr, dst);
		}

		if (mres.arbb != null) {
			dst.put(MULTIPART_RETURN_ARBB_RES);
			RoundId.serialize(mres.arbb, dst);
		}

		if (mres.containsNextUnknownCfgId) {
			dst.put(MULTIPART_UPDATE_CFG_RES);
			dst.putShort((short) mres.nextUnknownCfgId);
		}

		if (mres.containsClock) {
			dst.put(MULTIPART_READ_CLOCK_RES);
			dst.putLong(mres.reqSent);
			dst.putLong(mres.c);
			dst.putLong(mres.e);
		}

		if (mres.relinquishedLease != null) {
			dst.put(MULTIPART_RELINQUISH_LEASE_RES);
			RoundId.serialize(mres.relinquishedLease, dst);
		}

		if (mres.gaveLease != null) {
			dst.put(MULTIPART_GIVE_10S_LEASE_RES);
			RoundId.serialize(mres.gaveLease, dst);
			dst.putLong(mres.reqSent);
		}
	}

	static MultipartResponse deserialize(ByteBuffer src) {
		MultipartResponse mres = new MultipartResponse();

		while (src.hasRemaining()) {
			byte part = src.get();
			if (part == MULTIPART_RETURN_MRHO_RES) {
				mres.mrho = RoundId.deserialize(src);
			} else if (part == MULTIPART_RETURN_CURRENT_ROUND_RES) {
				mres.cr = RoundId.deserialize(src);
			} else if (part == MULTIPART_RETURN_ARBB_RES) {
				mres.arbb = RoundId.deserialize(src);
			} else if (part == MULTIPART_UPDATE_CFG_RES) {
				mres.containsNextUnknownCfgId = true;
				mres.nextUnknownCfgId = src.getShort();
			} else if (part == MULTIPART_READ_CLOCK_RES) {
				mres.containsClock = true;
				mres.reqSent = src.getLong();
				mres.c = src.getLong();
				mres.e = src.getLong();
			} else if (part == MULTIPART_RELINQUISH_LEASE_RES) {
				mres.relinquishedLease = RoundId.deserialize(src);
			} else if (part == MULTIPART_GIVE_10S_LEASE_RES) {
				mres.gaveLease = RoundId.deserialize(src);
				mres.reqSent = src.getLong();
			}
		}

		return mres;
	}
}
