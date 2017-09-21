package icsync;

import java.nio.ByteBuffer;

public class RoundId implements Comparable<RoundId> {
	final int cfgId;
	final int lt;
	final int index;

	static final RoundId ZERO = new RoundId(0, 0, 0);

	public RoundId(int cfgId, int lt, int index) {
		this.cfgId = cfgId;
		this.lt = lt;
		this.index = index;
	}

	@Override
	public int compareTo(RoundId o) {
		int c = Integer.compare(cfgId, o.cfgId);
		if (c == 0) {
			c = Integer.compare(lt, o.lt);
			if (c == 0) {
				c = Integer.compare(index, o.index);
			}
		}
		return c;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RoundId) {
			RoundId o = (RoundId) obj;
			return cfgId == o.cfgId && lt == o.lt && index == o.index;
		}
		return false;
	}

	@Override
	public String toString() {
		return "(" + cfgId + ", " + lt + ", " + index + ")";
	}

	static void serialize(RoundId o, ByteBuffer dst) {
		dst.putShort((short) o.cfgId);
		dst.putInt(o.lt);
		dst.put((byte) o.index);
	}

	static RoundId deserialize(ByteBuffer src) {
		int cfgId = src.getShort();
		int lt = src.getInt();
		int index = src.get();
		return new RoundId(cfgId, lt, index);
	}
}
