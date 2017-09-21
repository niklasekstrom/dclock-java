package icsync;

import java.nio.ByteBuffer;
import java.util.Objects;

public class Configuration {
	final int cfgId;
	final Address[] members;

	public Configuration(int cfgId, Address[] members) {
		Objects.requireNonNull(members);
		for (Address e : members) {
			Objects.requireNonNull(e);
		}

		this.cfgId = cfgId;
		this.members = members;
	}

	public boolean contains(Address o) {
		for (Address e : members) {
			if (o == e || o.equals(e)) {
				return true;
			}
		}
		return false;
	}

	public int indexOf(Address o) {
		for (int i = 0; i < members.length; i++) {
			Object e = members[i];
			if (o == e || o.equals(e)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public String toString() {
		return "(" + cfgId + ": " + Util.join(", ", members) + ")";
	}

	public static void serialize(Configuration cfg, ByteBuffer dst) {
		dst.putShort((short) cfg.cfgId);
		dst.put((byte) cfg.members.length);
		for (Address m : cfg.members) {
			Address.serialize(m, dst);
		}
	}

	public static Configuration deserialize(ByteBuffer src) {
		int cfgId = src.getShort();
		int n = src.get();
		Address[] members = new Address[n];
		for (int i = 0; i < n; i++) {
			members[i] = Address.deserialize(src);
		}
		return new Configuration(cfgId, members);
	}
}
