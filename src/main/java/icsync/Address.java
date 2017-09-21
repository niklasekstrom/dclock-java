package icsync;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class Address implements Comparable<Address> {

	final InetSocketAddress address;

	public Address(byte[] addr, int port) {
		try {
			address = new InetSocketAddress(InetAddress.getByAddress(addr), port);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	public Address(InetSocketAddress address) {
		if (address.isUnresolved()) {
			throw new RuntimeException("InetSocketAddress must be resolved");
		}
		this.address = address;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (byte b : address.getAddress().getAddress()) {
			if (first) {
				first = false;
			} else {
				sb.append('.');
			}
			sb.append(b & 0xff);
		}
		sb.append(':');
		sb.append(address.getPort());
		return sb.toString();
	}

	@Override
	public int hashCode() {
		return address.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return obj != null && obj instanceof Address && address.equals(((Address) obj).address);
	}

	public int serializedLength() {
		byte[] b = address.getAddress().getAddress();
		return 1 + b.length + 2;
	}

	public static void serialize(Address address, ByteBuffer dst) {
		byte[] b = address.address.getAddress().getAddress();
		dst.put((byte) b.length);
		dst.put(b);
		dst.putShort((short) address.address.getPort());
	}

	public static Address deserialize(ByteBuffer src) {
		int l = src.get();
		byte[] b = new byte[l];
		src.get(b);
		int port = src.getShort() & 0xffff;
		return new Address(b, port);
	}

	@Override
	public int compareTo(Address o) {
		byte[] b1 = address.getAddress().getAddress();
		byte[] b2 = o.address.getAddress().getAddress();
		int c = 0;
		int l = b1.length < b2.length ? b1.length : b2.length;
		for (int i = 0; i < l; i++)  {
			c = (b1[i] & 0xff) - (b2[i] & 0xff);
			if (c != 0) {
				break;
			}
		}
		if (c == 0) {
			c = b1.length - b2.length;
			if (c == 0) {
				c = address.getPort() - o.address.getPort();
			}
		}
		return c;
	}
}
