package icsync;

import java.util.concurrent.TimeUnit;

public class ClockTriple {

	static final int RHO = 100; // Parts per million (PPM).
	static final int BETA = 100; // Parts per million (PPM).

	static final long DELTA = 0;
	static final long DELTA_MIN = DELTA * (1000000L - RHO) / 1000000L;

	static final long LEASE = TimeUnit.SECONDS.toNanos(10);
	static final long LEASE_MIN = LEASE * (1000000L - RHO) / 1000000L;
	static final long LEASE_MAX = (LEASE * (1000000L + RHO) + 999999L) / 1000000L;

	final long h;
	final long c;
	final long e;

	public ClockTriple(long h, long c, long e) {
		this.h = h;
		this.c = c;
		this.e = e;
	}

	public ClockTriple advanceGrowing(long h2) {
		long dh = h2 - h;
		if (dh < 0) {
			throw new RuntimeException("Cannot advance to a previous time");
		}
		long de = (dh * (RHO + RHO + BETA) + 999999L) / 1000000L;
		return new ClockTriple(h2, c + dh, e + de);
	}

	public ClockTriple advanceShrinking(long h2) {
		long dh = h2 - h;
		if (dh < 0) {
			throw new RuntimeException("Cannot advance to a previous time");
		}
		long de = (dh * BETA) / 1000000L;
		return new ClockTriple(h2, c + dh, e - de > 0 ? e - de : 0L);
	}

	public boolean isImprovedBy(ClockTriple o) {
		if (h != o.h) {
			throw new RuntimeException("Can only intersect two intervals at the same local time");
		}
		return c - e < o.c - o.e || c + e > o.c + o.e;
	}

	public ClockTriple intersection(ClockTriple o) {
		if (h != o.h) {
			throw new RuntimeException("Can only intersect two intervals at the same local time");
		}

		long l = c - e;
		long u = c + e;

		long ol = o.c - o.e;
		long ou = o.c + o.e;

		if (l >= ol && u <= ou) {
			return this;
		}

		if (ol > l) {
			l = ol;
		}
		if (ou < u) {
			u = ou;
		}
		if (u < l) {
			throw new RuntimeException("Inconsistent clocks");
		}

		u += (u - l) & 1L; // The width u - l must be an even number.

		return new ClockTriple(h, (u + l) / 2, (u - l) / 2);
	}

	public static ClockTriple sync(long sent, long now, long c, long e) {
		long u = (c + e) + ((now - sent - DELTA_MIN) * (1000000L + RHO + RHO + BETA) + 999999L) / 1000000L;
		long l = (c - e) + DELTA_MIN * (1000000L - RHO - RHO - BETA) / 1000000L;
		u += (u - l) & 1L;
		return new ClockTriple(now, (u + l) / 2, (u - l) / 2);
	}

	@Override
	public String toString() {
		return "(" + h + ", " + (c / 1000000000.0) + " s, " + (e / 1000000.0) + " ms)";
	}
}
