package icsync;

public class ClockTriple {

	// These default values should be updated from configuration.
	static int MY_RHO = 100;	// The drift for local hardware clock, in parts per million (ppm).
	static int CS_RHO = 100;	// The max drift for any clock server's hardware clock.
	static int BETA = 100;		// The extra drift allowed to the steering clock.

	static long driftMin(long t) {
		return (t * (1000000L - MY_RHO)) / 1000000L;
	}

	static long driftMax(long t) {
		return (t * (1000000L + MY_RHO) + 999999L) / 1000000L;
	}

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
		long de = (dh * (MY_RHO + CS_RHO + BETA) + 999999L) / 1000000L;
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

	public static ClockTriple sync(long sent, long now, long c, long e, long delta) {
		long deltaMin = driftMin(delta);
		long u = (c + e) + ((now - sent - deltaMin) * (1000000L + MY_RHO + MY_RHO + BETA) + 999999L) / 1000000L;
		long l = (c - e) + (deltaMin * (1000000L - MY_RHO - MY_RHO - BETA)) / 1000000L;
		u += (u - l) & 1L;
		return new ClockTriple(now, (u + l) / 2, (u - l) / 2);
	}

	@Override
	public String toString() {
		return "(" + h + ", " + (c / 1000000000.0) + " s, " + (e / 1000000.0) + " ms)";
	}
}
