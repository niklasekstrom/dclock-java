package icsync;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinLatencyMap {

	static class Pair {
		final Pattern pattern;
		final long minLatency;

		public Pair(Pattern pattern, long minLatency) {
			this.pattern = pattern;
			this.minLatency = minLatency;
		}
	}

	final Logger logger = LoggerFactory.getLogger(MinLatencyMap.class);

	final Address myAddress;
	final ArrayList<Pair> pairs = new ArrayList<>();

	public MinLatencyMap(Address myAddress) {
		this.myAddress = myAddress;
		load();
	}

	public long getMinLatency(Address address) {
		String ha = address.address.getAddress().getHostAddress();
		for (Pair p : pairs) {
			if (p.pattern.matcher(ha).matches()) {
				return p.minLatency;
			}
		}
		return 0L;
	}

	void load() {
		ArrayList<String> lines = new ArrayList<>();
		try (InputStream is = MinLatencyMap.class.getResourceAsStream("/minLatencyMap.txt")) {
			if (is == null) {
				logger.info("Unable to open minLatencyMap.txt");
				return;
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
			String x = br.readLine();
			while (x != null) {
				lines.add(x.trim());
				x = br.readLine();
			}
		} catch (IOException e) {
			logger.warn("Failed to read minLatencyMap.txt", e);
			return;
		}

		for (String line : lines) {
			if (line.equals("") || line.startsWith("#")) {
				continue;
			}

			String[] parts = line.split("\\s+");
			if (parts.length != 3) {
				logger.warn("Malformed line in minLatencyMap.txt: {}", line);
				continue;
			}

			long minLatency = 0;
			try {
				int ms = Integer.parseInt(parts[2]);
				minLatency = TimeUnit.MILLISECONDS.toNanos(ms);
			} catch (NumberFormatException ex) {
				logger.warn("Malformed line in minLatencyMap.txt: {}", line);
				continue;
			}

			Pattern p1 = Pattern.compile(parts[0]);
			Pattern p2 = Pattern.compile(parts[1]);
			String ha = myAddress.address.getAddress().getHostAddress();
			boolean m1 = p1.matcher(ha).matches();
			boolean m2 = p2.matcher(ha).matches();

			if (m1 && m2) {
				logger.warn("Both the to and from address matches my address, which is an error: " + line);
			} else if (m1) {
				pairs.add(new Pair(p2, minLatency));
			} else if (m2) {
				pairs.add(new Pair(p1, minLatency));
			}
		}

		for (Pair p : pairs) {
			logger.info("Addresses with pattern {} has min latency {} ns", p.pattern, p.minLatency);
		}
	}
}
