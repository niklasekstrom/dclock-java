package icsync;

public class Util {

	public static boolean arrayContains(Object[] arr, Object o) {
		for (Object e : arr) {
			if (o == null ? e == null : o.equals(e)) {
				return true;
			}
		}
		return false;
	}

	public static int arrayIndexOf(Object[] arr, Object o) {
		for (int i = 0; i < arr.length; i++) {
			Object e = arr[i];
			if (o == null ? e == null : o.equals(e)) {
				return i;
			}
		}
		return -1;
	}

	public static String join(String sep, Object[] os) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Object o : os) {
			if (first) {
				first = false;
			} else {
				sb.append(sep);
			}
			sb.append(o.toString());
		}
		return sb.toString();
	}

	public static String join(String sep, Iterable<?> os) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Object o : os) {
			if (first) {
				first = false;
			} else {
				sb.append(sep);
			}
			sb.append(o.toString());
		}
		return sb.toString();
	}
}
