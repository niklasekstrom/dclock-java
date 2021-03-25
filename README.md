# Distributed Clock in Java

A fault-tolerant distributed clock (dclock) implemented in Java.

A distributed clock is in many ways similar to a local clock, and in some ways different.
A distributed clock is a distributed object that can be read by any process in the system.
The distributed clock starts at some arbitrary value, and increments by rate r(t) per second, which is bounded by 1-Theta <= r(t) <= 1+Theta,
and where Theta is in the order of 1e-4.

When a process reads the distributed clock, a clock value and an uncertainty is returned, which forms an interval.
The interval is expressed as a pair (c, e) where c is the center and e is the half-width, so that the values covered by the interval are [c-e .. c+e].
The center values of the intervals returned when the clock is read by some particular process are monotonically increasing.

Let (c1, e1) be an interval observed at time t1 by process p1, and let (c2, e2) be an interval observed at time t2 by process p2.
An invariant that is guaranteed by the distributed clock, and that can be used to implement external consistency, is that t1 <= t2 implies that c1-e1 <= c2+e2.
Stated different, if a process reads the clock and the lower bound is l, then if any process later reads the clock then the upper bound is at least l.

This implementation of the distributed clock is fault-tolerant, as a set of clock servers are responsible for keeping the width of the intervals small.
At each point in time, at most one clock server is steering, which is a means to shrink the interval width.
If the clock server that is steering crashes or becomes unreachable for any reason, then another clock server will take over to steer the clock.
The set of clock servers can be reconfigured using a coordinator process.
Clients of the service implement interval clocks by synchronizing periodically against clock servers to avoid that the intervals diverge.

Another implementation of a distributed clock is the Google TrueTime service. Their implementation is not available outside of Google and reportedly requires access to GPS receivers, which is not required by this algorithm.

Things to improve in the short term:
- The clock skew rho and beta should be configurable.

Improvements in a slightly longer term:
- The current design of the coordinator is not fault-tolerant, but it should be straight-forward to make it fault-tolerant.
- If the code gets actual use it can probably be improved a lot.
