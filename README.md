# ICSync

Fault-tolerant interval clock synchronization.

An interval clock is a clock that, at each point in time, tells an interval of clock values, instead of a scalar clock value.
The interval is expressed as a pair (c, e) where c is the center and e is the half-width, so that the values covered by the interval are [c-e .. c+e].
Two interval clocks are correct (a.k.a. consistent) if their intervals, when observed at the same real time, overlap (their intersection is nonempty).
The center values may jump back occasionally, but over time the center value will increase within an envelope of real time (bounded skew).

Let (c1, e1) be an interval observed at time t1 on clock C1, and let (c2, e2) be an interval observed at time t2 on clock C2.
An invariant that is guaranteed by a set of interval clocks, and that can be used to implement external consistency, is that t1 <= t2 implies that c1-e1 <= c2+e2.
Stated different, if a clock is read and its lower bound is l, then if any clock is read after that then the upper bound is at least l.

This implementation of interval clocks is fault-tolerant, as a set of clock servers are responsible for keeping the width of the intervals small.
At each point in time, at most one clock server is steering, which is a means to shrink the interval width.
If the clock server that is steering crashes or becomes unreachable for any reason, then another clock server will take over to steer the clock.
The set of clock servers can be reconfigured using a coordinator process.
Clients of the service implement interval clocks by synchronizing periodically against clock servers to avoid that the intervals diverge.

Interval clocks are implemented also by the Google TrueTime service. Their implementation is not available outside of Google and reportedly requires access to GPS receivers, which is not required by ICSync.

Things to improve in the short term:
- There should be a minimal latency matrix, such that if one process synchronizes against another then that minimal latency is taken into account to reduce the uncertainty that contributes to the clock interval width.
- The clock skew rho and beta should be configurable.

Improvements in a slightly longer term:
- The current design of the coordinator is not fault-tolerant, but it should be straight-forward to make it fault-tolerant.
- Clock servers cannot currently be restarted (by design), but that would probably be a useful feature.
- If the code gets actual use it can probably be improved a lot.
