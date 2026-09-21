# macavity

Deterministic, session-local fault injection for PostgreSQL.

> ## ⚠ DESTRUCTIVE — TEST CLUSTERS ONLY
>
> macavity exists to break PostgreSQL on purpose. The `crash` action
> terminates the calling backend with `SIGKILL`; PostgreSQL's postmaster
> responds to any unclean backend exit by **disconnecting every other
> session and running crash recovery**. Never install this on a cluster
> holding data you care about, and never on a production cluster.

## What it is

macavity lets a session *arm* fault events at named execution points, run
normally, and then have those faults injected deterministically:

```sql
SELECT macavity_arm('executor_start', 'error', 3);
 macavity_arm
--------------
            1
-- statement 1: normal
-- statement 2: normal
-- statement 3: ERROR: macavity: injected error at fault point "executor_start"
```

Events are one-shot, counted per session, and reusable. The name is a nod to T. S.
Eliot's mystery cat, who is reliably absent from the scene of the crime —
which is roughly how a fault behaves here: it does its damage and is no
longer armed by the time you look. The evidence, though, is still on the
record, which is the one place the analogy breaks down deliberately.

- each session has its own in-memory **event registry**; **any number of
  events can be armed at once**, each with its own counters and state
- every event has a unique, backend-local integer **`event_id`** (1, 2, 3,
  …), returned by the function that created it
- an event is **`armed`**, **`completed`** (it fired) or **`disarmed`**;
  completed and disarmed events **stay in the registry** and can be
  **reinstated** by ID, which resets their counters
- when several events meet at one hit, they run in the fixed order
  **`delay` > `crash` > `error`**, then ascending `event_id`
- `macavity_status()` lists the whole registry, one row per event, ordered
  by `event_id`: `event_id`, `point`, `action`, `occurrence`, `hits`,
  `remaining`, `state`
- `hits` counts matching fault-point hits; `remaining` (`occurrence - hits`)
  falls by one on every matching hit, and both are updated **before** the
  fault action runs, so the hit that fires an event is always recorded
- `crash` terminates the current backend; PostgreSQL then disconnects other
  backends as crash containment — that is the server's behaviour, not
  macavity state crossing sessions (see [About `crash`](#about-crash))
- a newly established session starts with an empty registry

## Why it exists

Error paths are the least-tested part of most database code. Making
PostgreSQL fail *on cue* — at commit, mid-statement, or by killing the
backend outright — turns "what happens if this errors out?" into a
repeatable test:

- checking that an extension's own hooks, callbacks and cleanup paths
  survive an error raised from inside the executor or at commit time
- exercising crash and recovery behaviour with a real, unclean backend death
- verifying that application retry logic does the right thing when a commit
  fails or a connection disappears
- reproducing timing-dependent bugs by stretching a specific operation

This is **v0.2**. v0.1 laid a deliberately small foundation — four fault
points, three actions, no shared state — so the whole thing can be read in
one sitting and extended without redesign. v0.2 extends it in exactly that
way: the single armed fault becomes a backend-local registry of reusable
events, and nothing else about points, actions or counting changes.

## Installation

Requires PostgreSQL 16 or later (server development headers: the
`postgresql-server-dev-*` package on Debian/Ubuntu, `postgresql*-devel` on
RHEL). Older majors are rejected at compile time rather than failing
obscurely partway through the build.

### Tested versions and platforms

| | Status |
| --- | --- |
| PostgreSQL 16, 17, 18 | Tested: the full `pg_regress` suite, `session_test.sh` and `crash_test.sh` pass on each (most recently 16.15, 17.11 and 18.6). |
| PostgreSQL 19 and later | **Not tested.** "16 or later" is the compile-time minimum, not a promise about future majors: the executor hook signatures macavity uses have changed between majors before (PostgreSQL 18 changed `ExecutorRun_hook`), so build and run the test suites before relying on a newer major. |
| Linux (x86-64) | Tested. |
| macOS, FreeBSD and other Unix-likes | Not tested. macavity uses only PostgreSQL's extension APIs plus POSIX `kill()`/`SIGKILL`, so it is expected to work, but that has not been verified. |
| Windows | **Not supported.** The `crash` action relies on POSIX `SIGKILL`, which PostgreSQL's Windows signal emulation cannot deliver, so on Windows builds `macavity_arm()` refuses `crash` with `feature_not_supported`. The rest of the extension has not been built or tested on Windows. |

```sh
make
make install          # may need sudo
```

The distribution carries a PGXN `META.json` (release status `testing`), so
it can also be built and installed with `pgxn install macavity` once
published.

Then, in a database on a **test** cluster:

```sql
CREATE EXTENSION macavity;
```

To upgrade an existing v0.1 installation, install the new build and run
`ALTER EXTENSION macavity UPDATE;`. The update recreates `macavity_arm()`,
`macavity_disarm()` and `macavity_status()`, whose return types changed, so
re-issue any `GRANT`s on them afterwards. Existing sessions keep the old
library loaded until they reconnect.

No `shared_preload_libraries` entry is needed: macavity allocates no shared
memory and installs its hooks when the library is loaded on first use. If
you prefer to load it in every session, `session_preload_libraries =
'macavity'` also works.

## Example usage

```sql
CREATE EXTENSION macavity;

SELECT * FROM macavity_points();
     point      |                                    description
----------------+------------------------------------------------------------------------------------
 executor_start | before executor execution begins (ExecutorStart_hook)
 executor_end   | after executor execution completes (ExecutorEnd_hook)
 before_commit  | before transaction commit processing (XACT_EVENT_PRE_COMMIT)
 before_abort   | during top-level transaction abort, not subtransaction rollback (XACT_EVENT_ABORT)

-- fail the very next statement; the new event's ID comes back
SELECT macavity_arm('executor_start', 'error');
 macavity_arm
--------------
            1

SELECT 1;
ERROR:  macavity: injected error at fault point "executor_start"

-- the event has fired, so it is completed -- but the hit that fired it was
-- counted before the error was raised, so the counters are still there
SELECT * FROM macavity_status();
 event_id |     point      | action | occurrence | hits | remaining |   state
----------+----------------+--------+------------+------+-----------+-----------
        1 | executor_start | error  |          1 |    1 |         0 | completed

-- reinstate it by ID: same event, fresh counters, armed again
SELECT macavity_arm(1);
 macavity_arm
--------------
 t

-- armed again means it fires again: the very next statement -- whatever it
-- is, even a macavity function -- is the matching executor_start hit
SELECT 2;
ERROR:  macavity: injected error at fault point "executor_start"

-- the event is completed once more, so now nothing is armed and
-- macavity_reset() runs normally: it forgets every event, returns how many
-- it discarded, and restarts IDs at 1
SELECT macavity_reset();
 macavity_reset
----------------
              1

SELECT * FROM macavity_status();
 event_id | point | action | occurrence | hits | remaining | state
----------+-------+--------+------------+------+-----------+-------
(0 rows)
```

Keep this in mind whenever an `executor_start` or `executor_end` event is
armed: every statement you run counts, including `macavity_status()`,
`macavity_disarm()` and `macavity_reset()` themselves. To clean up while an
event is still due, `macavity_disarm()` or `macavity_reset()` may itself
take the hit; run it again once it has.

Delay the fifth matching hit instead of failing it — the action-specific
shorthands do the same as `macavity_arm(point, action, occurrence)`:

```sql
SELECT macavity_arm_delay('executor_start', 5);
```

Make a commit fail, to test retry logic:

```sql
BEGIN;
SELECT macavity_arm('before_commit', 'error');
INSERT INTO orders VALUES (...);
COMMIT;
ERROR:  macavity: injected error at fault point "before_commit"
-- the transaction rolled back; the INSERT is gone
```

Arm several events at once, and inspect or cancel them at any time:

```sql
SELECT macavity_arm_delay('executor_start', 2),   -- 1
       macavity_arm_error('executor_start', 5),   -- 2
       macavity_arm_crash('before_commit', 10);   -- 3

SELECT * FROM macavity_status();
 event_id |     point      | action | occurrence | hits | remaining | state
----------+----------------+--------+------------+------+-----------+-------
        1 | executor_start | delay  |          2 |    1 |         1 | armed
        2 | executor_start | error  |          5 |    1 |         4 | armed
        3 | before_commit  | crash  |         10 |    0 |        10 | armed

SELECT macavity_disarm(3);   -- just event 3; true if it was armed
SELECT macavity_disarm();    -- every armed event
```

The `macavity_status()` query above is itself hit 1 of the two
`executor_start` events, so the `macavity_disarm(3)` after it is hit 2 and
is delayed by one second by event 1 before it runs.

## Events and the registry

Each session (backend) keeps an in-memory **event registry**. Every call
that creates an event adds one entry and returns its `event_id`; nothing is
ever removed from the registry except by `macavity_reset()`, so the
registry *is* the session's history of events — there is no separate log.
It exists only inside the current backend: it is not persisted, not shared
with other sessions, and gone when the session ends.

- **IDs** are integers handed out 1, 2, 3, … in creation order, across all
  the arming functions. They are never reused while the registry lives —
  disarming or completing an event does not free its ID — and restart at 1
  only after `macavity_reset()`. The same statements always produce the
  same IDs.
- **Any number of events can be armed at once**, at the same or different
  points, each with completely independent counters and state.
- Every event is in one of three **states**:

  | State | Meaning | Fires? |
  | --- | --- | --- |
  | `armed` | waiting for its occurrence | yes |
  | `completed` | reached its occurrence and fired (`armed → completed`) | no |
  | `disarmed` | cancelled by `macavity_disarm()` (`armed → disarmed`) | no |

- **Reinstating** — `macavity_arm(event_id)` takes a `completed` or
  `disarmed` event back to `armed`, keeping its ID, point, action and
  occurrence, and **resetting its counters** (`hits = 0`, `remaining =
  occurrence`), so it is a fresh run of the same event. On an event that is
  already `armed` it changes nothing — not even the counters — and returns
  `false`.

### Firing order

When several armed events match the same fault point, each hit visits them
in a fixed order:

1. by action: **`delay` > `crash` > `error`**
2. within one action: ascending **`event_id`** (ID order, not the order in
   which events were armed or reinstated)

Each event is counted and, if due, fired before the next is visited. A
`delay` returns, so evaluation carries on to the next event; `crash` and
`error` do not, so the events after them are **not reached** for that hit —
neither counted nor fired — exactly as if the fault point had been reached
one time fewer for them. For example, a `delay` and an `error` both due at
the same hit give a one-second pause followed by the error; an `error` and
a `crash` both due give a crash, and the error is never raised. The order is
not configurable.

## SQL API

| Function | Returns | Notes |
| --- | --- | --- |
| `macavity_arm(point text, action text, occurrence integer DEFAULT 1)` | `integer` — the new `event_id` | Creates a new armed event in the current session. Errors on an unknown point or action, on `occurrence <= 0`, on a NULL argument, and on `error` at `before_abort`. |
| `macavity_arm_error(point text, occurrence integer DEFAULT 1)`<br>`macavity_arm_delay(…)`<br>`macavity_arm_crash(…)` | `integer` — the new `event_id` | Shorthand for `macavity_arm(point, '<action>', occurrence)`, with the same validation. |
| `macavity_arm(event_id integer)` | `boolean` | Reinstates an existing event (see above): `true` if a `completed` or `disarmed` event was re-armed, `false` if it was already `armed` and left untouched. Errors on an ID that is not in the registry, or NULL. A single argument always means this form, because creating an event needs at least a point and an action. |
| `macavity_disarm(event_id integer DEFAULT NULL)` | `boolean` | With an ID, disarms that event; with no argument (or NULL), disarms every armed event. Returns `true` if at least one event went from `armed` to `disarmed`. `completed` and already-`disarmed` events are not changed; an unknown ID just returns `false`. Disarmed events keep their counters. |
| `macavity_status()` | set of `event_id, point, action, occurrence, hits, remaining, state` | Every event in the registry, in any state, ordered by `event_id`. No rows when the registry is empty. See below. |
| `macavity_reset()` | `integer` — the number of events discarded | Empties the registry and restarts event IDs at 1. Afterwards the session behaves as though no event had ever been created. |
| `macavity_points()` | set of `point, description` | Read straight out of the implemented-points table, so it can never advertise a point this build does not have. |

All of these except `macavity_points()` are revoked from `PUBLIC`; grant
them explicitly to the roles that should be able to inject faults.

### `macavity_status()` and the counters

`macavity_status()` returns one row per event, whatever its state, ordered
by `event_id`:

| `state` | Counters |
| --- | --- |
| `armed` | `hits` counting matching hits since the event was (re)armed |
| `completed` | the final counters of the run that fired (`remaining` is 0) |
| `disarmed` | how far the event got before it was disarmed |

Completed events are kept deliberately, so you can confirm after an
injected error that the hit was recorded. Reinstating an event resets its
counters; `macavity_reset()` removes it altogether.

Column by column:

- **`hits`** counts *matching fault-point hits*: hits at the event's point,
  in this session, since the event was last armed. Hits at other points,
  and in other sessions, do not count — nor does a hit at which the event
  was not reached because an earlier event in the firing order did not
  return.
- **`remaining`** is `occurrence - hits`, falling by one on every matching
  hit and reaching 0 on the hit that fires the event.
- Both are updated **before** the configured action runs, so the hit that
  fires the event is always included.
- The counters, and the registry as a whole, are **not transactional**: an
  injected `error` aborts its transaction, but the recorded hit is not
  rolled back with it — which is precisely what makes a completed event's
  counters worth reading. Likewise, an event created, disarmed or
  reinstated inside a transaction that rolls back stays that way.

### The arming statement does not count itself

The statement that calls an arming function (or reinstates an event) is
already running when the event becomes armed, and some of its fault points
are still ahead of it. Those are not counted:

- **`executor_end`** — the arming statement's own `ExecutorEnd` is skipped,
  so the first matching hit is the next statement's.
- **`before_commit` / `before_abort`** — in autocommit mode, the implicit
  commit of the arming statement is skipped. Inside `BEGIN … COMMIT` your
  own `COMMIT` (or the abort of that block) is a legitimate target and is
  counted.
- **`executor_start`** — nothing to skip: the arming statement's
  `ExecutorStart` ran before the event existed.

Each skip applies at most once and never outlives the arming statement. If
the arming statement fails after arming — say
`SELECT macavity_arm('executor_end', 'error', 2), 1 / (random() * 0)::int;`
— its `ExecutorEnd` never runs and nothing is skipped: the event stays
armed and the next two statements are hits 1 and 2. The same holds when
the failure is caught by a PL/pgSQL exception block.

For `macavity_arm('executor_end', 'error', 3)`, the sequence is:

| Matching hit | `hits` | `remaining` | Action |
| --- | --- | --- | --- |
| — (just armed) | 0 | 3 | — |
| 1 | 1 | 2 | none |
| 2 | 2 | 1 | none |
| 3 | 3 | 0 | `ERROR` raised, *after* the counters reached 3 / 0; `state` is `completed` |

The same ordering applies to `crash` — the hit is counted before the
`SIGKILL` — but nothing can read the result back afterwards, because the
registry lived in the backend that has just died.

## Supported actions

| Action | Effect |
| --- | --- |
| `error` | Raises `ERROR` (`SQLSTATE P0001`) at the fault point. Normal PostgreSQL semantics follow: the statement fails and the transaction is aborted. |
| `crash` | **Destructive.** Sends `SIGKILL` to the current backend's own PID. The backend dies immediately and uncleanly. See the warning below. |
| `delay` | Sleeps for a fixed 1 second. The duration is not part of the API yet; because `occurrence` is the last argument, a future version can add `duration` after it without breaking existing calls. Outside of abort processing, `delay` sleeps on the process latch, so `statement_timeout` and query cancellation still work during it. |

### About `crash`

`crash` sends `SIGKILL` to `MyProcPid` — the backend that armed the event
and reached the fault point — and nowhere else; macavity never signals the
postmaster or another backend directly.

**The crashed connection cannot restore itself.** Everything that lived in
its memory, including macavity's event registry, is gone. The client must
reconnect, and the new session starts with an empty registry, its IDs
starting again at 1 — the registry is backend-local, so there is nothing to
inherit. Any statement in flight is
lost; anything already committed survives (crash recovery replays it from
WAL).

**What happens to other sessions is PostgreSQL's doing, not macavity's.** A
backend that exits uncleanly always causes the postmaster to terminate the
remaining backends and run crash recovery — this is PostgreSQL's crash
containment, protecting shared memory after an unclean exit, and not
something an extension can opt out of. Those other sessions never had a
fault armed; they are disconnected exactly as they would be for any other
unclean backend exit. The event registry stays strictly session-local
throughout — the *mechanism* touches only the backend that armed the event,
but the *consequence* is a cluster-wide restart performed by PostgreSQL
itself. That is exactly why this is test-cluster-only tooling.

## Supported fault points

| Point | PostgreSQL API used | Fires |
| --- | --- | --- |
| `executor_start` | `ExecutorStart_hook` | After the executor is initialized, before any tuple is produced. |
| `executor_end` | `ExecutorEnd_hook` | After the executor has shut down for that statement. |
| `before_commit` | `RegisterXactCallback`, `XACT_EVENT_PRE_COMMIT` | Before the commit record is written, while an error can still safely abort the transaction. |
| `before_abort` | `RegisterXactCallback`, `XACT_EVENT_ABORT` | While a **top-level** transaction is aborting: an error outside a savepoint, an explicit `ROLLBACK`, or a failed `COMMIT`. |

**`before_abort` does not fire on subtransaction rollback.** `ROLLBACK TO
SAVEPOINT`, a PL/pgSQL `BEGIN … EXCEPTION` block catching an error, and
anything else that rolls back only a subtransaction are not matching hits:
a `delay` armed there adds no time, and `hits` does not move. PostgreSQL
reports those through `RegisterSubXactCallback` (`SUBXACT_EVENT_ABORT_SUB`),
which macavity does not hook. The name means "before the transaction's
abort completes", not "before any rollback". If the enclosing transaction
later aborts as a whole, that abort is a hit as usual.

All four are available through PostgreSQL's supported extension hook/callback 
APIs; macavity patches nothing in PostgreSQL core and does not depend on 
undocumented backend internals.

## Testing

```sh
make install
make installcheck    # pg_regress: API, validation, counters, faults, event registry
```

The five suites are:

| Suite | Covers |
| --- | --- |
| `macavity_basic` | API surface, ID-returning arm functions, disarm/reset bookkeeping, the skip that stops the arming statement counting itself |
| `macavity_errors` | every validation path, for both the generic and action-specific forms: unknown point, unknown action, `occurrence <= 0`, NULL arguments, `error` at `before_abort`, reinstating an unknown ID; and that failed calls create nothing and consume no ID |
| `macavity_counters` | `hits`/`remaining` after every matching hit at `occurrence` 1, 2 and 3; that the counters advance before the action runs; that non-matching hits and aborts do not move them; that reinstating resets them and disarming keeps them; that they are not transactional |
| `macavity_faults` | events that actually fire, using `error` and `delay`, at each point — including `executor_end` at `occurrence` 1, which checks that the hit is recorded (`hits` 1, `remaining` 0) even though the action raised an `ERROR`, the autocommit skip on reinstatement, an arming statement that fails after arming `executor_end` (top level, in a rolled-back block, and caught in PL/pgSQL), and `before_abort` not firing on `ROLLBACK TO SAVEPOINT` or a PL/pgSQL exception block |
| `macavity_events` | the registry: monotonic IDs and their restart after reset; several events at the same and at different points with independent counters and states; completion and retention; disarming one and all, and disarming completed/disarmed/unknown events; reinstating completed, disarmed and already-armed events; status showing every state; precedence `delay` > `error`, equal actions in ID order, several delays, several errors, and delay/error/abort interplay at `COMMIT` |

Counter readings while a `before_commit` fault is armed are taken inside
`BEGIN ... ROLLBACK`, since an abort does not disturb the count. Keep that
pattern in mind when adding tests.

Two things can't go in `pg_regress` and ship as scripts instead. Point them
at a **throwaway** cluster where `CREATE EXTENSION macavity` has been run:

```sh
test/session_test.sh -h /tmp -p 5432 -d contrib_regression   # safe
test/crash_test.sh   -h /tmp -p 5432 -d contrib_regression   # CRASHES the cluster
```

- `session_test.sh` needs concurrent connections, which a single
  `pg_regress` session cannot provide: it shows that session A's event
  fires only in session A, that session B starts with an empty registry,
  that two live sessions allocate event IDs independently (both start at
  1), that disarm and reset in one session leave the other's events armed,
  and that the registry does not outlive its session. It injects only
  `error`, so it is safe on any test cluster.
- `crash_test.sh` covers the `crash` action: that `hits` advances on the
  matching hits leading up to the crash, that the backend dies at the
  configured occurrence and not before, that the cluster comes back, that
  the reconnected session has an empty registry with IDs starting at 1,
  that data committed before the crash survives recovery, and the two
  precedence rules involving `crash` — `crash` beats `error`, and `delay`
  runs before `crash`, each regardless of event IDs. It crashes the
  cluster three times. `pg_regress` cannot survive a cluster-wide restart
  mid-run, which is why this is separate.

The crashing hit itself can't be read back — the counters lived in the
backend that died — so the ordering guarantee is instead verified through
the `error` action, which takes the identical code path in
`macavity_event_next()` before the action runs.

## License

MIT. See [LICENSE](LICENSE).

Copyright © 2026 Sivaprasad Murali.
