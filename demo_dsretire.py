#!/usr/bin/env python3
"""
Multi-process orchestration demo for dsretire.

A parent process drives real child processes (consumers / retirement
coordinators) with synchronization barriers, kills some of them with SIGKILL
at exact protocol points, and then independently verifies the outcome with
direct SQL (pg_catalog + metadata tables) — never trusting the module's own
success reports.

Scenarios
  A  three-process barrier choreography: old consumer reading old_value,
     retirer prechecks and closes admission, another old consumer races the
     closure (admitted -> counted in-flight -> finishes before DROP), a late
     old consumer is clearly rejected, a new-version consumer reading only
     new_value completes while the old ones drain.
  B  SIGKILL the consumer mid-stream and SIGKILL the coordinator twice
     (after admission closure, while waiting); a fresh coordinator resumes
     from the database alone; the old column is never re-opened.
  C  coordinator crashes after the DDL commit but before its response;
     recovery must not drop a same-name replacement column.
  C2 out-of-band DROP + same-name recreate mid-retirement; resume must
     treat it as recovered-complete and leave the impostor column alone.
  D  a dependent SQL view blocks retirement (no CASCADE, view preserved);
     DDL lock timeout is retriable; success after unblock.
  E  revision immutability, display-name rename keeps identity, drain ->
     re-activate allowed before any retirement starts.
"""

import json
import multiprocessing as mp
import os
import queue as queue_mod
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psycopg  # noqa: E402
import dsretire  # noqa: E402

CONN = os.environ.get(
    "DSRETIRE_CONNINFO",
    "host=/workspace/.bootstrap/pgsocket port=54329 dbname=dsdemo user=node",
)
DATA = "dsdata"
META = "ds_meta"
FLAG_DIR = "/workspace/.bootstrap"
REPORT_PATH = "/workspace/retirement_demo_report.json"

REPORT = {"timeline": [], "assertions": [], "scenarios": {}}


# --------------------------------------------------------------------------- #
# parent-side independent helpers (direct SQL, no module code)
# --------------------------------------------------------------------------- #
def pg(sqltext, params=()):
    with psycopg.connect(CONN, autocommit=True) as c:
        cur = c.execute(sqltext, params)
        return cur.fetchall() if cur.description else []


def pg1(sqltext, params=()):
    rows = pg(sqltext, params)
    return rows[0][0] if rows else None


def log(msg):
    REPORT["timeline"].append({"t": time.time(), "msg": msg})
    print(f"  [demo] {msg}", flush=True)


def check(name, cond, detail=""):
    ok = bool(cond)
    REPORT["assertions"].append({"name": name, "ok": ok, "detail": str(detail)})
    print(f"  {'PASS' if ok else 'FAIL'}  {name}"
          + (f"  -- {detail}" if detail and not ok else ""), flush=True)
    return ok


def column_live(table, column):
    return pg1(
        "SELECT count(*) FROM pg_attribute a"
        " JOIN pg_class c ON c.oid = a.attrelid"
        " JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s"
        " AND a.attnum > 0 AND NOT a.attisdropped",
        (DATA, table, column),
    )


def meta_snapshot():
    return {
        "runs": pg("SELECT run_id::text, status FROM ds_meta.ds_runs"
                   " ORDER BY 1"),
        "retirements": pg("SELECT retirement_id, phase"
                          " FROM ds_meta.ds_retirements ORDER BY 1"),
        "fields": pg("SELECT field_id::text, status, display_name"
                     " FROM ds_meta.ds_fields ORDER BY 1"),
        "revisions": pg("SELECT revision_id::text, status"
                        " FROM ds_meta.ds_revisions ORDER BY 1"),
    }


def wait_phase(rid, phases, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        ph = pg1("SELECT phase FROM ds_meta.ds_retirements"
                 " WHERE retirement_id = %s", (rid,))
        if ph in phases:
            return ph
        time.sleep(0.1)
    return None


def collect(q, tags, timeout=90.0):
    out = {}
    deadline = time.time() + timeout
    while set(out) < set(tags) and time.time() < deadline:
        try:
            tag, ok, payload = q.get(timeout=1.0)
            out[tag] = (ok, payload)
        except queue_mod.Empty:
            pass
    return out


def join_all(procs, timeout=60.0):
    for p in procs:
        p.join(timeout)
    for p in procs:
        if p.is_alive():
            p.kill()
            p.join()


# --------------------------------------------------------------------------- #
# child process entry points
# --------------------------------------------------------------------------- #
def child_reader(revision_id, projection, order, barrier, ev_release, q, tag):
    """Old consumer: opens a run, reads one row, syncs on the barrier, then
    finishes reading only when released."""
    try:
        m = dsretire.Module(CONN)
        with m.run_query(revision_id=revision_id, select=projection,
                         order_by=[order]) as h:
            it = iter(h)
            first = next(it)
            barrier.wait(timeout=30)
            ev_release.wait(90)
            rest = list(it)
        q.put((tag, True, {"run_id": h.run_id, "first": dict(first),
                           "n_rest": len(rest)}))
    except Exception as e:  # noqa: BLE001
        q.put((tag, False, repr(e)))


def child_retirer_a(r1, barrier, ev_closed, q):
    """Retirer: precheck (read-only), sync, drain, close admission, wait for
    in-flight, then DDL."""
    try:
        m = dsretire.Module(CONN)
        pre = m.retire_precheck(DATA, "metrics_a", "old_value")
        barrier.wait(timeout=30)
        m.drain_revision(r1)
        st = m.retire_start(
            "ra-1", DATA, "metrics_a", "old_value", poll_interval=0.1,
            hooks={dsretire.Hooks.AFTER_ADMISSION_CLOSED:
                   lambda: ev_closed.set()})
        q.put(("RET", True, {"status": st, "precheck_blocks": pre["blocks"]}))
    except Exception as e:  # noqa: BLE001
        q.put(("RET", False, repr(e)))


def child_late_old(r1, q):
    """Old consumer trying to open a run after admission was closed."""
    try:
        m = dsretire.Module(CONN)
        h = m.run_query(revision_id=r1, select=["old_value"])
        h.close()
        q.put(("P4", False, "admitted unexpectedly"))
    except dsretire.AdmissionRejected as e:
        q.put(("P4", True, f"clearly rejected: {e}"))
    except Exception as e:  # noqa: BLE001
        q.put(("P4", False, f"unexpected error: {e!r}"))


def child_new_consumer(r2, q):
    """New-version consumer reading only new_value during the drain."""
    try:
        m = dsretire.Module(CONN)
        with m.run_query(revision_id=r2, select=["new_value"],
                         filters=[("new_value", ">=", 0)]) as h:
            rows = list(h)
        q.put(("P5", True, {"n": len(rows), "run_id": h.run_id}))
    except Exception as e:  # noqa: BLE001
        q.put(("P5", False, repr(e)))


def child_reader_hold(revision_id, ev_inflight, q, tag):
    """Opens a run, reads one row, then holds it open until killed."""
    try:
        m = dsretire.Module(CONN)
        h = m.run_query(revision_id=revision_id, select=["old_value"],
                        order_by=["old_value"])
        it = iter(h)
        next(it)
        ev_inflight.set()
        while True:
            time.sleep(1)
    except Exception as e:  # noqa: BLE001
        q.put((tag, False, repr(e)))
        ev_inflight.set()


def child_retirer_crash(rid, table, column, crash_point):
    """Coordinator that dies at an exact protocol point."""
    m = dsretire.Module(CONN)
    hooks = {}
    if crash_point == "after_admission_closed":
        hooks[dsretire.Hooks.AFTER_ADMISSION_CLOSED] = lambda: os._exit(1)
    elif crash_point == "after_ddl_commit":
        def die_after_commit():
            with open(os.path.join(FLAG_DIR, f"{rid}.committed"), "w") as f:
                f.write("committed")
            os._exit(1)
        hooks[dsretire.Hooks.AFTER_DDL_COMMIT] = die_after_commit
    m.retire_start(rid, DATA, table, column, hooks=hooks, poll_interval=0.1)


def child_retirer_plain(rid, table, column, q, tag, **kw):
    try:
        m = dsretire.Module(CONN)
        kw.setdefault("poll_interval", 0.1)
        st = m.retire_start(rid, DATA, table, column, **kw)
        q.put((tag, True, st))
    except Exception as e:  # noqa: BLE001
        q.put((tag, False, repr(e)))


def child_blocker(table, ev_locked, ev_unblock):
    """Holds ACCESS EXCLUSIVE on the data table until released."""
    conn = psycopg.connect(CONN)
    conn.execute(f"LOCK TABLE {DATA}.{table} IN ACCESS EXCLUSIVE MODE")
    ev_locked.set()
    ev_unblock.wait(90)
    conn.rollback()
    conn.close()


# --------------------------------------------------------------------------- #
# scenarios
# --------------------------------------------------------------------------- #
def scenario_a(m):
    log("scenario A: barrier choreography with 3 processes")
    m.create_table(DATA, "metrics_a",
                   [("old_value", "integer"), ("new_value", "bigint")])
    pg("INSERT INTO dsdata.metrics_a SELECT i, i*10 FROM generate_series(1,5) i")
    r1 = m.register_revision(
        "etl-old", "a1", DATA, "metrics_a",
        required=[("old_value", ["integer"]), ("new_value", ["bigint"])],
        mappings=[("old_value", "new_value")])
    r2 = m.register_revision(
        "etl-new", "a1", DATA, "metrics_a",
        required=[("new_value", ["bigint"])])

    snap0 = meta_snapshot()
    pre = m.retire_precheck(DATA, "metrics_a", "old_value")
    snap1 = meta_snapshot()
    check("A: precheck is read-only (no run identity consumed, no admission "
          "change)", snap0 == snap1, f"{snap0} vs {snap1}")
    check("A: precheck derives dependency from declarations",
          any(r["consumer_name"] == "etl-old"
              for r in pre["dependent_revisions"]))
    check("A: precheck blocks start while dependent revision active",
          not pre["can_start"] and any("etl-old" in b for b in pre["blocks"]),
          str(pre["blocks"]))
    REPORT["scenarios"]["A"] = {"precheck": pre}

    barrier = mp.Barrier(3)
    ev_closed, ev_release = mp.Event(), mp.Event()
    q = mp.Queue()
    p1 = mp.Process(target=child_reader,
                    args=(r1, ["old_value", "new_value"], "old_value",
                          barrier, ev_release, q, "P1"))
    p3 = mp.Process(target=child_reader,
                    args=(r1, ["new_value"], "new_value",
                          barrier, ev_release, q, "P3"))
    ret = mp.Process(target=child_retirer_a, args=(r1, barrier, ev_closed, q))
    p1.start(); p3.start(); ret.start()

    if not ev_closed.wait(60):
        check("A: admission closed by retirer", False, "timeout")
    log("admission closed; old readers P1/P3 still in flight")

    # P4: another old consumer tries to open AFTER closure -> clear rejection
    p4 = mp.Process(target=child_late_old, args=(r1, q))
    # P5: new-version consumer (new_value only) runs during the drain
    p5 = mp.Process(target=child_new_consumer, args=(r2, q))
    p4.start(); p5.start()

    time.sleep(1.5)  # let P4 be rejected and P5 be mid-run
    # independent parent checks while old readers are still in flight:
    pg("INSERT INTO dsdata.metrics_a VALUES (6, 60)")
    log("parent inserted a row directly while retirement waits")
    locks = pg(
        "SELECT l.mode FROM pg_locks l"
        " JOIN pg_class c ON c.oid = l.relation"
        " JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = %s AND c.relname = 'metrics_a'", (DATA,))
    REPORT["scenarios"]["A"]["locks_during_wait"] = [l[0] for l in locks]
    check("A: no table-wide exclusive lock held while waiting",
          all(l[0] != "AccessExclusiveLock" for l in locks),
          str(locks))

    res = collect(q, ["P4", "P5"])
    check("A: late old consumer clearly rejected after closure",
          res.get("P4", (False, ""))[0], str(res.get("P4")))
    check("A: new-version consumer completed during drain",
          res.get("P5", (False, ""))[0], str(res.get("P5")))

    ev_release.set()
    res.update(collect(q, ["P1", "P3", "RET"]))
    join_all([p1, p3, p4, p5, ret])
    check("A: P1 (old reader) finished its stream",
          res.get("P1", (False, ""))[0], str(res.get("P1")))
    check("A: P3 (old revision, projected only new_value) finished",
          res.get("P3", (False, ""))[0], str(res.get("P3")))
    check("A: retirer completed", res.get("RET", (False, ""))[0],
          str(res.get("RET")))
    REPORT["scenarios"]["A"]["results"] = {
        k: [v[0], str(v[1])] for k, v in res.items()}

    # ---- independent parent assertions on real catalog / metadata ----
    check("A: old_value physically dropped",
          column_live("metrics_a", "old_value") == 0)
    check("A: new_value still present", column_live("metrics_a", "new_value") == 1)
    check("A: new_value data intact (5 rows + parent insert)",
          pg1("SELECT sum(new_value) FROM dsdata.metrics_a") == 210
          and pg1("SELECT count(*) FROM dsdata.metrics_a") == 6)
    check("A: field recorded retired",
          pg1("SELECT status FROM ds_meta.ds_fields f"
              " JOIN ds_meta.ds_tables t USING (table_id)"
              " WHERE t.table_name='metrics_a' AND f.column_name='old_value'")
          == "retired")
    check("A: retirement recorded done",
          pg1("SELECT phase FROM ds_meta.ds_retirements"
              " WHERE retirement_id='ra-1'") == "done")

    ddl_at = pg1("SELECT ddl_committed_at FROM ds_meta.ds_retirements"
                 " WHERE retirement_id='ra-1'")
    closed_at = pg1("SELECT at FROM ds_meta.ds_events"
                    " WHERE kind='retirement_admission_closed'"
                    " AND detail->>'retirement_id'='ra-1'")
    runs = pg(
        "SELECT r.run_id::text, r.status, r.projection::text, r.started_at,"
        " r.finished_at, v.consumer_name"
        " FROM ds_meta.ds_runs r"
        " JOIN ds_meta.ds_revisions v USING (revision_id)"
        " JOIN ds_meta.ds_tables t ON t.table_id = v.table_id"
        " WHERE t.table_name='metrics_a' ORDER BY r.started_at")
    REPORT["scenarios"]["A"]["runs"] = [list(map(str, r)) for r in runs]
    old_runs = [r for r in runs if r[5] == "etl-old"]
    new_runs = [r for r in runs if r[5] == "etl-new"]
    check("A: exactly two runs admitted for old revision (P1, P3; P4 got no "
          "run identity)", len(old_runs) == 2, str(runs))
    check("A: both old runs finished before the DROP",
          all(r[4] and ddl_at and r[4] <= ddl_at for r in old_runs),
          f"ddl_at={ddl_at}")
    check("A: P3 admitted before closure (racing admission is counted "
          "in-flight)",
          any(r[3] and closed_at and r[3] < closed_at for r in old_runs))
    p3_run = [r for r in old_runs if r[2] == '["new_value"]']
    check("A: dependency came from the declaration, not the projection "
          "(P3 selected only new_value yet DROP waited for it)",
          len(p3_run) == 1 and p3_run[0][4] and p3_run[0][4] <= ddl_at)
    check("A: new-version run completed before the DROP",
          len(new_runs) == 1 and new_runs[0][4]
          and new_runs[0][4] <= ddl_at
          and new_runs[0][3] >= closed_at)
    ev_kinds = [r[0] for r in pg(
        "SELECT kind FROM ds_meta.ds_events"
        " WHERE detail->>'retirement_id'='ra-1' OR kind ~ '^run_'"
        " ORDER BY event_id")]
    check("A: audit events recorded (admission_closed -> waiting -> "
          "column_dropped)",
          "retirement_admission_closed" in ev_kinds
          and "retirement_waiting" in ev_kinds
          and "column_dropped" in ev_kinds
          and ev_kinds.index("retirement_admission_closed")
          < ev_kinds.index("column_dropped"), str(ev_kinds))
    return r2


def scenario_b(m):
    log("scenario B: SIGKILL consumer and coordinators; resume from DB alone")
    m.create_table(DATA, "metrics_b",
                   [("old_value", "integer"), ("new_value", "bigint")])
    pg("INSERT INTO dsdata.metrics_b SELECT i, i*10 FROM generate_series(1,3) i")
    r1b = m.register_revision(
        "etl-old", "b1", DATA, "metrics_b",
        required=[("old_value", ["integer"]), ("new_value", ["bigint"])],
        mappings=[("old_value", "new_value")])

    ev_inf, q = mp.Event(), mp.Queue()
    p1b = mp.Process(target=child_reader_hold, args=(r1b, ev_inf, q, "P1B"))
    p1b.start()
    check("B: old consumer in flight", ev_inf.wait(30))
    m.drain_revision(r1b)

    # coordinator 1: dies right after closing admission
    c1 = mp.Process(target=child_retirer_crash,
                    args=("rb-1", "metrics_b", "old_value",
                          "after_admission_closed"))
    c1.start(); c1.join(60)
    check("B: coordinator-1 crashed after admission closure",
          c1.exitcode not in (0, None), f"exitcode={c1.exitcode}")
    check("B: closure persisted in DB (not a transient lock)",
          wait_phase("rb-1", ("admission_closed", "waiting_inflight"), 10)
          is not None)

    # coordinator 2: dies while waiting for the in-flight run
    c2 = mp.Process(target=child_retirer_plain,
                    args=("rb-1", "metrics_b", "old_value", q, "RETB2"))
    c2.start()
    got = wait_phase("rb-1", ("waiting_inflight",), 30)
    check("B: coordinator-2 reached the waiting phase", got == "waiting_inflight")
    os.kill(c2.pid, signal.SIGKILL); c2.join()
    check("B: coordinator-2 SIGKILLed while waiting", c2.exitcode == -9)

    # the old column must not be re-opened to old versions
    try:
        h = m.run_query(revision_id=r1b, select=["old_value"])
        h.close()
        check("B: old revision still rejected after coordinator crashes", False)
    except dsretire.AdmissionRejected:
        check("B: old revision still rejected after coordinator crashes", True)
    try:
        m.register_revision("etl-late", "b1", DATA, "metrics_b",
                            required=[("old_value", ["integer"])])
        check("B: new dependent revision refused after retirement started",
              False)
    except dsretire.PreconditionFailed:
        check("B: new dependent revision refused after retirement started",
              True)
    try:
        m.set_revision_active(r1b)
        check("B: old revision admission cannot be restored", False)
    except dsretire.PreconditionFailed:
        check("B: old revision admission cannot be restored", True)

    # SIGKILL the in-flight consumer mid-stream
    os.kill(p1b.pid, signal.SIGKILL); p1b.join()
    check("B: consumer SIGKILLed mid-stream", p1b.exitcode == -9)

    # coordinator 3: resumes from the database alone and finishes
    c3 = mp.Process(target=child_retirer_plain,
                    args=("rb-1", "metrics_b", "old_value", q, "RETB3"))
    c3.start(); c3.join(90)
    res = collect(q, ["RETB3"], timeout=10)
    check("B: coordinator-3 resumed and completed",
          res.get("RETB3", (False, ""))[0], str(res.get("RETB3")))

    check("B: killed consumer's run reclaimed as aborted (no permanent "
          "block)",
          pg1("SELECT r.status FROM ds_meta.ds_runs r"
              " JOIN ds_meta.ds_revisions v USING (revision_id)"
              " WHERE v.consumer_name='etl-old' AND v.revision_label='b1'")
          == "aborted")
    check("B: old_value physically dropped",
          column_live("metrics_b", "old_value") == 0)
    check("B: new_value data intact",
          pg1("SELECT sum(new_value) FROM dsdata.metrics_b") == 60)
    check("B: retirement recorded done",
          pg1("SELECT phase FROM ds_meta.ds_retirements"
              " WHERE retirement_id='rb-1'") == "done")
    check("B: reclaim event audited",
          pg1("SELECT count(*) FROM ds_meta.ds_events"
              " WHERE kind='run_aborted_dead_owner'"
              " AND detail->>'retirement_id'='rb-1'") == 1)

    # same retirement id, different target -> rejected
    try:
        m.retire_start("rb-1", DATA, "metrics_b", "new_value", wait=False)
        check("B: same id with changed target rejected", False)
    except dsretire.TargetMismatch:
        check("B: same id with changed target rejected", True)
    # same id, same target -> idempotent done
    st = m.retire_start("rb-1", DATA, "metrics_b", "old_value", wait=False)
    check("B: same id same target resumes as done", st["done"])
    # old column never re-opened even now
    try:
        m.register_revision("etl-late", "b2", DATA, "metrics_b",
                            required=[("old_value", ["integer"])])
        check("B: registration on retired column refused", False)
    except dsretire.DsError:
        check("B: registration on retired column refused", True)


def scenario_c(m):
    log("scenario C: crash after DDL commit, before response")
    m.create_table(DATA, "metrics_c",
                   [("old_value", "integer"), ("new_value", "bigint")])
    pg("INSERT INTO dsdata.metrics_c SELECT i, i*10 FROM generate_series(1,4) i")
    r1c = m.register_revision(
        "etl-old", "c1", DATA, "metrics_c",
        required=[("old_value", ["integer"])],
        mappings=[("old_value", "new_value")])
    m.drain_revision(r1c)

    flag = os.path.join(FLAG_DIR, "rc-1.committed")
    if os.path.exists(flag):
        os.unlink(flag)
    c1 = mp.Process(target=child_retirer_crash,
                    args=("rc-1", "metrics_c", "old_value", "after_ddl_commit"))
    c1.start()
    deadline = time.time() + 60
    while not os.path.exists(flag) and time.time() < deadline:
        time.sleep(0.1)
    c1.join(30)
    check("C: coordinator crashed after DDL commit, before responding",
          os.path.exists(flag) and c1.exitcode not in (0, None),
          f"exitcode={c1.exitcode}")
    check("C: committed phase is visible in the database",
          pg1("SELECT phase FROM ds_meta.ds_retirements"
              " WHERE retirement_id='rc-1'") == "done")

    q = mp.Queue()
    c2 = mp.Process(target=child_retirer_plain,
                    args=("rc-1", "metrics_c", "old_value", q, "RETC2"))
    c2.start(); c2.join(60)
    res = collect(q, ["RETC2"], timeout=10)
    check("C: another process recovered completion from the database alone",
          res.get("RETC2", (False, ""))[0]
          and res["RETC2"][1].get("done"), str(res.get("RETC2")))
    check("C: old_value physically dropped",
          column_live("metrics_c", "old_value") == 0)
    check("C: new_value data intact",
          pg1("SELECT sum(new_value) FROM dsdata.metrics_c") == 100)

    # a NEW same-name column is a different field; recovery must not drop it
    new_fid = m.add_field(DATA, "metrics_c", "old_value", "text")
    pg("INSERT INTO dsdata.metrics_c (new_value, old_value)"
       " VALUES (999, 'reborn')")
    st = m.retire_start("rc-1", DATA, "metrics_c", "old_value")
    check("C: re-resume with same-name new column present reports done",
          st["done"])
    check("C: same-name new column NOT dropped by recovery",
          column_live("metrics_c", "old_value") == 1)
    check("C: same-name new column data intact",
          pg1("SELECT count(*) FROM dsdata.metrics_c"
              " WHERE old_value = 'reborn'") == 1)
    old_fid = pg1("SELECT field_id::text FROM ds_meta.ds_fields f"
                  " JOIN ds_meta.ds_tables t USING (table_id)"
                  " WHERE t.table_name='metrics_c' AND f.column_name='old_value'"
                  " AND f.status='retired'")
    check("C: name reuse did not reuse identity",
          old_fid is not None and old_fid != new_fid)


def scenario_c2(m):
    log("scenario C2: out-of-band drop + same-name recreate mid-retirement")
    m.create_table(DATA, "metrics_c2",
                   [("old_value", "integer"), ("new_value", "bigint")])
    pg("INSERT INTO dsdata.metrics_c2 SELECT i, i*10 FROM generate_series(1,3) i")
    r1 = m.register_revision(
        "etl-old", "c2", DATA, "metrics_c2",
        required=[("old_value", ["integer"])])
    m.drain_revision(r1)
    c1 = mp.Process(target=child_retirer_crash,
                    args=("rc2-1", "metrics_c2", "old_value",
                          "after_admission_closed"))
    c1.start(); c1.join(60)
    check("C2: coordinator crashed after admission closure",
          c1.exitcode not in (0, None))
    # out-of-band: someone drops the column and re-adds a same-name one
    pg("ALTER TABLE dsdata.metrics_c2 DROP COLUMN old_value")
    pg("ALTER TABLE dsdata.metrics_c2 ADD COLUMN old_value text")
    pg("INSERT INTO dsdata.metrics_c2 (new_value, old_value)"
       " VALUES (7, 'impostor')")
    st = m.retire_start("rc2-1", DATA, "metrics_c2", "old_value")
    check("C2: resume treats target as recovered-complete via persistent "
          "record + identity match", st["done"])
    check("C2: impostor same-name column NOT dropped",
          column_live("metrics_c2", "old_value") == 1)
    check("C2: impostor data intact",
          pg1("SELECT count(*) FROM dsdata.metrics_c2"
              " WHERE old_value='impostor'") == 1)
    check("C2: original field recorded retired",
          pg1("SELECT status FROM ds_meta.ds_fields f"
              " JOIN ds_meta.ds_tables t USING (table_id)"
              " WHERE t.table_name='metrics_c2'"
              " AND f.column_name='old_value'") == "retired")


def scenario_d(m):
    log("scenario D: dependent view blocks; DDL lock timeout retriable")
    m.create_table(DATA, "metrics_d",
                   [("old_value", "integer"), ("new_value", "bigint")])
    pg("INSERT INTO dsdata.metrics_d SELECT i, i*10 FROM generate_series(1,3) i")
    r1 = m.register_revision(
        "etl-old", "d1", DATA, "metrics_d",
        required=[("old_value", ["integer"])])
    m.drain_revision(r1)
    pg("CREATE VIEW dsdata.v_d_old AS SELECT old_value FROM dsdata.metrics_d")

    try:
        m.retire_start("rd-1", DATA, "metrics_d", "old_value")
        check("D: retirement blocked by dependent view", False,
              "unexpectedly succeeded")
    except dsretire.RetirementBlocked as b:
        check("D: retirement blocked by dependent view",
              "v_d_old" in str(b), str(b))
    check("D: blocked phase persisted",
          pg1("SELECT phase FROM ds_meta.ds_retirements"
              " WHERE retirement_id='rd-1'") == "blocked")
    check("D: view preserved (no CASCADE)",
          pg1("SELECT count(*) FROM pg_class c JOIN pg_namespace n"
              " ON n.oid=c.relnamespace WHERE n.nspname=%s"
              " AND c.relname='v_d_old' AND c.relkind='v'", (DATA,)) == 1)
    check("D: column preserved while blocked",
          column_live("metrics_d", "old_value") == 1)
    check("D: view still usable",
          pg1("SELECT count(*) FROM dsdata.v_d_old") == 3)

    # DDL lock timeout is a retriable state
    ev_locked, ev_unblock = mp.Event(), mp.Event()
    blk = mp.Process(target=child_blocker,
                     args=("metrics_d", ev_locked, ev_unblock))
    blk.start()
    check("D: blocker holds ACCESS EXCLUSIVE", ev_locked.wait(30))
    pg("DROP VIEW dsdata.v_d_old")
    try:
        m.retire_start("rd-1", DATA, "metrics_d", "old_value",
                       ddl_lock_timeout_ms=1000)
        check("D: DDL lock timeout surfaces as retriable failure", False,
              "unexpectedly succeeded")
    except dsretire.RetriableDdlFailure:
        check("D: DDL lock timeout surfaces as retriable failure", True)
    check("D: column intact after timed-out DDL",
          column_live("metrics_d", "old_value") == 1)
    check("D: retirement not done yet (retriable state)",
          pg1("SELECT phase FROM ds_meta.ds_retirements"
              " WHERE retirement_id='rd-1'") != "done")
    ev_unblock.set(); blk.join(30)

    st = m.retire_start("rd-1", DATA, "metrics_d", "old_value")
    check("D: retry after unblock completes", st["done"])
    check("D: old_value dropped, new_value intact",
          column_live("metrics_d", "old_value") == 0
          and pg1("SELECT sum(new_value) FROM dsdata.metrics_d") == 60)


def scenario_e(m, r2_a):
    log("scenario E: immutability, display rename, drain/re-activate")
    try:
        pg("UPDATE ds_meta.ds_revisions SET required_fields='[]'::jsonb"
           " WHERE consumer_name='etl-new'")
        check("E: revision declaration immutable (trigger)", False)
    except Exception as e:  # noqa: BLE001
        check("E: revision declaration immutable (trigger)",
              "immutable" in str(e), str(e)[:80])
    check("E: declaration unchanged after attempt",
          pg1("SELECT required_fields <> '[]'::jsonb"
              " FROM ds_meta.ds_revisions WHERE consumer_name='etl-new'"
              " AND revision_label='a1'") is True)

    fid = pg1("SELECT field_id::text FROM ds_meta.ds_fields f"
              " JOIN ds_meta.ds_tables t USING (table_id)"
              " WHERE t.table_name='metrics_a' AND f.column_name='new_value'")
    m.rename_field_display(fid, "New Value")
    with m.run_query(revision_id=r2_a, select=["New Value"]) as h:
        rows = list(h)
    check("E: display rename keeps identity; query by new display name works",
          len(rows) == 6 and "New Value" in rows[0])
    check("E: field_id unchanged by rename",
          pg1("SELECT field_id::text FROM ds_meta.ds_fields"
              " WHERE display_name='New Value'") == fid)

    m.drain_revision(r2_a)
    m.set_revision_active(r2_a)  # allowed: no retirement started on new_value
    with m.run_query(revision_id=r2_a, select=["New Value"], limit=1) as h:
        one = list(h)
    check("E: drain -> re-activate works before any retirement on its fields",
          len(one) == 1)


def main():
    mp.set_start_method("fork")
    with psycopg.connect(CONN, autocommit=True) as c:
        c.execute("DROP SCHEMA IF EXISTS dsdata CASCADE")
        c.execute("DROP SCHEMA IF EXISTS ds_meta CASCADE")
    m = dsretire.Module(CONN)
    m.init_schema()
    log("schema initialized")

    r2_a = scenario_a(m)
    scenario_b(m)
    scenario_c(m)
    scenario_c2(m)
    scenario_d(m)
    scenario_e(m, r2_a)

    # catalog change record + audit trail for the report
    REPORT["catalog"] = {
        t: pg("SELECT a.attname, a.atttypid::regtype::text, a.attisdropped"
              " FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid"
              " JOIN pg_namespace n ON n.oid=c.relnamespace"
              " WHERE n.nspname=%s AND c.relname=%s AND a.attnum>0"
              " ORDER BY a.attnum", (DATA, t))
        for t in ("metrics_a", "metrics_b", "metrics_c", "metrics_c2",
                  "metrics_d")
    }
    REPORT["retirements"] = [
        dict(zip(("retirement_id", "phase", "blocked_reason",
                  "ddl_committed_at"), (r[0], r[1], r[2], str(r[3]))))
        for r in pg("SELECT retirement_id, phase, blocked_reason,"
                    " ddl_committed_at FROM ds_meta.ds_retirements ORDER BY 1")
    ]
    REPORT["events"] = [
        {"id": r[0], "at": str(r[1]), "pid": r[2], "kind": r[3],
         "detail": r[4]}
        for r in pg("SELECT event_id, at, actor_pid, kind, detail::text"
                    " FROM ds_meta.ds_events ORDER BY event_id")
    ]
    failed = [a for a in REPORT["assertions"] if not a["ok"]]
    REPORT["summary"] = {"total": len(REPORT["assertions"]),
                         "failed": len(failed)}
    with open(REPORT_PATH, "w") as f:
        json.dump(REPORT, f, indent=2, default=str)
    print(f"\n== {REPORT['summary']['total']} assertions, "
          f"{len(failed)} failed; report: {REPORT_PATH}")
    if failed:
        for a in failed:
            print("FAILED:", a["name"], "--", a["detail"][:200])
    print("\n" + m.explain(retirement_id="ra-1"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
