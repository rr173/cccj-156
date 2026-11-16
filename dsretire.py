"""
dsretire — managed shared-dataset column retirement coordinator (Python + PostgreSQL).

Scope / trust model
-------------------
* The module manages only tables it created itself and only governs consumers
  that go through this module's entry points (restricted projection / filter
  queries).  A superuser bypassing the module with raw SQL is explicitly out of
  scope; we do not promise to stop that.

Design invariants
-----------------
* Fields have a stable identity (field_id + identity token stored in the
  physical column's COMMENT).  Display names may change; identity is never
  reused through a name.  A re-created same-name column is a *different* field.
* consumer_revision rows are immutable declarations (enforced by trigger):
  required stable field ids, allowed types, approved old->new field mappings.
  Only `status` may transition (active -> draining -> retired).
* Field dependencies are computed ONLY from real revision declarations, never
  from what a single query happened to select.
* A run is in-flight from declaration validation until the result stream is
  fully read or closed.  In-flight protection = a ds_runs row + a session-level
  advisory lock on the run's dedicated connection, so a dead process releases
  protection automatically (the lock dies with the backend).
* Retirement decisions are persisted in the database (ds_fields.status =
  'retiring' + ds_retirements row), not held as transient check locks, so any
  coordinator process can resume from the database alone after a crash.
* DROP COLUMN is executed only when every dependent revision is draining or
  retired AND no dependent run is in flight, re-verified inside a transaction
  that is mutually exclusive with admission (exclusive advisory lock on the
  table's admission key), re-checking physical table identity, stable field
  identity and all registered dependencies right before the DDL.
* Existing catalog dependencies (SQL views etc.) are never CASCADE-dropped:
  retirement blocks explicitly and preserves them.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
import uuid

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

META_SCHEMA = "ds_meta"

# revision / field / run / retirement statuses and phases
REV_ACTIVE, REV_DRAINING, REV_RETIRED = "active", "draining", "retired"
FLD_ACTIVE, FLD_RETIRING, FLD_RETIRED = "active", "retiring", "retired"
RUN_IN_FLIGHT, RUN_DONE, RUN_ABORTED = "in_flight", "done", "aborted"
PH_ADMISSION_CLOSED = "admission_closed"
PH_WAITING = "waiting_inflight"
PH_BLOCKED = "blocked"
PH_DONE = "done"


# --------------------------------------------------------------------------- #
# errors
# --------------------------------------------------------------------------- #
class DsError(Exception):
    """Base class for module errors."""


class AdmissionRejected(DsError):
    """Run admission refused (revision draining/retired, or declaration stale)."""


class PreconditionFailed(DsError):
    """A persisted precondition is not met (e.g. dependent revisions active)."""


class TargetMismatch(DsError):
    """Same retirement id reused for a different target."""


class RetirementConflict(DsError):
    """A different retirement id already manages this field."""


class IdentityViolation(DsError):
    """Physical catalog identity does not match the persisted identity token."""


class FieldNotFound(DsError):
    """No managed field, and no persistent retirement record to recover from."""


class RetriableDdlFailure(DsError):
    """The final DDL failed in a retriable way (e.g. lock timeout)."""


class InFlightRemaining(RetriableDdlFailure):
    """In-flight dependent runs reappeared before the DDL; retry later."""


class RetirementBlocked(DsError):
    """Blocked by catalog dependencies (views etc.); nothing was dropped."""

    def __init__(self, message, dependents=None):
        super().__init__(message)
        self.dependents = dependents or []


class UnknownField(DsError):
    """Projection/filter references a field not declared by the revision."""


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

_TYPE_ALIASES = {
    "int": "integer",
    "int4": "integer",
    "integer": "integer",
    "int8": "bigint",
    "bigint": "bigint",
    "text": "text",
    "varchar": "character varying",
    "character varying": "character varying",
    "bool": "boolean",
    "boolean": "boolean",
    "float8": "double precision",
    "double precision": "double precision",
    "numeric": "numeric",
    "timestamptz": "timestamp with time zone",
    "timestamp with time zone": "timestamp with time zone",
}


def _norm_type(t: str) -> str:
    t = re.sub(r"\s+", " ", t.strip().lower())
    return _TYPE_ALIASES.get(t, t)


def _check_ident(name: str, what: str = "identifier") -> str:
    if not _IDENT_RE.match(name or ""):
        raise DsError(f"invalid {what}: {name!r}")
    return name


def _adv_key(tag: str) -> int:
    """Stable signed-int8 advisory lock key."""
    digest = hashlib.sha256(b"dsretire\x00" + tag.encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _table_admission_key(table_id) -> int:
    return _adv_key(f"table-admission:{table_id}")


def _run_key(run_id) -> int:
    return _adv_key(f"run:{run_id}")


def _table_token_str(token) -> str:
    return f"dsretire:table:{token}"


def _field_token_str(token) -> str:
    return f"dsretire:field:{token}"


class Hooks:
    """Optional instrumentation callbacks for retire_start (used by tests to
    simulate coordinator crashes at exact protocol points)."""

    AFTER_ADMISSION_CLOSED = "after_admission_closed"
    AFTER_DDL_COMMIT = "after_ddl_commit"


# --------------------------------------------------------------------------- #
# module
# --------------------------------------------------------------------------- #
class Module:
    def __init__(self, conninfo: str, meta_schema: str = META_SCHEMA):
        self.conninfo = conninfo
        self.meta = _check_ident(meta_schema, "meta schema")

    # -- connections -------------------------------------------------------- #
    def _connect(self, autocommit: bool = False):
        return psycopg.connect(
            self.conninfo, autocommit=autocommit, row_factory=dict_row
        )

    def _event(self, conn, kind: str, **detail):
        conn.execute(
            f"INSERT INTO {self.meta}.ds_events(actor_pid, kind, detail)"
            " VALUES (%s, %s, %s)",
            (os.getpid(), kind, psycopg.types.json.Jsonb(detail)),
        )

    # ------------------------------------------------------------------ #
    # schema initialisation
    # ------------------------------------------------------------------ #
    def init_schema(self):
        m = self.meta
        with self._connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {m}")
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {m}.ds_tables (
                    table_id        uuid PRIMARY KEY,
                    schema_name     text NOT NULL,
                    table_name      text NOT NULL,
                    identity_token  uuid NOT NULL,
                    created_at      timestamptz NOT NULL DEFAULT now(),
                    UNIQUE (schema_name, table_name)
                )"""
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {m}.ds_fields (
                    field_id        uuid PRIMARY KEY,
                    table_id        uuid NOT NULL REFERENCES {m}.ds_tables,
                    column_name     text NOT NULL,
                    display_name    text NOT NULL,
                    data_type       text NOT NULL,
                    status          text NOT NULL DEFAULT 'active',
                    identity_token  uuid NOT NULL,
                    created_at      timestamptz NOT NULL DEFAULT now(),
                    retired_at      timestamptz
                )"""
            )
            # physical name unique among live fields; retired rows keep history
            # and a re-created same-name column becomes a NEW identity.
            conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS ds_fields_live_name
                ON {m}.ds_fields (table_id, column_name)
                WHERE status IN ('active', 'retiring')"""
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {m}.ds_revisions (
                    revision_id     uuid PRIMARY KEY,
                    consumer_name   text NOT NULL,
                    revision_label  text NOT NULL,
                    table_id        uuid NOT NULL REFERENCES {m}.ds_tables,
                    status          text NOT NULL DEFAULT 'active',
                    required_fields jsonb NOT NULL,
                    field_mappings  jsonb NOT NULL DEFAULT '[]',
                    created_at      timestamptz NOT NULL DEFAULT now(),
                    UNIQUE (consumer_name, revision_label)
                )"""
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {m}.ds_runs (
                    run_id       uuid PRIMARY KEY,
                    revision_id  uuid NOT NULL REFERENCES {m}.ds_revisions,
                    status       text NOT NULL DEFAULT 'in_flight',
                    owner_pid    int,
                    backend_pid  int,
                    projection   jsonb,
                    started_at   timestamptz NOT NULL DEFAULT now(),
                    finished_at  timestamptz,
                    note         text
                )"""
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {m}.ds_retirements (
                    retirement_id        text PRIMARY KEY,
                    field_id             uuid NOT NULL REFERENCES {m}.ds_fields,
                    table_id             uuid NOT NULL REFERENCES {m}.ds_tables,
                    schema_name          text NOT NULL,
                    table_name           text NOT NULL,
                    column_name          text NOT NULL,
                    field_identity_token uuid NOT NULL,
                    table_identity_token uuid NOT NULL,
                    phase                text NOT NULL,
                    blocked_reason       text,
                    created_at           timestamptz NOT NULL DEFAULT now(),
                    updated_at           timestamptz NOT NULL DEFAULT now(),
                    ddl_committed_at     timestamptz
                )"""
            )
            # at most one non-done retirement per field, enforced by the DB
            conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS ds_retirements_one_active
                ON {m}.ds_retirements (field_id) WHERE phase <> 'done'"""
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {m}.ds_events (
                    event_id   bigserial PRIMARY KEY,
                    at         timestamptz NOT NULL DEFAULT now(),
                    actor_pid  int NOT NULL,
                    kind       text NOT NULL,
                    detail     jsonb NOT NULL DEFAULT '{{}}'
                )"""
            )
            # consumer_revision declarations are immutable: only `status`
            # may change, and rows may never be deleted.
            conn.execute(
                f"""
                CREATE OR REPLACE FUNCTION {m}.revision_immutable() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF TG_OP = 'DELETE' THEN
                        RAISE EXCEPTION 'consumer revisions are never deleted';
                    END IF;
                    IF NEW.consumer_name   IS DISTINCT FROM OLD.consumer_name
                    OR NEW.revision_label  IS DISTINCT FROM OLD.revision_label
                    OR NEW.table_id        IS DISTINCT FROM OLD.table_id
                    OR NEW.required_fields IS DISTINCT FROM OLD.required_fields
                    OR NEW.field_mappings  IS DISTINCT FROM OLD.field_mappings THEN
                        RAISE EXCEPTION 'consumer revision declarations are immutable';
                    END IF;
                    RETURN NEW;
                END $$"""
            )
            conn.execute(
                f"DROP TRIGGER IF EXISTS revision_immutable ON {m}.ds_revisions"
            )
            conn.execute(
                f"""
                CREATE TRIGGER revision_immutable
                BEFORE UPDATE OR DELETE ON {m}.ds_revisions
                FOR EACH ROW EXECUTE FUNCTION {m}.revision_immutable()"""
            )
            conn.commit()

    # ------------------------------------------------------------------ #
    # managed tables and fields
    # ------------------------------------------------------------------ #
    def create_table(self, schema: str, table: str, fields):
        """Create a managed table. fields: [(column, data_type, display_name?)]."""
        _check_ident(schema, "schema")
        _check_ident(table, "table")
        cols = []
        for f in fields:
            col, typ = f[0], f[1]
            _check_ident(col, "column")
            cols.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(typ)))
        with self._connect() as conn:
            conn.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
            conn.execute(
                sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(cols),
                )
            )
            table_id, token = uuid.uuid4(), uuid.uuid4()
            conn.execute(
                sql.SQL("COMMENT ON TABLE {}.{} IS {}").format(
                    sql.Identifier(schema), sql.Identifier(table),
                    sql.Literal(_table_token_str(token)),
                )
            )
            conn.execute(
                f"INSERT INTO {self.meta}.ds_tables"
                "(table_id, schema_name, table_name, identity_token)"
                " VALUES (%s, %s, %s, %s)",
                (table_id, schema, table, token),
            )
            for f in fields:
                col, typ = f[0], f[1]
                disp = f[2] if len(f) > 2 else col
                self._insert_field_row(conn, table_id, schema, table, col, typ, disp)
            self._event(conn, "table_created", schema=schema, table=table)
            conn.commit()
            return str(table_id)

    def _insert_field_row(self, conn, table_id, schema, table, col, typ, display):
        field_id, token = uuid.uuid4(), uuid.uuid4()
        conn.execute(
            sql.SQL("COMMENT ON COLUMN {}.{}.{} IS {}").format(
                sql.Identifier(schema), sql.Identifier(table),
                sql.Identifier(col), sql.Literal(_field_token_str(token)),
            )
        )
        row = conn.execute(
            "SELECT format_type(a.atttypid, a.atttypmod) AS t"
            " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s"
            " AND NOT a.attisdropped",
            (schema, table, col),
        ).fetchone()
        canonical = _norm_type(row["t"]) if row else _norm_type(typ)
        conn.execute(
            f"INSERT INTO {self.meta}.ds_fields"
            "(field_id, table_id, column_name, display_name, data_type,"
            " identity_token)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            (field_id, table_id, col, display, canonical, token),
        )
        self._event(
            conn, "field_registered", table_id=str(table_id), column=col,
            field_id=str(field_id),
        )
        return field_id

    def add_field(self, schema, table, column, data_type, display_name=None):
        """Add a new field (new stable identity) to a managed table."""
        _check_ident(column, "column")
        with self._connect() as conn:
            trow = self._table_row(conn, schema, table)
            conn.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(column),
                    sql.SQL(data_type),
                )
            )
            fid = self._insert_field_row(
                conn, trow["table_id"], schema, table, column, data_type,
                display_name or column,
            )
            conn.commit()
            return str(fid)

    def rename_field_display(self, field_id, new_display_name):
        """Display names are mutable; identity is not affected."""
        with self._connect() as conn:
            cur = conn.execute(
                f"UPDATE {self.meta}.ds_fields SET display_name = %s"
                " WHERE field_id = %s",
                (new_display_name, field_id),
            )
            if cur.rowcount != 1:
                raise FieldNotFound(f"field {field_id}")
            self._event(conn, "field_display_renamed", field_id=str(field_id),
                        display_name=new_display_name)
            conn.commit()

    # ------------------------------------------------------------------ #
    # consumer revisions
    # ------------------------------------------------------------------ #
    def register_revision(self, consumer, label, schema, table, required, mappings=()):
        """Publish an immutable consumer revision.

        required: [(field_ref, [allowed_type, ...]), ...]
        mappings: [(old_field_ref, new_field_ref), ...]  approved old->new map
        field_ref: field_id (uuid/str) or current display/column name.
        """
        with self._connect() as conn:
            trow = self._table_row(conn, schema, table)
            tid = trow["table_id"]
            # admission protocol: registration is mutually exclusive with the
            # retirement final phase via the table admission key.
            conn.execute(
                "SELECT pg_advisory_xact_lock_shared(%s)",
                (_table_admission_key(tid),),
            )
            decl, field_ids = [], []
            for ref, allowed in required:
                if not allowed:
                    raise DsError(f"allowed types required for field {ref!r}")
                frow = self._resolve_field(conn, tid, ref, lock=True)
                if frow["status"] != FLD_ACTIVE:
                    raise PreconditionFailed(
                        f"field {frow['column_name']!r} is {frow['status']}: "
                        "retirement has started; new dependent revisions are "
                        "no longer admitted"
                    )
                allowed_n = [_norm_type(t) for t in allowed]
                if _norm_type(frow["data_type"]) not in allowed_n:
                    raise DsError(
                        f"field {frow['column_name']!r} type "
                        f"{frow['data_type']} not in allowed {allowed_n}"
                    )
                decl.append(
                    {"field_id": str(frow["field_id"]), "allowed_types": allowed_n}
                )
                field_ids.append(str(frow["field_id"]))
            mdecl = []
            for old_ref, new_ref in mappings:
                of = self._resolve_field(conn, tid, old_ref, lock=True)
                nf = self._resolve_field(conn, tid, new_ref, lock=True)
                if nf["status"] != FLD_ACTIVE:
                    raise PreconditionFailed(
                        f"mapping target {nf['column_name']!r} is {nf['status']}"
                    )
                mdecl.append(
                    {"old_field_id": str(of["field_id"]),
                     "new_field_id": str(nf["field_id"])}
                )
            rid = uuid.uuid4()
            conn.execute(
                f"INSERT INTO {self.meta}.ds_revisions"
                "(revision_id, consumer_name, revision_label, table_id,"
                " required_fields, field_mappings)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                (rid, consumer, label, tid,
                 psycopg.types.json.Jsonb(decl), psycopg.types.json.Jsonb(mdecl)),
            )
            self._event(conn, "revision_registered", consumer=consumer,
                        label=label, revision_id=str(rid), fields=field_ids)
            conn.commit()
            return str(rid)

    def _transition_revision(self, revision_id, allowed_from, to, extra_check=None):
        with self._connect() as conn:
            rev = conn.execute(
                f"SELECT * FROM {self.meta}.ds_revisions WHERE revision_id = %s"
                " FOR UPDATE",
                (revision_id,),
            ).fetchone()
            if not rev:
                raise DsError(f"unknown revision {revision_id}")
            if rev["status"] == to:
                return rev["status"]  # idempotent
            if rev["status"] not in allowed_from:
                raise PreconditionFailed(
                    f"revision is {rev['status']}; cannot become {to}"
                )
            if extra_check:
                extra_check(conn, rev)
            conn.execute(
                f"UPDATE {self.meta}.ds_revisions SET status = %s"
                " WHERE revision_id = %s",
                (to, revision_id),
            )
            self._event(conn, f"revision_{to}", revision_id=str(revision_id))
            conn.commit()
            return to

    def drain_revision(self, revision_id):
        """Admin: no new runs may start; in-flight runs finish normally."""
        return self._transition_revision(
            revision_id, (REV_ACTIVE,), REV_DRAINING
        )

    def retire_revision(self, revision_id):
        def no_inflight(conn, rev):
            n = conn.execute(
                f"SELECT count(*) AS n FROM {self.meta}.ds_runs"
                " WHERE revision_id = %s AND status = 'in_flight'",
                (rev["revision_id"],),
            ).fetchone()["n"]
            if n:
                raise PreconditionFailed(
                    f"revision still has {n} in-flight run(s)"
                )
        return self._transition_revision(
            revision_id, (REV_ACTIVE, REV_DRAINING), REV_RETIRED, no_inflight
        )

    def set_revision_active(self, revision_id):
        """Re-admit a draining revision — refused once any required field is
        retiring/retired (retirement decisions are persistent)."""
        def fields_not_retiring(conn, rev):
            rows = conn.execute(
                f"SELECT f.column_name, f.status FROM {self.meta}.ds_fields f"
                " WHERE f.field_id IN ("
                "   SELECT (e->>'field_id')::uuid"
                "   FROM jsonb_array_elements(%s::jsonb) e)"
                " AND f.status <> 'active'",
                (psycopg.types.json.Jsonb(rev["required_fields"]),),
            ).fetchall()
            if rows:
                raise PreconditionFailed(
                    "cannot re-admit: required field(s) "
                    + ", ".join(f"{r['column_name']}({r['status']})" for r in rows)
                    + " are retiring/retired"
                )
        return self._transition_revision(
            revision_id, (REV_DRAINING,), REV_ACTIVE, fields_not_retiring
        )

    # ------------------------------------------------------------------ #
    # runs (restricted projection / filter queries through the module)
    # ------------------------------------------------------------------ #
    def run_query(self, *, revision_id=None, consumer=None, label=None,
                  select, filters=(), order_by=(), limit=None):
        """Open a protected read run. Returns a RunHandle (iterator of dicts).

        The run is in-flight from declaration validation until the returned
        stream is fully consumed or closed.  The caller MUST close the handle
        (context manager provided); killing the process releases protection
        automatically via the connection-bound advisory lock.
        """
        conn = self._connect()
        try:
            rev = self._admit(conn, revision_id, consumer, label)
            declared = {d["field_id"]: d["allowed_types"]
                        for d in rev["required_fields"]}
            frows = self._fields_by_ids(conn, list(declared))
            by_name = {}
            for f in frows:
                by_name.setdefault(f["display_name"], f)
                by_name.setdefault(f["column_name"], f)
                by_name.setdefault(str(f["field_id"]), f)

            def resolve(ref):
                f = by_name.get(str(ref))
                if not f:
                    raise UnknownField(
                        f"{ref!r} is not declared by revision "
                        f"{rev['consumer_name']}/{rev['revision_label']}"
                    )
                return f

            # declaration validation (physical type re-check, part of the run)
            for f in frows:
                phys = self._physical_type(
                    conn, rev["schema_name"], rev["table_name"], f["column_name"]
                )
                allowed = [_norm_type(t) for t in declared[str(f["field_id"])]]
                if phys and _norm_type(phys) not in allowed:
                    raise AdmissionRejected(
                        f"field {f['column_name']!r} physical type {phys} no "
                        f"longer matches allowed types {allowed}"
                    )
            proj_fields = [resolve(r) for r in select]
            if not proj_fields:
                raise DsError("empty projection")
            clauses, params = [], []
            ops = {"=", "!=", "<", "<=", ">", ">=", "LIKE"}
            for flt in filters:
                ref, op = flt[0], flt[1].upper()
                f = resolve(ref)
                if op in ("IS NULL", "IS NOT NULL"):
                    clauses.append(
                        sql.SQL("{} ").format(sql.Identifier(f["column_name"]))
                        + sql.SQL(op)
                    )
                elif op in ops:
                    clauses.append(
                        sql.SQL("{} ").format(sql.Identifier(f["column_name"]))
                        + sql.SQL(f"{op} %s")
                    )
                    params.append(flt[2])
                else:
                    raise DsError(f"operator {op!r} not allowed")
            cols = sql.SQL(", ").join(
                sql.SQL("{} AS {}").format(
                    sql.Identifier(f["column_name"]),
                    sql.Identifier(f["display_name"]),
                )
                for f in proj_fields
            )
            query = sql.SQL("SELECT {} FROM {}.{}").format(
                cols, sql.Identifier(rev["schema_name"]),
                sql.Identifier(rev["table_name"]),
            )
            if clauses:
                query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses)
            if order_by:
                ob = []
                for o in order_by:
                    ref, _, direction = o.partition(" ")
                    f = resolve(ref)
                    direction = direction.strip().upper()
                    if direction not in ("", "ASC", "DESC"):
                        raise DsError(f"bad order direction {direction!r}")
                    ob.append(
                        sql.SQL("{} {}").format(
                            sql.Identifier(f["column_name"]),
                            sql.SQL(direction or "ASC"),
                        )
                    )
                query += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(ob)
            if limit is not None:
                query += sql.SQL(" LIMIT %s")
                params.append(limit)

            run_id = uuid.uuid4()
            conn.execute(
                f"INSERT INTO {self.meta}.ds_runs"
                "(run_id, revision_id, owner_pid, backend_pid, projection)"
                " VALUES (%s, %s, %s, pg_backend_pid(), %s)",
                (run_id, rev["revision_id"], os.getpid(),
                 psycopg.types.json.Jsonb([f["display_name"]
                                           for f in proj_fields])),
            )
            self._event(conn, "run_admitted", run_id=str(run_id),
                        revision_id=str(rev["revision_id"]))
            conn.commit()

            # in-flight protection: session-level shared advisory lock, held
            # until the stream is fully read or closed; dies with the process.
            conn.execute("SELECT pg_advisory_lock_shared(%s)",
                         (_run_key(run_id),))
            conn.commit()  # end the lock statement's implicit transaction

            # protected read transaction: stable snapshot, read-only
            conn.read_only = True
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            cur = conn.cursor(name=f"run_{run_id.hex[:24]}")
            cur.itersize = 128
            cur.execute(query, params)
            return RunHandle(self, conn, cur, run_id, rev["revision_id"])
        except Exception:
            conn.close()
            raise

    def _admit(self, conn, revision_id, consumer, label):
        m = self.meta
        if revision_id is None:
            row = conn.execute(
                f"SELECT revision_id FROM {m}.ds_revisions"
                " WHERE consumer_name = %s AND revision_label = %s",
                (consumer, label),
            ).fetchone()
            if not row:
                raise DsError(f"unknown revision {consumer}/{label}")
            revision_id = row["revision_id"]
        rev = conn.execute(
            f"SELECT r.*, t.schema_name, t.table_name"
            f" FROM {m}.ds_revisions r JOIN {m}.ds_tables t USING (table_id)"
            " WHERE r.revision_id = %s FOR UPDATE",
            (revision_id,),
        ).fetchone()
        if not rev:
            raise DsError(f"unknown revision {revision_id}")
        # admission protocol: mutually exclusive with the retirement final
        # phase (which takes this key exclusively) for the admission txn only.
        conn.execute(
            "SELECT pg_advisory_xact_lock_shared(%s)",
            (_table_admission_key(rev["table_id"]),),
        )
        if rev["status"] != REV_ACTIVE:
            raise AdmissionRejected(
                f"revision {rev['consumer_name']}/{rev['revision_label']} is "
                f"{rev['status']}: admission closed (in-flight runs may "
                "finish, no new runs start)"
            )
        return rev

    def _finalize_run(self, conn, run_id):
        """Mark the run done, then release the advisory lock (this order keeps
        the retirement waiter from reclaiming a live run)."""
        try:
            conn.execute(
                f"UPDATE {self.meta}.ds_runs SET status = %s, finished_at = now()"
                " WHERE run_id = %s AND status = 'in_flight'",
                (RUN_DONE, run_id),
            )
            self._event(conn, "run_finished", run_id=str(run_id))
            conn.commit()
        finally:
            try:
                conn.execute("SELECT pg_advisory_unlock(%s)", (_run_key(run_id),))
            finally:
                conn.close()

    # ------------------------------------------------------------------ #
    # retirement: precheck (read-only)
    # ------------------------------------------------------------------ #
    def retire_precheck(self, schema, table, column):
        """Read-only precheck.  Consumes no run identity, changes no admission
        state, takes no locks beyond MVCC snapshots."""
        with self._connect(autocommit=True) as conn:
            out = {"schema": schema, "table": table, "column": column}
            trow = self._table_row(conn, schema, table, required=False)
            out["table_registered"] = bool(trow)
            if not trow:
                out["can_start"] = False
                out["blocks"] = ["table is not managed by this module"]
                return out
            frow = self._field_row(conn, trow["table_id"], column)
            out["field"] = dict(frow) if frow else None
            if frow:
                for k in ("field_id", "table_id", "identity_token"):
                    out["field"][k] = str(out["field"][k])
                out["dependent_revisions"] = self._dependent_revisions(
                    conn, trow["table_id"], frow["field_id"]
                )
                phys = self._physical_column(conn, schema, table, column)
                out["physical_column_present"] = bool(phys)
                if phys:
                    out["catalog_dependents"] = self._catalog_dependents(
                        conn, phys["attrelid"], phys["attnum"]
                    )
            else:
                out["dependent_revisions"] = []
                out["physical_column_present"] = False
            blocks = []
            if not frow:
                blocks.append("no such managed field")
            else:
                if frow["status"] != FLD_ACTIVE:
                    blocks.append(f"field status is {frow['status']}")
                for r in out["dependent_revisions"]:
                    if r["status"] not in (REV_DRAINING, REV_RETIRED):
                        blocks.append(
                            f"revision {r['consumer_name']}/{r['revision_label']}"
                            f" is {r['status']} (must be drained first)"
                        )
                for r in out["dependent_revisions"]:
                    if r["in_flight"]:
                        blocks.append(
                            f"revision {r['consumer_name']}/{r['revision_label']}"
                            f" has {r['in_flight']} in-flight run(s)"
                        )
                for d in out.get("catalog_dependents", []):
                    blocks.append(
                        f"catalog dependency: {d['kind']} {d['object']}"
                    )
            out["can_start"] = not blocks
            out["blocks"] = blocks
            return out

    # ------------------------------------------------------------------ #
    # retirement: start / resume
    # ------------------------------------------------------------------ #
    def retire_start(self, retirement_id, schema, table, column, *,
                     wait=True, wait_timeout=None, poll_interval=0.2,
                     ddl_lock_timeout_ms=5000, hooks=None):
        """Start or resume the managed retirement of one column.

        Idempotent on `retirement_id`: re-calling with the same target resumes
        from the persisted phase; re-using the id for a different target is
        rejected.  Safe to call again after any coordinator crash.
        """
        hooks = hooks or {}
        rrow, closed_now = self._open_or_resume(
            retirement_id, schema, table, column
        )
        if closed_now and hooks.get(Hooks.AFTER_ADMISSION_CLOSED):
            hooks[Hooks.AFTER_ADMISSION_CLOSED]()

        rrow = self._ensure_waiting(rrow)
        if rrow["phase"] == PH_DONE:
            return self._status(rrow, done=True)
        if not wait:
            return self._status(rrow, done=False)

        self._wait_for_inflight(rrow, poll_interval, wait_timeout)
        result = self._final_phase(rrow, ddl_lock_timeout_ms)
        if result.get("ddl_committed") and hooks.get(Hooks.AFTER_DDL_COMMIT):
            hooks[Hooks.AFTER_DDL_COMMIT]()
        return result

    def _open_or_resume(self, retirement_id, schema, table, column):
        m = self.meta
        with self._connect() as conn:
            rrow = conn.execute(
                f"SELECT * FROM {m}.ds_retirements WHERE retirement_id = %s",
                (retirement_id,),
            ).fetchone()
            if rrow:
                # same persistent target? compare against the snapshot taken
                # at start, not against whatever currently owns the name.
                if (rrow["schema_name"], rrow["table_name"],
                        rrow["column_name"]) != (schema, table, column):
                    raise TargetMismatch(
                        f"retirement {retirement_id!r} targets "
                        f"{rrow['schema_name']}.{rrow['table_name']}."
                        f"{rrow['column_name']}, not {schema}.{table}.{column}"
                    )
                return rrow, False

            trow = self._table_row(conn, schema, table)
            frow = self._field_row(conn, trow["table_id"], column)
            if not frow:
                # the column may be physically gone; without a persistent
                # retirement record we cannot verify identity -> refuse.
                raise FieldNotFound(
                    f"no managed field {schema}.{table}.{column} and no "
                    "persistent retirement record; refusing to treat a "
                    "missing column as retired"
                )
            conflict = conn.execute(
                f"SELECT retirement_id FROM {m}.ds_retirements"
                " WHERE field_id = %s AND phase <> 'done'",
                (frow["field_id"],),
            ).fetchone()
            if conflict:
                raise RetirementConflict(
                    f"field already retiring under "
                    f"{conflict['retirement_id']!r}"
                )
            bad = [r for r in self._dependent_revisions(
                conn, trow["table_id"], frow["field_id"])
                if r["status"] not in (REV_DRAINING, REV_RETIRED)]
            if bad:
                raise PreconditionFailed(
                    "dependent revisions must be draining/retired first: "
                    + ", ".join(f"{r['consumer_name']}/{r['revision_label']}"
                                f"({r['status']})" for r in bad)
                )
            # persistently close admission for this field
            conn.execute(
                f"UPDATE {m}.ds_fields SET status = %s"
                " WHERE field_id = %s AND status = 'active'",
                (FLD_RETIRING, frow["field_id"]),
            )
            conn.execute(
                f"INSERT INTO {m}.ds_retirements"
                "(retirement_id, field_id, table_id, schema_name, table_name,"
                " column_name, field_identity_token, table_identity_token,"
                " phase)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (retirement_id, frow["field_id"], trow["table_id"], schema,
                 table, column, frow["identity_token"],
                 trow["identity_token"], PH_ADMISSION_CLOSED),
            )
            self._event(conn, "retirement_admission_closed",
                        retirement_id=retirement_id,
                        field_id=str(frow["field_id"]), column=column)
            conn.commit()
            rrow = conn.execute(
                f"SELECT * FROM {m}.ds_retirements WHERE retirement_id = %s",
                (retirement_id,),
            ).fetchone()
            return rrow, True

    def _target_state(self, conn, trow, frow, rrow):
        """'ok' (physical column is our field), 'gone' (column absent),
        'foreign' (a same-name column with a different identity occupies the
        name — must NOT be dropped)."""
        phys = self._physical_column(
            conn, trow["schema_name"], trow["table_name"], frow["column_name"]
        )
        if not phys:
            return "gone"
        desc = phys["col_description"] or ""
        return "ok" if desc == _field_token_str(rrow["field_identity_token"]) \
            else "foreign"

    def _mark_recovered_done(self, conn, rrow, note):
        m = self.meta
        conn.execute(
            f"UPDATE {m}.ds_fields SET status = %s,"
            " retired_at = COALESCE(retired_at, now())"
            " WHERE field_id = %s",
            (FLD_RETIRED, rrow["field_id"]),
        )
        conn.execute(
            f"UPDATE {m}.ds_retirements SET phase = %s, updated_at = now(),"
            " blocked_reason = NULL WHERE retirement_id = %s",
            (PH_DONE, rrow["retirement_id"]),
        )
        self._event(conn, "retirement_recovered_complete",
                    retirement_id=rrow["retirement_id"], note=note)

    def _ensure_waiting(self, rrow):
        """Reload state, apply recovery fast-paths, persist the waiting phase."""
        m = self.meta
        with self._connect() as conn:
            rrow = conn.execute(
                f"SELECT * FROM {m}.ds_retirements WHERE retirement_id = %s"
                " FOR UPDATE",
                (rrow["retirement_id"],),
            ).fetchone()
            if rrow["phase"] == PH_DONE:
                return rrow
            trow = conn.execute(
                f"SELECT * FROM {m}.ds_tables WHERE table_id = %s",
                (rrow["table_id"],),
            ).fetchone()
            frow = conn.execute(
                f"SELECT * FROM {m}.ds_fields WHERE field_id = %s",
                (rrow["field_id"],),
            ).fetchone()
            # actual table identity, not a seconds-old list
            phys_t = self._physical_table(
                conn, trow["schema_name"], trow["table_name"]
            )
            if (not phys_t or phys_t["obj_description"]
                    != _table_token_str(trow["identity_token"])):
                raise IdentityViolation(
                    f"physical table {trow['schema_name']}.{trow['table_name']}"
                    " does not match the registered identity (recreated?); "
                    "refusing to continue"
                )
            state = self._target_state(conn, trow, frow, rrow)
            if state != "ok":
                # target already gone; only the persistent record + matching
                # original field identity allow recovered-complete handling.
                if frow["identity_token"] != rrow["field_identity_token"]:
                    raise IdentityViolation(
                        "persisted field identity does not match the "
                        "retirement record; refusing recovered-complete"
                    )
                note = ("column already absent" if state == "gone" else
                        "a different same-name field now occupies the name; "
                        "leaving it untouched")
                self._mark_recovered_done(conn, rrow, note)
                conn.commit()
                rrow["phase"] = PH_DONE
                return rrow
            if frow["status"] == FLD_ACTIVE:
                # crash fix-up (should not happen; closure is atomic with the
                # retirement row): keep admission closed.
                conn.execute(
                    f"UPDATE {m}.ds_fields SET status = %s WHERE field_id = %s",
                    (FLD_RETIRING, frow["field_id"]),
                )
            if rrow["phase"] == PH_ADMISSION_CLOSED:
                conn.execute(
                    f"UPDATE {m}.ds_retirements SET phase = %s,"
                    " updated_at = now() WHERE retirement_id = %s",
                    (PH_WAITING, rrow["retirement_id"]),
                )
                self._event(conn, "retirement_waiting",
                            retirement_id=rrow["retirement_id"])
                rrow["phase"] = PH_WAITING
            conn.commit()
            return rrow

    def _wait_for_inflight(self, rrow, poll_interval, wait_timeout):
        """Wait until no dependent revision has a live in-flight run.

        Holds NO lock on the data table: consumers that do not depend on the
        retiring column keep working while we wait.  Dead runs (killed query
        processes) are reclaimed via advisory-lock probing, so a force-killed
        consumer never blocks retirement forever.
        """
        m = self.meta
        deadline = (time.monotonic() + wait_timeout) if wait_timeout else None
        with self._connect(autocommit=True) as conn:
            while True:
                runs = conn.execute(
                    f"SELECT r.run_id FROM {m}.ds_runs r"
                    f" JOIN {m}.ds_revisions v USING (revision_id)"
                    " WHERE r.status = 'in_flight' AND v.table_id = %s"
                    " AND EXISTS (SELECT 1 FROM jsonb_array_elements("
                    "   v.required_fields) e"
                    "   WHERE (e->>'field_id')::uuid = %s)",
                    (rrow["table_id"], rrow["field_id"]),
                ).fetchall()
                alive = 0
                for run in runs:
                    key = _run_key(run["run_id"])
                    got = conn.execute(
                        "SELECT pg_try_advisory_lock(%s) AS got", (key,)
                    ).fetchone()["got"]
                    if got:
                        # owner vanished (process exit releases the lock)
                        cur = conn.execute(
                            f"UPDATE {m}.ds_runs SET status = %s,"
                            " finished_at = now(),"
                            " note = 'owner vanished; reclaimed by retirement'"
                            " WHERE run_id = %s AND status = 'in_flight'",
                            (RUN_ABORTED, run["run_id"]),
                        )
                        if cur.rowcount:
                            self._event(conn, "run_aborted_dead_owner",
                                        run_id=str(run["run_id"]),
                                        retirement_id=rrow["retirement_id"])
                        conn.execute("SELECT pg_advisory_unlock(%s)", (key,))
                    else:
                        alive += 1
                if alive == 0:
                    return
                if deadline and time.monotonic() > deadline:
                    raise RetriableDdlFailure(
                        f"timed out waiting for {alive} in-flight run(s); "
                        "state is persisted, call retire_start again to resume"
                    )
                time.sleep(poll_interval)

    def _final_phase(self, rrow, ddl_lock_timeout_ms):
        """Re-verify everything under the admission-exclusive protocol, then
        execute the physical DDL and the completion record in ONE transaction.
        """
        m = self.meta
        conn = self._connect()
        try:
            # mutually exclusive with admission (run starts / registrations)
            conn.execute("SELECT pg_advisory_xact_lock(%s)",
                         (_table_admission_key(rrow["table_id"]),))
            conn.execute("SELECT set_config('lock_timeout', %s, true)",
                         (str(ddl_lock_timeout_ms),))

            trow = conn.execute(
                f"SELECT * FROM {m}.ds_tables WHERE table_id = %s",
                (rrow["table_id"],),
            ).fetchone()
            frow = conn.execute(
                f"SELECT * FROM {m}.ds_fields WHERE field_id = %s FOR UPDATE",
                (rrow["field_id"],),
            ).fetchone()

            # (a) actual table identity
            phys_t = self._physical_table(
                conn, trow["schema_name"], trow["table_name"]
            )
            if (not phys_t or phys_t["obj_description"]
                    != _table_token_str(trow["identity_token"])):
                raise IdentityViolation(
                    "physical table identity mismatch; refusing DDL"
                )
            # (b) stable field identity — a same-name column with a different
            # identity is NOT our target and must not be dropped.
            state = self._target_state(conn, trow, frow, rrow)
            if state != "ok":
                self._mark_recovered_done(
                    conn, rrow,
                    f"target already gone ({state}) at final recheck",
                )
                conn.commit()
                return self._status(self._reload(rrow), done=True,
                                    recovered=True)
            # (c) all registered dependencies: dependent revisions must be
            # draining/retired (historical declarations remain, but admission
            # eligibility is what matters)
            bad = [r for r in self._dependent_revisions(
                conn, rrow["table_id"], rrow["field_id"])
                if r["status"] not in (REV_DRAINING, REV_RETIRED)]
            if bad:
                raise PreconditionFailed(
                    "dependent revisions no longer draining/retired: "
                    + ", ".join(r["consumer_name"] for r in bad)
                )
            inflight = conn.execute(
                f"SELECT r.run_id FROM {m}.ds_runs r"
                f" JOIN {m}.ds_revisions v USING (revision_id)"
                " WHERE r.status = 'in_flight' AND v.table_id = %s"
                " AND EXISTS (SELECT 1 FROM jsonb_array_elements("
                "   v.required_fields) e WHERE (e->>'field_id')::uuid = %s)",
                (rrow["table_id"], rrow["field_id"]),
            ).fetchall()
            if inflight:
                raise InFlightRemaining(
                    f"{len(inflight)} dependent run(s) still in flight"
                )
            # (d) catalog dependencies (views etc.): never CASCADE, block.
            deps = self._catalog_dependents(
                conn, phys_t["oid"], self._physical_column(
                    conn, trow["schema_name"], trow["table_name"],
                    frow["column_name"])["attnum"])
            if deps:
                reason = "; ".join(f"{d['kind']} {d['object']}" for d in deps)
                conn.execute(
                    f"UPDATE {m}.ds_retirements SET phase = %s,"
                    " blocked_reason = %s, updated_at = now()"
                    " WHERE retirement_id = %s",
                    (PH_BLOCKED,
                     f"catalog dependencies exist (not dropped): {reason}",
                     rrow["retirement_id"]),
                )
                self._event(conn, "retirement_blocked",
                            retirement_id=rrow["retirement_id"],
                            dependents=deps)
                conn.commit()
                raise RetirementBlocked(
                    f"retirement {rrow['retirement_id']} blocked by catalog "
                    f"dependencies (preserved, no CASCADE): {reason}",
                    dependents=deps,
                )
            # (e) physical DDL — brief ACCESS EXCLUSIVE, bounded by
            # lock_timeout; retriable on failure.
            conn.execute(
                sql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                    sql.Identifier(trow["schema_name"]),
                    sql.Identifier(trow["table_name"]),
                    sql.Identifier(frow["column_name"]),
                )
            )
            conn.execute(
                f"UPDATE {m}.ds_fields SET status = %s, retired_at = now()"
                " WHERE field_id = %s",
                (FLD_RETIRED, frow["field_id"]),
            )
            conn.execute(
                f"UPDATE {m}.ds_retirements SET phase = %s,"
                " ddl_committed_at = now(), updated_at = now(),"
                " blocked_reason = NULL WHERE retirement_id = %s",
                (PH_DONE, rrow["retirement_id"]),
            )
            self._event(conn, "column_dropped",
                        retirement_id=rrow["retirement_id"],
                        column=frow["column_name"])
            conn.commit()
            out = self._status(self._reload(rrow), done=True)
            out["ddl_committed"] = True
            return out
        except (psycopg.errors.LockNotAvailable,
                psycopg.errors.QueryCanceled) as e:
            conn.rollback()
            raise RetriableDdlFailure(
                f"DDL could not acquire its lock within "
                f"{ddl_lock_timeout_ms} ms ({e}); state persisted as "
                f"'{PH_WAITING}', call retire_start again to retry"
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # status / explanation
    # ------------------------------------------------------------------ #
    def _reload(self, rrow):
        with self._connect(autocommit=True) as conn:
            return conn.execute(
                f"SELECT * FROM {self.meta}.ds_retirements"
                " WHERE retirement_id = %s",
                (rrow["retirement_id"],),
            ).fetchone()

    def _status(self, rrow, done=False, recovered=False):
        return {
            "retirement_id": rrow["retirement_id"],
            "target": f"{rrow['schema_name']}.{rrow['table_name']}."
                      f"{rrow['column_name']}",
            "phase": rrow["phase"],
            "done": done or rrow["phase"] == PH_DONE,
            "recovered": recovered,
            "blocked_reason": rrow["blocked_reason"],
            "ddl_committed_at": str(rrow["ddl_committed_at"]),
        }

    def retire_status(self, retirement_id):
        with self._connect(autocommit=True) as conn:
            rrow = conn.execute(
                f"SELECT * FROM {self.meta}.ds_retirements"
                " WHERE retirement_id = %s",
                (retirement_id,),
            ).fetchone()
            if not rrow:
                return None
            out = self._status(rrow)
            out["field_id"] = str(rrow["field_id"])
            out["dependent_revisions"] = self._dependent_revisions(
                conn, rrow["table_id"], rrow["field_id"]
            )
            return out

    def explain(self, *, retirement_id=None, schema=None, table=None,
                column=None):
        """Human-readable status explanation entry point."""
        lines = []
        with self._connect(autocommit=True) as conn:
            if retirement_id:
                rrow = conn.execute(
                    f"SELECT * FROM {self.meta}.ds_retirements"
                    " WHERE retirement_id = %s",
                    (retirement_id,),
                ).fetchone()
                if not rrow:
                    return f"retirement {retirement_id!r}: unknown"
                schema, table, column = (rrow["schema_name"],
                                         rrow["table_name"],
                                         rrow["column_name"])
                lines.append(
                    f"retirement {retirement_id}: phase={rrow['phase']}"
                    + (f" blocked_reason={rrow['blocked_reason']}"
                       if rrow["blocked_reason"] else "")
                    + (f" ddl_committed_at={rrow['ddl_committed_at']}"
                       if rrow["ddl_committed_at"] else ""))
            pre = self.retire_precheck(schema, table, column)
            f = pre.get("field")
            if not f:
                # retired fields keep their history row; report them too
                trow = self._table_row(conn, schema, table, required=False)
                if trow:
                    fr = conn.execute(
                        f"SELECT * FROM {self.meta}.ds_fields"
                        " WHERE table_id = %s AND column_name = %s"
                        " ORDER BY created_at DESC LIMIT 1",
                        (trow["table_id"], column),
                    ).fetchone()
                    if fr:
                        f = {"status": fr["status"],
                             "field_id": str(fr["field_id"])}
            if f:
                lines.append(
                    f"field {schema}.{table}.{column}: status={f['status']}"
                    f" field_id={f['field_id']}")
            else:
                lines.append(f"field {schema}.{table}.{column}: not managed")
            lines.append(f"physical column present: "
                         f"{pre.get('physical_column_present')}")
            for r in pre.get("dependent_revisions", []):
                lines.append(
                    f"  revision {r['consumer_name']}/{r['revision_label']}:"
                    f" status={r['status']} in_flight={r['in_flight']}")
            for d in pre.get("catalog_dependents", []):
                lines.append(f"  catalog dependent: {d['kind']} {d['object']}")
            if pre.get("blocks"):
                lines.append("blocks: " + " | ".join(pre["blocks"]))
            else:
                lines.append("no blocks: retirement may proceed")
        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    # catalog helpers
    # ------------------------------------------------------------------ #
    def _table_row(self, conn, schema, table, required=True):
        row = conn.execute(
            f"SELECT * FROM {self.meta}.ds_tables"
            " WHERE schema_name = %s AND table_name = %s",
            (schema, table),
        ).fetchone()
        if required and not row:
            raise DsError(f"table {schema}.{table} is not managed")
        return row

    def _field_row(self, conn, table_id, column):
        return conn.execute(
            f"SELECT * FROM {self.meta}.ds_fields"
            " WHERE table_id = %s AND column_name = %s"
            " AND status IN ('active', 'retiring')",
            (table_id, column),
        ).fetchone()

    def _resolve_field(self, conn, table_id, ref, lock=False):
        row = None
        try:
            uid = uuid.UUID(str(ref))
        except (ValueError, AttributeError, TypeError):
            uid = None
        if uid:
            row = conn.execute(
                f"SELECT * FROM {self.meta}.ds_fields WHERE field_id = %s"
                + (" FOR UPDATE" if lock else ""),
                (uid,),
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT * FROM {self.meta}.ds_fields"
                " WHERE table_id = %s AND status <> 'retired'"
                " AND (display_name = %s OR column_name = %s)"
                + (" FOR UPDATE" if lock else ""),
                (table_id, str(ref), str(ref)),
            ).fetchone()
        if not row:
            raise UnknownField(f"no live field {ref!r} on table {table_id}")
        return row

    def _fields_by_ids(self, conn, ids):
        if not ids:
            return []
        return conn.execute(
            f"SELECT * FROM {self.meta}.ds_fields WHERE field_id = ANY(%s)",
            (ids,),
        ).fetchall()

    def _dependent_revisions(self, conn, table_id, field_id):
        rows = conn.execute(
            f"""
            SELECT v.revision_id, v.consumer_name, v.revision_label, v.status,
                   (SELECT count(*) FROM {self.meta}.ds_runs r
                     WHERE r.revision_id = v.revision_id
                       AND r.status = 'in_flight') AS in_flight
            FROM {self.meta}.ds_revisions v
            WHERE v.table_id = %s
              AND EXISTS (SELECT 1 FROM jsonb_array_elements(v.required_fields) e
                          WHERE (e->>'field_id')::uuid = %s)
            ORDER BY v.consumer_name, v.revision_label
            """,
            (table_id, field_id),
        ).fetchall()
        for r in rows:
            r["revision_id"] = str(r["revision_id"])
        return rows

    def _physical_table(self, conn, schema, table):
        return conn.execute(
            "SELECT c.oid, obj_description(c.oid, 'pg_class') AS obj_description"
            " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'",
            (schema, table),
        ).fetchone()

    def _physical_column(self, conn, schema, table, column):
        return conn.execute(
            "SELECT a.attrelid, a.attnum,"
            " col_description(a.attrelid, a.attnum) AS col_description"
            " FROM pg_attribute a"
            " JOIN pg_class c ON c.oid = a.attrelid"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s"
            " AND a.attnum > 0 AND NOT a.attisdropped",
            (schema, table, column),
        ).fetchone()

    def _physical_type(self, conn, schema, table, column):
        row = conn.execute(
            "SELECT format_type(a.atttypid, a.atttypmod) AS t"
            " FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s"
            " AND NOT a.attisdropped",
            (schema, table, column),
        ).fetchone()
        return row["t"] if row else None

    def _catalog_dependents(self, conn, table_oid, attnum):
        """Normal (non-auto) catalog dependencies on the column: views and
        other objects that DROP COLUMN would have to CASCADE — we never do."""
        rows = conn.execute(
            """
            SELECT d.classid::regclass::text AS catalog,
                   CASE d.classid
                     WHEN 'pg_rewrite'::regclass THEN
                       (SELECT 'view ' || cl.relname
                          FROM pg_rewrite rw JOIN pg_class cl
                            ON cl.oid = rw.ev_class
                         WHERE rw.oid = d.objid)
                     WHEN 'pg_constraint'::regclass THEN
                       (SELECT 'constraint ' || conname
                          FROM pg_constraint WHERE oid = d.objid)
                     WHEN 'pg_policy'::regclass THEN
                       (SELECT 'policy ' || polname
                          FROM pg_policy WHERE oid = d.objid)
                     WHEN 'pg_trigger'::regclass THEN
                       (SELECT 'trigger ' || tgname
                          FROM pg_trigger WHERE oid = d.objid)
                     ELSE d.classid::regclass::text || ' ' || d.objid::text
                   END AS object
            FROM pg_depend d
            WHERE d.refclassid = 'pg_class'::regclass
              AND d.refobjid = %s AND d.refobjsubid = %s
              AND d.deptype = 'n'
            """,
            (table_oid, attnum),
        ).fetchall()
        return [{"kind": r["catalog"], "object": r["object"]} for r in rows]


# --------------------------------------------------------------------------- #
# run handle
# --------------------------------------------------------------------------- #
class RunHandle:
    """A protected read run.  Iterate to stream rows; the in-flight protection
    is released only when the stream is exhausted or close() is called."""

    def __init__(self, module, conn, cur, run_id, revision_id):
        self._module = module
        self._conn = conn
        self._cur = cur
        self.run_id = str(run_id)
        self.revision_id = str(revision_id)
        self._closed = False

    def __iter__(self):
        try:
            while True:
                chunk = self._cur.fetchmany(self._cur.itersize)
                if not chunk:
                    break
                for row in chunk:
                    yield row
        finally:
            self.close()

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            self.close()
        return row

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._cur.close()
        except Exception:
            pass
        try:
            self._conn.commit()  # end the protected read transaction
        except Exception:
            try:
                self._conn.rollback()
            except Exception:
                pass
        try:
            self._conn.read_only = False
            self._conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
            self._module._finalize_run(self._conn, self.run_id)
        except Exception:
            try:
                self._conn.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
