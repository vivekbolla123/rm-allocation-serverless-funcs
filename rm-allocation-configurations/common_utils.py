"""Datadog tracing utilities for SQLAlchemy engines.

Since ddtrace doesn't auto-instrument pymssql, this module provides
manual instrumentation via SQLAlchemy engine events. Every query
executed through an instrumented engine gets a Datadog APM span.

Safe to import and use even if ddtrace is unavailable — silently no-ops.

Usage:
    from common_utils import instrument_engine
    engine = create_engine("mssql+pymssql://...")
    instrument_engine(engine)
"""

import re
import logging

logger = logging.getLogger(__name__)

try:
    from ddtrace import tracer
    from sqlalchemy import event
    _TRACING_AVAILABLE = True
except ImportError:
    _TRACING_AVAILABLE = False

# Pattern to extract [DatabaseName].[Schema].[Table] or DatabaseName.Schema.Table from SQL
_DB_SCHEMA_TABLE_RE = re.compile(
    r'\[?(\w+)\]?\.\[?(\w+)\]?\.\[?(\w+)\]?'
)


def _extract_db_meta(statement):
    """Extract database, schema, and table from a SQL statement.
    
    Handles patterns like:
        [REZ0KOD01].[Rez].[InventoryLeg]
        REZ0KWB01.rez.Booking
        {self.NAV_DB_NAME}[Rez].[InventoryLegClass]  (already interpolated)
    
    Returns (db_name, schema, table) or (None, None, None).
    """
    match = _DB_SCHEMA_TABLE_RE.search(statement)
    if match:
        return match.group(1), match.group(2), match.group(3)
    return None, None, None


def instrument_engine(engine, service_name="navitaire-mssql"):
    """Attach Datadog trace spans to SQLAlchemy engine events.

    Creates a span for every query executed through the engine, capturing
    the SQL statement, database name, schema, table, row count, and errors.
    Tags follow Datadog's standard naming conventions for database spans.

    Args:
        engine: SQLAlchemy Engine instance to instrument.
        service_name: Datadog service name for the spans (default: navitaire-mssql).
    """
    if not _TRACING_AVAILABLE:
        logger.debug("ddtrace or sqlalchemy not available, skipping engine instrumentation")
        return

    # Extract host and port from engine URL for connection-level tags
    db_host = None
    db_port = None
    try:
        db_host = engine.url.host
        db_port = engine.url.port
    except Exception:
        pass

    try:
        @event.listens_for(engine, "before_cursor_execute")
        def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            span = tracer.trace("mssql.query", resource=statement[:1024], service=service_name)
            span.set_tag("sql.query", statement[:4096])
            span.span_type = "sql"

            # Standard Datadog DB tags
            span.set_tag("db.system", "mssql")
            if db_host:
                span.set_tag("out.host", db_host)
            if db_port:
                span.set_tag("out.port", db_port)

            # Extract database/schema/table from the query
            db_name, schema, table = _extract_db_meta(statement)
            if db_name:
                span.set_tag("db.name", db_name)
            if schema:
                span.set_tag("db.mssql.schema", schema)
            if table:
                span.set_tag("db.mssql.table", table)

            if not hasattr(conn, '_dd_spans'):
                conn._dd_spans = []
            conn._dd_spans.append(span)

        @event.listens_for(engine, "after_cursor_execute")
        def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            if hasattr(conn, '_dd_spans') and conn._dd_spans:
                span = conn._dd_spans.pop()
                try:
                    if cursor.rowcount and cursor.rowcount >= 0:
                        span.set_tag("db.row_count", cursor.rowcount)
                except Exception:
                    pass
                span.finish()

        @event.listens_for(engine, "handle_error")
        def _handle_error(exception_context):
            conn = exception_context.connection
            if hasattr(conn, '_dd_spans') and conn._dd_spans:
                span = conn._dd_spans.pop()
                span.set_exc_info(
                    type(exception_context.original_exception),
                    exception_context.original_exception,
                    exception_context.original_exception.__traceback__
                )
                span.finish()
    except Exception:
        logger.debug("Failed to attach ddtrace instrumentation to engine, tracing will be skipped")
