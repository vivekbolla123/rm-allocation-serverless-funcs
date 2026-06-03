"""Datadog tracing utilities.

Provides manual instrumentation helpers for Datadog APM:
- trace_span / finish_span: Generic span helpers for any operation (SFTP, HTTP, etc.)
- instrument_engine: SQLAlchemy engine auto-instrumentation for pymssql

Safe to import and use even if ddtrace is unavailable — silently no-ops.

Usage:
    from common_utils import trace_span, finish_span, instrument_engine

    # Generic spans
    span = trace_span("sftp.upload", resource="/path", service="au-sftp", tags={"run_id": "123"})
    try:
        do_work()
    except Exception:
        if span: span.set_exc_info(*sys.exc_info())
        raise
    finally:
        finish_span(span)

    # SQLAlchemy engine instrumentation
    engine = create_engine("mssql+pymssql://...")
    instrument_engine(engine)
"""

import re
import base64
import logging
import requests
import configsettings

logger = logging.getLogger(__name__)

try:
    from ddtrace import tracer
    from sqlalchemy import event
    _TRACING_AVAILABLE = True
except ImportError:
    _TRACING_AVAILABLE = False


def trace_span(name, resource=None, service=None, tags=None):
    """Create a safe Datadog trace span that never breaks business logic.

    Args:
        name: Span operation name (e.g. "sftp.upload", "http.request").
        resource: The resource being acted on (e.g. file path, URL).
        service: Datadog service name override for this span.
        tags: Dict of additional tags to set on the span.

    Returns:
        A ddtrace Span, or None if tracing is unavailable.
    """
    if not _TRACING_AVAILABLE:
        return None
    try:
        span = tracer.trace(name, resource=resource, service=service)
        span.set_tag("Department", "RM")
        if tags:
            for k, v in tags.items():
                span.set_tag(k, v)
        return span
    except Exception:
        return None


def finish_span(span):
    """Safely finish a trace span."""
    if span:
        try:
            span.finish()
        except Exception:
            pass


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
            span.set_tag("Department", "RM")
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
                    if cursor.rowcount is not None and cursor.rowcount >= 0:
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


class GraphMailClient:
    """
    Sends emails via Microsoft Graph API using client credentials flow.
    Reads tenant/client/secret from configsettings at call time.

    Usage:
        from common_utils import GraphMailClient
        mail_client = GraphMailClient(logger)
        mail_client.send_email("Subject", "<h1>Hello</h1>", "recipient@example.com", is_html=True)
    """

    def __init__(self, log):
        self.logger = log

    def _get_access_token(self):
        """Obtain an access token from Microsoft identity platform using client credentials."""

        token_url = f"https://login.microsoftonline.com/{configsettings.ALERTS_TENANT_ID}/oauth2/v2.0/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": configsettings.ALERTS_CLIENT_ID,
            "client_secret": configsettings.ALERTS_CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default"
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response = requests.post(token_url, data=payload, headers=headers)
        response.raise_for_status()

        return response.json()["access_token"]

    def send_email(self, subject, body, recipient, is_html=False, attachments=None):
        """
        Send an email via Microsoft Graph API (draft + send pattern).

        Args:
            subject: Email subject line.
            body: Email body content.
            recipient: Recipient email address (single string).
            is_html: If True, body is treated as HTML; otherwise plain text.
            attachments: Optional list of dicts with keys:
                - 'filename': Name of the file (e.g., 'report.csv')
                - 'content': Raw string content of the file
                - 'file_type': Type of file ('csv' or 'txt'). Used for encoding.
        """

        try:
            # Step 1: Get access token
            access_token = self._get_access_token()

            user_id = configsettings.SENDER_EMAIL
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }

            # Step 2: Draft the mail
            content_type = "HTML" if is_html else "Text"
            draft_payload = {
                "subject": subject,
                "importance": "Normal",
                "body": {
                    "contentType": content_type,
                    "content": body
                },
                "toRecipients": [
                    {
                        "emailAddress": {
                            "address": recipient
                        }
                    }
                ]
            }

            draft_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages"
            draft_response = requests.post(draft_url, headers=headers, json=draft_payload)
            draft_response.raise_for_status()

            draft_mail_id = draft_response.json()["id"]

            # Step 3: Add attachments if provided
            if attachments:
                for attachment in attachments:
                    encoded_content = self._encode_file_content(
                        attachment['content'], attachment.get('file_type', 'csv')
                    )
                    self._add_attachment(draft_mail_id, attachment['filename'], encoded_content, headers)

            # Step 4: Send the drafted mail
            send_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{draft_mail_id}/send"
            send_response = requests.post(send_url, headers=headers)

            if send_response.status_code == 202:
                self.logger.info(f"Email sent to {recipient} successfully via Graph API.")
            else:
                self.logger.error(
                    f"Failed to send email via Graph API. "
                    f"Status: {send_response.status_code}, Response: {send_response.text}"
                )

        except Exception as e:
            self.logger.error(f"Failed to send email via Graph API: {e}")

    @staticmethod
    def _encode_file_content(content, file_type='csv'):
        """
        Encode file content to base64 for Graph API attachment.

        Args:
            content: Raw string content of the file.
            file_type: Type of file ('csv' or 'txt'). Both use UTF-8 encoding.

        Returns:
            Base64-encoded string suitable for Graph API contentBytes field.
        """
        # Both csv and txt are text-based, encode as UTF-8
        if isinstance(content, str):
            content_bytes = content.encode('utf-8')
        else:
            content_bytes = content

        return base64.b64encode(content_bytes).decode('utf-8')

    def _add_attachment(self, draft_mail_id, filename, content_bytes, headers):
        """
        Add a file attachment to a draft email via Graph API.

        Args:
            draft_mail_id: The ID of the draft message.
            filename: Name of the attachment file (e.g., 'violations.csv').
            content_bytes: Base64-encoded file content string.
            headers: Authorization headers dict.
        """
        try:
            user_id = configsettings.SENDER_EMAIL
            attachment_url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{draft_mail_id}/attachments"

            attachment_body = {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": filename,
                "contentBytes": content_bytes
            }

            response = requests.post(attachment_url, json=attachment_body, headers=headers)

            if response.status_code == 201:
                self.logger.info(f"Attachment '{filename}' added to draft {draft_mail_id}")
            else:
                self.logger.error(
                    f"Failed to add attachment '{filename}'. "
                    f"Status: {response.status_code}, Response: {response.text}"
                )

        except Exception as e:
            self.logger.error(f"Error adding attachment '{filename}': {e}")
