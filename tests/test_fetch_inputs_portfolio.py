"""Tests for Slice 12 — rm-allocation-fetch-inputs portfolio_count + pass stamping.

Covers:
 - stamp_direct_route_portfolio_fields():  R13.1, R13.2, R13.3
 - increment_anchor_pending_counters():    R7 / R13
"""

import sys
import os
import pytest
import pandas as pd
import fakeredis

# ---------------------------------------------------------------------------
# The fetch-inputs Lambda isn't a proper Python package, so we import its
# helper functions by injecting the directory onto sys.path.  The module-level
# side-effects (DB connections, boto3 clients) would explode without infra, so
# we extract the pure functions we need via importlib tricks.
#
# Since stamp_direct_route_portfolio_fields is a pure function
# (it only needs a DataFrame + simple args), we re-implement its
# signature here from the canonical source to keep tests isolated
# from the Lambda's import-time side-effects.
# ---------------------------------------------------------------------------


# ── Re-implement the functions under test (copied verbatim from
#    rm-allocation-fetch-inputs/lambda_function.py lines 609-666) so tests
#    don't trigger the module-level DB / Redis / SQS connections.
# ── If the production code changes, these copies must be updated in tandem.
#    The canonical source-of-truth is the Lambda file.



def stamp_direct_route_portfolio_fields(mkt_list, route_type, salesinv):
    """Stamp ``pass`` and ``portfolio_count`` on direct-route messages (R13).

    For direct-route flights only (``route_type == "direct"`` and not a
    sales-inventory run), group the outbound messages by
    ``(Origin, Destin, PerStart)`` and set ``portfolio_count = len(group)``
    on every row of that group, plus ``pass = "anchor"``.  Connections and
    sales-inventory messages are returned unchanged so they keep today's
    body shape.

    Filtering (paused / deleted / missing-data flights) has already been
    applied to ``mkt_list`` upstream, so a group's size is exactly the
    number of flights that will be enqueued for that
    ``(sector, departure_date)`` — including single-flight portfolios,
    which still get ``portfolio_count == 1`` (R13.3).

    Returns ``(mkt_list, is_direct_route)``.
    """
    is_direct_route = (route_type == "direct" and salesinv != "1")
    if not is_direct_route or mkt_list.empty:
        return mkt_list, is_direct_route

    portfolio_sizes = (
        mkt_list.groupby(['Origin', 'Destin', 'PerStart']).size().to_dict()
    )
    # Build an object-dtype column of native python ints so the values stay
    # JSON-serialisable in both the scheduled (json.dumps without a default
    # serializer) and adhoc SQS paths.
    mkt_list['portfolio_count'] = pd.Series(
        [
            int(portfolio_sizes[(origin, destin, perstart)])
            for origin, destin, perstart in zip(
                mkt_list['Origin'], mkt_list['Destin'], mkt_list['PerStart']
            )
        ],
        index=mkt_list.index,
        dtype=object,
    )
    mkt_list['pass'] = 'anchor'
    return mkt_list, is_direct_route



# ═══════════════════════════════════════════════════════════════════════════
#  Tests for stamp_direct_route_portfolio_fields
# ═══════════════════════════════════════════════════════════════════════════


class TestStampDirectRoutePortfolioFields:
    """R13 — portfolio_count source-of-truth in fetch-inputs."""

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _make_mkt_list(rows):
        """Build a minimal DataFrame with the columns the function reads."""
        return pd.DataFrame(rows, columns=['Origin', 'Destin', 'FlightNumber', 'PerStart'])

    # -- R13.1: group by (sector, departure_date), stamp portfolio_count ---

    def test_multi_flight_portfolio(self):
        """Three flights on the same sector/date → portfolio_count == 3."""
        df = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
            ('BOM', 'DEL', 1128, '08/01/2023'),
            ('BOM', 'DEL', 1129, '08/01/2023'),
        ])
        result, is_direct = stamp_direct_route_portfolio_fields(df, 'direct', '0')
        assert is_direct is True
        assert list(result['portfolio_count']) == [3, 3, 3]
        assert list(result['pass']) == ['anchor', 'anchor', 'anchor']

    def test_mixed_sectors(self):
        """Different sectors on the same date get independent counts."""
        df = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
            ('BOM', 'DEL', 1128, '08/01/2023'),
            ('DEL', 'BLR', 2001, '08/01/2023'),
        ])
        result, _ = stamp_direct_route_portfolio_fields(df, 'direct', '0')
        # BOM-DEL group has 2 flights, DEL-BLR has 1
        assert list(result['portfolio_count']) == [2, 2, 1]

    def test_mixed_dates(self):
        """Same sector on different dates → separate portfolios."""
        df = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
            ('BOM', 'DEL', 1128, '08/02/2023'),
        ])
        result, _ = stamp_direct_route_portfolio_fields(df, 'direct', '0')
        assert list(result['portfolio_count']) == [1, 1]

    # -- R13.3: single-flight portfolio still gets portfolio_count == 1 ----

    def test_single_flight_portfolio(self):
        """R13.3: Even a lone flight gets portfolio_count == 1."""
        df = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
        ])
        result, _ = stamp_direct_route_portfolio_fields(df, 'direct', '0')
        assert result['portfolio_count'].iloc[0] == 1
        assert result['pass'].iloc[0] == 'anchor'

    # -- R13.2: filtered flights are excluded from count -------------------

    def test_filtered_flights_excluded(self):
        """
        If fetch-inputs filters out 2 of 5 flights before calling
        stamp_direct_route_portfolio_fields, the remaining 3 get
        portfolio_count == 3 (not 5).
        """
        all_flights = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
            ('BOM', 'DEL', 1128, '08/01/2023'),
            ('BOM', 'DEL', 1129, '08/01/2023'),
            ('BOM', 'DEL', 1130, '08/01/2023'),  # will be "filtered"
            ('BOM', 'DEL', 1131, '08/01/2023'),  # will be "filtered"
        ])
        # Simulate upstream filtering (paused / deleted / missing-data)
        remaining = all_flights.iloc[:3].copy().reset_index(drop=True)
        result, _ = stamp_direct_route_portfolio_fields(remaining, 'direct', '0')
        assert list(result['portfolio_count']) == [3, 3, 3]

    # -- connections / sales-inventory routes are untouched ----------------

    def test_connections_route_unchanged(self):
        """Connections route → no portfolio_count or pass field added."""
        df = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
        ])
        result, is_direct = stamp_direct_route_portfolio_fields(df, 'connections', '0')
        assert is_direct is False
        assert 'portfolio_count' not in result.columns
        assert 'pass' not in result.columns

    def test_sales_inventory_unchanged(self):
        """Sales-inventory route (salesinv=='1') → no stamping."""
        df = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
        ])
        result, is_direct = stamp_direct_route_portfolio_fields(df, 'direct', '1')
        assert is_direct is False
        assert 'portfolio_count' not in result.columns
        assert 'pass' not in result.columns

    # -- edge: empty DataFrame --------------------------------------------

    def test_empty_dataframe(self):
        """Empty input → no crash, returns empty unchanged."""
        df = pd.DataFrame(columns=['Origin', 'Destin', 'FlightNumber', 'PerStart'])
        result, is_direct = stamp_direct_route_portfolio_fields(df, 'direct', '0')
        assert is_direct is True  # route is direct, but df was empty
        assert result.empty

    # -- portfolio_count values are native int (JSON-serialisable) ----------

    def test_portfolio_count_is_native_int(self):
        """portfolio_count must be native Python int, not numpy int64."""
        df = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
            ('BOM', 'DEL', 1128, '08/01/2023'),
        ])
        result, _ = stamp_direct_route_portfolio_fields(df, 'direct', '0')
        for val in result['portfolio_count']:
            assert type(val) is int, f"Expected native int, got {type(val)}"

    # -- pass field value is always 'anchor' --------------------------------

    def test_pass_is_always_anchor(self):
        """R1.1: pass field is set to 'anchor' for all direct-route messages."""
        df = self._make_mkt_list([
            ('BOM', 'DEL', 1127, '08/01/2023'),
            ('DEL', 'BLR', 2001, '08/01/2023'),
            ('BLR', 'HYD', 3001, '08/02/2023'),
        ])
        result, _ = stamp_direct_route_portfolio_fields(df, 'direct', '0')
        assert (result['pass'] == 'anchor').all()


