import json

from Constants import *


class RedisHelper:

    # ------------------------------------------------------------------
    # Lua scripts for portfolio coordination (R3)
    # ------------------------------------------------------------------
    # Atomically inserts a portfolio entry, refreshes the TTL, and
    # decides whether the calling invocation owns the trigger that
    # fans out the allocation pass.
    #
    # KEYS[1] = portfolio hash key (Portfolio:{run_id}_{sector}_{dep_date})
    # ARGV[1] = flight_uid (HSET field name)
    # ARGV[2] = entry JSON
    # ARGV[3] = portfolio_count (string int)
    # ARGV[4] = TTL seconds (string int)
    #
    # Returns {active_entry_count, won_flag} where:
    #   - active_entry_count excludes the 'completed' sentinel field.
    #   - won_flag is 1 only for the first invocation that fills the
    #     portfolio AND atomically claims the 'completed' sentinel.
    INSERT_AND_CHECK_TRIGGER = """
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    redis.call('EXPIRE', KEYS[1], ARGV[4])

    local completed_exists = redis.call('HEXISTS', KEYS[1], 'completed')
    local n = redis.call('HLEN', KEYS[1]) - completed_exists

    if n < tonumber(ARGV[3]) then
        return {n, 0}
    end

    local won = redis.call('HSETNX', KEYS[1], 'completed', 'true')
    if won == 1 then
        return {n, 1}
    else
        return {n, 0}
    end
    """

    # Fetch all flight entries from the portfolio hash, filtering out the
    # ``completed`` sentinel field server-side so it never leaks into the
    # returned list.  The winning invocation (the one that claimed the
    # trigger via INSERT_AND_CHECK_TRIGGER) calls this to gather every
    # entry for aggregate computation and fan-out.  Duplicate-fan-out
    # protection comes from the atomic ``won`` flag, not from this read
    # (R3.3 / R3.4), so this returns entries regardless of completion.
    #
    # KEYS[1] = portfolio hash key
    FETCH_PORTFOLIO_ENTRIES_SCRIPT = """
    local all = redis.call('HGETALL', KEYS[1])
    local out = {}
    for i = 1, #all, 2 do
        if all[i] ~= 'completed' then
            out[#out + 1] = all[i + 1]
        end
    end
    return out
    """

    def __init__(self, runid, cache_client, input_source):
        self.runId = runid
        self.cache_client = cache_client
        self.inputSource = input_source

    def updateRunAudit(self):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            self.cache_client.incr(KEY_B2B_DONE_COUNT + self.runId)
            self.cache_client.incr(KEY_B2C_DONE_COUNT + self.runId)

    def updateRunAuditReady(self):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            self.cache_client.incr(KEY_B2B_READY_COUNT + self.runId)
            self.cache_client.incr(KEY_B2C_READY_COUNT + self.runId)

    def updateAnchorRunAuditReady(self):
        """Increment the fare-anchor (F) ready counters for this run.

        Mirrors :py:meth:`updateRunAuditReady` but targets the F-tagged
        counter pair so the orchestrator can distinguish flights that
        have started the fare-anchor pass from those that have started
        the allocation pass.  See R7.1.
        """
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            self.cache_client.incr(KEY_B2B_READY_COUNT_F + self.runId)
            self.cache_client.incr(KEY_B2C_READY_COUNT_F + self.runId)

    def setAllRunStatus2Ready(self):
        b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + self.runId)
        b2cPendingCount = self.cache_client.get(KEY_B2C_PENDING_COUNT + self.runId)
        self.cache_client.set(KEY_B2B_READY_COUNT + self.runId, b2bPendingCount)
        self.cache_client.set(KEY_B2C_READY_COUNT + self.runId, b2cPendingCount)

    # ------------------------------------------------------------------
    # Portfolio coordination (R3)
    # ------------------------------------------------------------------
    @staticmethod
    def _portfolio_key(run_id, sector, dep_date):
        return f"{PORTFOLIO_KEY_PREFIX}{run_id}_{sector}_{dep_date}"

    def insert_portfolio_entry(
        self, run_id, sector, dep_date, flight_uid, entry_json, portfolio_count
    ):
        """Atomically insert a fare-anchor entry and check the trigger.

        Returns a tuple ``(active_entry_count, won)``:

        * ``active_entry_count`` — number of distinct flights now in
          the portfolio hash (excludes the ``completed`` sentinel).
        * ``won`` — ``True`` only for the first invocation that fills
          the portfolio AND atomically claims the trigger.  Duplicate
          inserts of the same ``flight_uid`` and inserts after another
          invocation has already won SHALL return ``False``.

        See R3.1 - R3.5.
        """
        hkey = self._portfolio_key(run_id, sector, dep_date)
        result = self.cache_client.eval(
            self.INSERT_AND_CHECK_TRIGGER,
            1,
            hkey,
            flight_uid,
            entry_json,
            str(portfolio_count),
            str(PORTFOLIO_TTL_SEC),
        )
        # Lua tables come back as Python lists; the second element is
        # 0 / 1 which we coerce to a bool.
        active_count = int(result[0])
        won = bool(int(result[1]))
        return active_count, won

    def fetch_portfolio_entries(self, run_id, sector, dep_date):
        """Return the JSON-decoded flight entries for a portfolio.

        Returns every flight entry currently in the portfolio hash,
        decoded back into Python dicts.  The ``completed`` sentinel
        field is filtered out server-side and never appears in the
        returned list.

        The winning fare-anchor-pass invocation (the one that received
        ``won=True`` from :py:meth:`insert_portfolio_entry`) calls this
        to gather the portfolio for aggregate computation and fan-out.
        Duplicate-fan-out protection is provided by the atomic ``won``
        flag (R3.3 / R3.4), so this read intentionally returns entries
        regardless of whether the ``completed`` sentinel is set.
        """
        hkey = self._portfolio_key(run_id, sector, dep_date)
        raw_values = self.cache_client.eval(
            self.FETCH_PORTFOLIO_ENTRIES_SCRIPT, 1, hkey
        )
        return [json.loads(v) for v in raw_values]

    def mark_portfolio_done(self, run_id, sector, dep_date):
        """Mark the portfolio as fanned out.

        Sets the ``completed`` sentinel on the portfolio hash and
        refreshes the TTL.  Subsequent ``insert_portfolio_entry`` calls
        for the same key SHALL return ``won=False`` (R3.4).
        """
        hkey = self._portfolio_key(run_id, sector, dep_date)
        self.cache_client.hset(hkey, "completed", "true")
        self.cache_client.expire(hkey, PORTFOLIO_TTL_SEC)
