"""Helpers for interpreting strategy-grid (d1d2) offsets.

Two offset formats are supported:

* **Integer offsets** (``-2``, ``0``, ``3``) — a positional step count along the QP
  fares ladder. The sign selects the direction of travel.
* **Multiplier offsets** (``X0.8``, ``X1.34``, ``X-1.12``) — a *price target* expressed
  as a multiple of the anchor fare. The ladder position is derived by snapping to the
  resolved target, so the numeric part is **not** a ladder step count.

Several call-sites need one of two questions answered about an offset:

1. "Which direction along the ladder does this move?" -> :func:`offset_snap_direction`
2. "How many ladder steps do I index by?"             -> :func:`ladder_step_count`

Keeping both answers here avoids the previous pattern of coercing multiplier offsets
to ``0`` at each call-site, which silently discarded the direction.
"""

OFFSET_UP = 1
OFFSET_DOWN = -1
OFFSET_MATCH = 0


def is_multiplier_offset(offset) -> bool:
    """True when ``offset`` is a multiplier offset (an ``X``-prefixed value)."""
    try:
        return str(offset).strip().upper().startswith('X')
    except Exception:
        return False


def parse_multiplier_offset(offset):
    """Return the numeric multiplier for an ``X``-prefixed offset.

    Returns ``None`` when ``offset`` is not a parseable multiplier offset, which lets
    callers fall back to integer-offset handling. Case-insensitive, so ``x0`` and
    ``X0`` behave identically.
    """
    try:
        text = str(offset).strip().upper()
    except Exception:
        return None
    if not text.startswith('X'):
        return None
    try:
        return float(text[1:])
    except ValueError:
        return None


def offset_snap_direction(offset):
    """Direction the offset travels along the fares ladder.

    Returns :data:`OFFSET_UP`, :data:`OFFSET_DOWN` or :data:`OFFSET_MATCH`.

    Multiplier offsets mirror ``DetermineRBD.resolve_multiplier_offset``: ``X0`` matches
    the anchor exactly, a multiplier of ``1`` or more moves up, anything else (a
    fractional or negative multiplier) moves down.

    Integer offsets keep the legacy rule: ``>= 0`` moves up, ``< 0`` moves down.

    Unparseable values default to :data:`OFFSET_UP`, matching the legacy behaviour for
    offset ``0``.
    """
    multiplier = parse_multiplier_offset(offset)
    if multiplier is not None:
        if multiplier == 0:
            return OFFSET_MATCH
        return OFFSET_UP if multiplier >= 1 else OFFSET_DOWN
    try:
        return OFFSET_UP if int(offset) >= 0 else OFFSET_DOWN
    except (TypeError, ValueError):
        return OFFSET_UP


def ladder_step_count(offset):
    """Positional step count to index by on an already direction-sorted ladder.

    Multiplier offsets resolve to ``0``: the ladder produced by
    ``DetermineRBD.apply_multiplier_offset_snap`` is already sorted so that index ``0``
    is the snapped target fare, so the multiplier must not be reused as a step count.

    Integer offsets return their own value. Unparseable values return ``0``.
    """
    if is_multiplier_offset(offset):
        return 0
    try:
        return int(offset)
    except (TypeError, ValueError):
        return 0
