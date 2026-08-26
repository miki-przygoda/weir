"""dm-flakey table construction and read-back.

Pure by design: no subprocess, no root, no device. The two failure modes that
matter are both silent, and both fail towards FALSE violations against weir —
so they are pinned by unit tests rather than discovered on the target machine.

1. Submit a table with no feature arguments and the kernel fills them in as
   `2 error_reads error_writes`. An innocuous-looking pass-through is an
   ERRORING device when down.
2. `dmsetup table` reports the underlying device as `major:minor`, not the path
   you submitted — so a naive round-trip comparison never matches.
"""

#: The only feature this phase injects. `error_writes` (fail-closed nacking) is
#: a different fault class and must not be relabelled power loss.
FEATURE = "drop_writes"


class UnexpectedTable(Exception):
    """A device-mapper table is not the flakey table we asked for."""


def flakey_table(device, sectors, engaged, down_secs=60):
    """One dm-flakey table line.

    `engaged` selects which interval covers the whole cycle:
      engaged   -> up=0, down=down_secs  (the fault is always active)
      disengaged-> up=down_secs, down=0  (the feature is inert)

    Both forms carry explicit feature args. Never emit a table without them.
    """
    if sectors <= 0:
        raise ValueError(f"sectors must be positive, got {sectors}")
    up, down = (0, down_secs) if engaged else (down_secs, 0)
    return f"0 {sectors} flakey {device} 0 {up} {down} 1 {FEATURE}"


def parse_table(line):
    """Splits a `dmsetup table` line into its fields.

    Returns a dict with `sectors`, `target`, `device`, `up`, `down`, `features`.
    Raises `UnexpectedTable` for anything that is not a single-feature
    `drop_writes` flakey table — including the erroring default the kernel
    substitutes when feature args are omitted.
    """
    parts = line.strip().split()
    if len(parts) < 8 or parts[2] != "flakey":
        raise UnexpectedTable(f"not a flakey table: {line!r}")
    features = parts[8:]
    if features != [FEATURE]:
        raise UnexpectedTable(
            f"expected exactly [{FEATURE!r}], got {features!r} — a table "
            f"submitted without feature args comes back as "
            f"['error_reads', 'error_writes'], which is an ERRORING device: {line!r}"
        )
    return {
        "sectors": int(parts[1]),
        "target": parts[2],
        "device": parts[3],
        "up": int(parts[5]),
        "down": int(parts[6]),
        "features": features,
    }


def table_is_engaged(line):
    """True if this table has the fault active (up=0, down>0)."""
    t = parse_table(line)
    return t["up"] == 0 and t["down"] > 0
