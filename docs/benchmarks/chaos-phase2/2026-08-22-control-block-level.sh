#!/bin/bash
# dm-flakey control experiment, BLOCK LEVEL (no filesystem in the way).
# Question: does drop_writes discard a write that fsync reported as succeeded,
# and does the SAME stack persist writes when drop_writes is not engaged?
set -u

# Needs root for losetup/dmsetup/blockdev. Run it as root (`sudo bash <this>`);
# if it is not root it falls back to plain `sudo` per command, which will prompt.
if [ "$(id -u)" -eq 0 ]; then S=""; else S="sudo"; fi

WORK=${WORK:-$(mktemp -d -t weir-flakey-control-XXXXXX)}
mkdir -p "$WORK"
IMG=$WORK/backing.img
truncate -s 256M "$IMG"
LOOP=$($S losetup --find --show "$IMG")
echo "loop=$LOOP"
SECTORS=$($S blockdev --getsz "$LOOP")
echo "sectors=$SECTORS"

# `dmsetup remove` can return BEFORE udev has released the device node. Creating
# the next mapping immediately then fails with EBUSY — and, far worse, leaves the
# PREVIOUS table in place, so the next experiment silently measures the old
# configuration and reports a verdict about a device it never created. That is
# exactly what happened when this script was first run as root: the sudo
# round-trip per command had been hiding the race.
remove_dm() {
  $S dmsetup remove --retry "$1" 2>/dev/null || $S dmsetup remove "$1" 2>/dev/null
  for _ in $(seq 1 50); do
    $S dmsetup info "$1" >/dev/null 2>&1 || return 0
    sleep 0.1
  done
  echo "FATAL: dm device '$1' still present after remove" >&2
  exit 1
}

# A failed create must ABORT. Carrying on produces a verdict about whatever
# mapping happened to be there instead, which is worse than no result.
create_dm() {
  local name=$1; shift
  if ! $S dmsetup create "$name" --table "$*"; then
    echo "FATAL: could not create dm device '$name' with table: $*" >&2
    exit 1
  fi
}

cleanup() {
  $S dmsetup remove --retry ctlflakey 2>/dev/null
  $S losetup -d "$LOOP" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

# Distinct 4KiB patterns so we can tell which write we are looking at.
python3 -c "import sys; sys.stdout.buffer.write(b'UP-INTERVAL-WRITE'.ljust(4096,b'A'))" > "$WORK/up.bin"
python3 -c "import sys; sys.stdout.buffer.write(b'DOWN-DROPWRITES-WRITE'.ljust(4096,b'B'))" > "$WORK/down.bin"
# Zero the target sector on the backing store so a stale read cannot fake a pass.
$S dd if=/dev/zero of="$LOOP" bs=4096 count=1 seek=100 conv=fsync status=none

echo
echo "===== DIRECTION B: flakey present but UP (drop_writes NOT engaged) ====="
# up_interval=60 down_interval=0  -> always up, feature args inert
create_dm ctlflakey "0 $SECTORS flakey $LOOP 0 60 0 1 drop_writes"
$S dmsetup table ctlflakey
$S dd if="$WORK/up.bin" of=/dev/mapper/ctlflakey bs=4096 count=1 seek=100 conv=fsync status=none
echo "dd(up) exit=$?"
$S blockdev --flushbufs /dev/mapper/ctlflakey
remove_dm ctlflakey
# Read from the BACKING STORE, bypassing the flakey device entirely.
$S blockdev --flushbufs "$LOOP"
GOT_UP=$($S dd if="$LOOP" bs=4096 count=1 skip=100 status=none | head -c 21)
echo "backing store now holds: [$GOT_UP]"

echo
echo "===== DIRECTION A: flakey DOWN with drop_writes engaged ====="
# up_interval=0 down_interval=60 -> always down, drop_writes active
create_dm ctlflakey "0 $SECTORS flakey $LOOP 0 0 60 1 drop_writes"
$S dmsetup table ctlflakey
$S dd if="$WORK/down.bin" of=/dev/mapper/ctlflakey bs=4096 count=1 seek=100 conv=fsync status=none
RC=$?
echo "dd(down) exit=$RC   <-- 0 means fsync REPORTED SUCCESS"
$S blockdev --flushbufs /dev/mapper/ctlflakey 2>&1
echo "flushbufs exit=$?"
remove_dm ctlflakey
$S blockdev --flushbufs "$LOOP"
GOT_DOWN=$($S dd if="$LOOP" bs=4096 count=1 skip=100 status=none | head -c 21)
echo "backing store now holds: [$GOT_DOWN]"

echo
echo "===== VERDICT ====="
if [[ "$GOT_UP" != UP-INTERVAL-WRITE* ]]; then
  echo "DIRECTION B FAILED: an un-engaged flakey device did not persist a normal write."
  echo "RESULT=CONTROL_INVALID"
elif [[ "$GOT_DOWN" == DOWN-DROPWRITES-WRITE* ]]; then
  echo "DIRECTION A FAILED: drop_writes did NOT drop the write; it reached the medium."
  echo "RESULT=NO_DROP"
elif [ "$RC" -ne 0 ]; then
  echo "DIRECTION A INCONCLUSIVE: write was dropped BUT dd reported failure (exit $RC),"
  echo "so this is an error-returning disk, not a silently-lying one."
  echo "RESULT=DROPS_BUT_ERRORS"
else
  echo "BOTH DIRECTIONS HOLD:"
  echo "  - un-engaged flakey persists writes (B)"
  echo "  - engaged drop_writes discards a write while fsync returns success (A)"
  echo "RESULT=CONTROL_OK"
fi
