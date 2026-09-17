# weir-attest

Tamper-evident hash chain over sealed WAB segments for
[weir](https://github.com/miki-przygoda/weir).

A CRC32 is an error-detection code, not a MAC: anyone who edits a record can
recompute it in microseconds and the file reads back clean. This crate chains
each sealed segment over its records' already-frozen `RecordId`s, linked to the
previous segment's head, so editing one is *detectable* — provided the head was
observed somewhere the editor does not control. Detection, never prevention, and
not a signature. Driven by `weir-ctl attest seal|verify|head`; the daemon is
never involved and nothing here runs on the ack path.

See the [workspace README](https://github.com/miki-przygoda/weir).
