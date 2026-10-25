# weir-status

The `weir.status/v1` status model for [weir](https://github.com/miki-przygoda/weir):
the Conditions catalogue, the pure evaluator, the readiness/liveness/startup
predicates and the Prometheus-exposition parser. The daemon and `weir-ctl`
share it so that every consumer computes the same answer from the same facts.
It performs no I/O and reads no clock.
