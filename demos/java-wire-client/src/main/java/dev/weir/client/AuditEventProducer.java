package dev.weir.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * Enterprise event-logging demo: a Java producer that streams structured audit
 * events to a weir daemon as durable, opaque JSON payloads.
 *
 * <p>Domain framing: weir is the durable write-ahead buffer in front of a
 * downstream SIEM / audit store. The producer is the application emitting
 * security-relevant events (logins, privilege changes, data exports). Each
 * event is pushed at {@code Sync} durability so the Ack means "on stable
 * storage" — the contract a compliance auditor cares about.
 *
 * <p>Usage:
 * <pre>
 *   java ... AuditEventProducer &lt;socket-path&gt; [event-count]
 * </pre>
 * Requires a running weir daemon at {@code socket-path}.
 */
public final class AuditEventProducer {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: AuditEventProducer <socket-path> [event-count]");
            System.exit(2);
        }
        Path socket = Path.of(args[0]);
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 5;

        System.out.println("=== weir enterprise audit-event producer (Java, stdlib-only) ===");
        System.out.println("socket: " + socket);
        System.out.println();

        try (WeirClient client = WeirClient.connect(socket)) {
            // 1. Liveness probe via HealthCheck (the correct no-payload frame).
            Frame hcr = client.healthCheck();
            System.out.println("[health] " + hcr + " -> daemon is live");

            // 2. Stream audit events at Sync durability.
            List<String> events = List.of(
                auditJson("auth.login", "user=alice", "result=success", "src=10.0.0.5"),
                auditJson("auth.login", "user=bob", "result=failure", "reason=bad_password"),
                auditJson("rbac.grant", "actor=admin", "subject=carol", "role=db_writer"),
                auditJson("data.export", "user=alice", "rows=12840", "dest=s3://audit-archive"),
                auditJson("auth.logout", "user=alice", "session=8c1f", "")
            );

            int acked = 0;
            for (int i = 0; i < count; i++) {
                String event = events.get(i % events.size());
                byte[] payload = event.getBytes(StandardCharsets.UTF_8);
                try {
                    Frame ack = client.push(payload, Wire.Durability.SYNC);
                    acked++;
                    System.out.printf("[push #%d] ACK (durable) %dB  %s%n",
                        i + 1, payload.length, event);
                } catch (ProtocolException pe) {
                    System.out.printf("[push #%d] NACK %s (retryable=%s)%n",
                        i + 1, pe.getMessage(), pe.isRetryable());
                    if (!pe.isRetryable()) {
                        // Permanent protocol error: the daemon has closed the connection.
                        throw pe;
                    }
                }
            }
            System.out.println();
            System.out.printf("Durably acked %d/%d audit events at Sync durability.%n", acked, count);
        }
    }

    /** Builds a minimal JSON audit record (opaque to weir). */
    private static String auditJson(String type, String... kvs) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"ts\":\"").append(Instant.now()).append("\"");
        sb.append(",\"type\":\"").append(type).append("\"");
        for (String kv : kvs) {
            if (kv == null || kv.isEmpty()) continue;
            int eq = kv.indexOf('=');
            String k = kv.substring(0, eq);
            String val = kv.substring(eq + 1);
            sb.append(",\"").append(k).append("\":\"").append(val).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }
}
