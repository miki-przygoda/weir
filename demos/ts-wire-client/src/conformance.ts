/**
 * Conformance harness: runs every vector in docs/conformance/wire_v1_vectors.json
 * against this client's encoder + decoder.
 *
 *   - "ok" vectors: decode must succeed and match the declared header/payload,
 *     AND re-encoding the decoded frame must reproduce the exact bytes (round-trip).
 *   - rejection vectors: decode must throw a DecodeError whose tag matches.
 *
 * Run:  node src/conformance.ts [path/to/wire_v1_vectors.json]
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  DecodeError,
  decodeFrame,
  durabilityName,
  encodeFrame,
  messageTypeName,
} from "./wire.ts";

interface Vector {
  name: string;
  notes?: string;
  hex: string;
  decode: string;
  message_type?: string;
  durability?: string;
  flags?: number;
  payload_hex?: string;
}

// Canonical vectors resolved relative to the repo (this file is at
// demos/ts-wire-client/src/), or via the WEIR_VECTORS env var. No vendored copy.
const DEFAULT_VECTORS =
  process.env.WEIR_CONFORMANCE_VECTORS ??
  fileURLToPath(
    new URL("../../../docs/conformance/wire_v1_vectors.json", import.meta.url),
  );

function run(path: string): number {
  const doc = JSON.parse(readFileSync(path, "utf8")) as {
    vectors: Vector[];
    max_payload_hard_cap: number;
  };
  const cap = doc.max_payload_hard_cap;
  let pass = 0;
  let fail = 0;
  const failures: string[] = [];

  for (const v of doc.vectors) {
    const buf = Buffer.from(v.hex, "hex");
    try {
      if (v.decode === "ok") {
        const decoded = decodeFrame(buf, cap);
        // Check decoded fields against the vector.
        const mtName = messageTypeName(decoded.messageType);
        const durName = durabilityName(decoded.durability);
        const errs: string[] = [];
        if (v.message_type && mtName !== v.message_type)
          errs.push(`message_type ${mtName} != ${v.message_type}`);
        if (v.durability && durName !== v.durability)
          errs.push(`durability ${durName} != ${v.durability}`);
        if (v.flags !== undefined && decoded.flags !== v.flags)
          errs.push(`flags ${decoded.flags} != ${v.flags}`);
        const pHex = decoded.payload.toString("hex");
        if (v.payload_hex !== undefined && pHex !== v.payload_hex)
          errs.push(`payload ${pHex} != ${v.payload_hex}`);

        // Round-trip: re-encode and compare to the original bytes.
        const reEncoded = encodeFrame(decoded.payload, {
          messageType: decoded.messageType,
          durability: decoded.durability,
          flags: decoded.flags,
        });
        if (!reEncoded.equals(buf))
          errs.push(`re-encode mismatch: ${reEncoded.toString("hex")} != ${v.hex}`);

        if (errs.length) {
          fail++;
          failures.push(`  FAIL ${v.name}: ${errs.join("; ")}`);
        } else {
          pass++;
        }
      } else {
        // rejection vector
        try {
          decodeFrame(buf, cap);
          fail++;
          failures.push(`  FAIL ${v.name}: expected ${v.decode}, decoded OK`);
        } catch (e) {
          if (e instanceof DecodeError && e.tag === v.decode) {
            pass++;
          } else {
            fail++;
            const got = e instanceof DecodeError ? e.tag : String(e);
            failures.push(`  FAIL ${v.name}: expected ${v.decode}, got ${got}`);
          }
        }
      }
    } catch (e) {
      fail++;
      failures.push(`  FAIL ${v.name}: unexpected throw ${String(e)}`);
    }
  }

  console.log(`conformance: ${pass}/${doc.vectors.length} passed`);
  if (failures.length) {
    console.log(failures.join("\n"));
  }
  return fail;
}

const path = process.argv[2] ?? DEFAULT_VECTORS;
const fail = run(path);
process.exit(fail === 0 ? 0 : 1);
