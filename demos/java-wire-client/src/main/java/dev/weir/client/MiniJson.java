package dev.weir.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A tiny, dependency-free JSON reader — just enough to parse the conformance
 * vectors file. Supports objects, arrays, strings (with \\uXXXX and standard
 * escapes), numbers, booleans, and null. Not a general-purpose parser; it
 * exists so the demo stays stdlib-only.
 */
public final class MiniJson {

    private final String s;
    private int i;

    private MiniJson(String s) {
        this.s = s;
    }

    public static Object parse(String json) {
        MiniJson p = new MiniJson(json);
        p.skipWs();
        Object v = p.readValue();
        p.skipWs();
        if (p.i != p.s.length()) {
            throw new IllegalArgumentException("trailing content at index " + p.i);
        }
        return v;
    }

    private Object readValue() {
        char c = peek();
        switch (c) {
            case '{': return readObject();
            case '[': return readArray();
            case '"': return readString();
            case 't': case 'f': return readBool();
            case 'n': expect("null"); return null;
            default:  return readNumber();
        }
    }

    private Map<String, Object> readObject() {
        Map<String, Object> m = new LinkedHashMap<>();
        expectChar('{');
        skipWs();
        if (peek() == '}') { i++; return m; }
        while (true) {
            skipWs();
            String key = readString();
            skipWs();
            expectChar(':');
            skipWs();
            m.put(key, readValue());
            skipWs();
            char c = next();
            if (c == '}') break;
            if (c != ',') throw err("expected ',' or '}'");
        }
        return m;
    }

    private List<Object> readArray() {
        List<Object> list = new ArrayList<>();
        expectChar('[');
        skipWs();
        if (peek() == ']') { i++; return list; }
        while (true) {
            skipWs();
            list.add(readValue());
            skipWs();
            char c = next();
            if (c == ']') break;
            if (c != ',') throw err("expected ',' or ']'");
        }
        return list;
    }

    private String readString() {
        expectChar('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            char c = next();
            if (c == '"') break;
            if (c == '\\') {
                char e = next();
                switch (e) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case '/': sb.append('/'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case 'u':
                        String hex = s.substring(i, i + 4);
                        i += 4;
                        sb.append((char) Integer.parseInt(hex, 16));
                        break;
                    default: throw err("bad escape \\" + e);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private Boolean readBool() {
        if (peek() == 't') { expect("true"); return Boolean.TRUE; }
        expect("false");
        return Boolean.FALSE;
    }

    private Number readNumber() {
        int start = i;
        while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
            i++;
        }
        String num = s.substring(start, i);
        if (num.contains(".") || num.contains("e") || num.contains("E")) {
            return Double.parseDouble(num);
        }
        return Long.parseLong(num);
    }

    private void expect(String token) {
        if (!s.startsWith(token, i)) throw err("expected '" + token + "'");
        i += token.length();
    }

    private void expectChar(char c) {
        if (next() != c) throw err("expected '" + c + "'");
    }

    private char peek() {
        if (i >= s.length()) throw err("unexpected end of input");
        return s.charAt(i);
    }

    private char next() {
        if (i >= s.length()) throw err("unexpected end of input");
        return s.charAt(i++);
    }

    private void skipWs() {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
    }

    private IllegalArgumentException err(String msg) {
        return new IllegalArgumentException("JSON parse error at index " + i + ": " + msg);
    }
}
