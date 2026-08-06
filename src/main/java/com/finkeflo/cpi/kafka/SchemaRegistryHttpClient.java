/*-
 * #%L
 * Kafka Adapter Plus
 * %%
 * Copyright (C) 2026 Florian Kube
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
package com.finkeflo.cpi.kafka;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal Confluent-compatible Schema Registry client using only JDK classes.
 * Replaces the Confluent {@code kafka-schema-registry-client} library (~4 MB of
 * transitive dependencies including Guava) with a lightweight HTTP client.
 *
 * <p>Supports:
 * <ul>
 *   <li>Fetching schemas by ID: {@code GET /schemas/ids/{id}}</li>
 *   <li>Fetching latest schema by subject: {@code GET /subjects/{subject}/versions/latest}</li>
 *   <li>Registering schemas: {@code POST /subjects/{subject}/versions}</li>
 *   <li>Basic Auth via {@code Authorization: Basic} header</li>
 *   <li>HTTPS via {@code HttpsURLConnection}</li>
 *   <li>In-memory schema cache (schema IDs are immutable in Confluent Schema Registry)</li>
 * </ul>
 */
final class SchemaRegistryHttpClient {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryHttpClient.class);

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
    private static final int DEFAULT_READ_TIMEOUT_MS    = 10_000;
    private static final int MAX_RESPONSE_BYTES         = 512 * 1024; // 512 KB

    private final String  baseUrl;
    private final String  authHeader; // null if no authentication required
    private final int     connectTimeoutMs;
    private final int     readTimeoutMs;

    /** Cache: schema ID → parsed Avro Schema (IDs are immutable in Schema Registry). */
    private final ConcurrentHashMap<Integer, Schema> idToSchema     = new ConcurrentHashMap<>();
    /** Cache: subject → latest schema ID. */
    private final ConcurrentHashMap<String, Integer> subjectToId    = new ConcurrentHashMap<>();
    /** Cache: subject → latest parsed Avro Schema. */
    private final ConcurrentHashMap<String, Schema>  subjectToSchema = new ConcurrentHashMap<>();

    SchemaRegistryHttpClient(String baseUrl, String username, String password) {
        this(baseUrl, username, password, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    SchemaRegistryHttpClient(String baseUrl, String username, String password,
                             int connectTimeoutMs, int readTimeoutMs) {
        this.baseUrl          = normalizeUrl(baseUrl);
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs    = readTimeoutMs;

        if (username != null && !username.isEmpty()) {
            String raw = username + ":" + (password != null ? password : "");
            this.authHeader = "Basic " + Base64.getEncoder()
                    .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
        } else {
            this.authHeader = null;
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Returns the Avro Schema for the given schema ID.
     * Result is cached; subsequent calls for the same ID return immediately.
     */
    Schema fetchSchemaById(int schemaId) throws Exception {
        Schema cached = idToSchema.get(schemaId);
        if (cached != null) {
            return cached;
        }
        String json      = get("/schemas/ids/" + schemaId);
        String schemaStr = extractStringField(json, "schema");
        Schema schema    = new Schema.Parser().parse(schemaStr);
        idToSchema.put(schemaId, schema);
        LOG.debug("[CPI-KAFKA-PLUS-DIAG] schema-registry: fetched schema id={}", schemaId);
        return schema;
    }

    /**
     * Returns the latest Avro Schema for the given subject.
     * Also populates the ID cache so {@link #getSchemaId} does not need an extra request.
     */
    Schema fetchSchemaBySubject(String subject) throws Exception {
        Schema cached = subjectToSchema.get(subject);
        if (cached != null) {
            return cached;
        }
        String json      = get("/subjects/" + urlEncode(subject) + "/versions/latest");
        String schemaStr = extractStringField(json, "schema");
        int    id        = extractIntField(json, "id");
        Schema schema    = new Schema.Parser().parse(schemaStr);
        idToSchema.put(id, schema);
        subjectToId.put(subject, id);
        subjectToSchema.put(subject, schema);
        LOG.debug("[CPI-KAFKA-PLUS-DIAG] schema-registry: fetched schema subject='{}' id={}", subject, id);
        return schema;
    }

    /**
     * Returns the schema ID for the latest version of the given subject.
     * Fetches the subject metadata if not already cached.
     */
    int getSchemaId(String subject) throws Exception {
        Integer cached = subjectToId.get(subject);
        if (cached != null) {
            return cached;
        }
        fetchSchemaBySubject(subject); // populates subjectToId as a side effect
        return subjectToId.get(subject);
    }

    /**
     * Registers a new schema version under the given subject and returns the assigned schema ID.
     * The response schema and ID are added to all caches.
     */
    int registerSchema(String subject, String schemaJson) throws Exception {
        String body     = "{\"schema\":" + toJsonString(schemaJson) + "}";
        String response = post("/subjects/" + urlEncode(subject) + "/versions", body);
        int    id       = extractIntField(response, "id");
        Schema schema   = new Schema.Parser().parse(schemaJson);
        idToSchema.put(id, schema);
        subjectToId.put(subject, id);
        subjectToSchema.put(subject, schema);
        LOG.info("[CPI-KAFKA-PLUS-DIAG] schema-registry: registered schema subject='{}' id={}", subject, id);
        return id;
    }

    // -----------------------------------------------------------------------
    // HTTP
    // -----------------------------------------------------------------------

    private String get(String path) throws Exception {
        HttpURLConnection conn = openConnection(path, "GET");
        try {
            int status = conn.getResponseCode();
            if (status == HttpURLConnection.HTTP_OK) {
                return readStream(conn.getInputStream());
            }
            String body = readStream(conn.getErrorStream());
            throw new SchemaRegistryException("GET " + path, status, body);
        } finally {
            conn.disconnect();
        }
    }

    private String post(String path, String body) throws Exception {
        HttpURLConnection conn = openConnection(path, "POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
        try (OutputStream out = conn.getOutputStream()) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
        try {
            int status = conn.getResponseCode();
            if (status == HttpURLConnection.HTTP_OK || status == HttpURLConnection.HTTP_CREATED) {
                return readStream(conn.getInputStream());
            }
            String responseBody = readStream(conn.getErrorStream());
            throw new SchemaRegistryException("POST " + path, status, responseBody);
        } finally {
            conn.disconnect();
        }
    }

    private HttpURLConnection openConnection(String path, String method) throws Exception {
        URL               url  = new URL(baseUrl + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json, application/json");
        conn.setConnectTimeout(connectTimeoutMs);
        conn.setReadTimeout(readTimeoutMs);
        conn.setUseCaches(false);
        if (authHeader != null) {
            conn.setRequestProperty("Authorization", authHeader);
        }
        return conn;
    }

    private static String readStream(InputStream is) throws Exception {
        if (is == null) {
            return "";
        }
        byte[] buf    = new byte[8192];
        int    total  = 0;
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {
            char[] cbuf = new char[4096];
            int n;
            while ((n = reader.read(cbuf)) != -1) {
                total += n * 2; // rough byte estimate
                if (total > MAX_RESPONSE_BYTES) {
                    sb.append("[truncated]");
                    break;
                }
                sb.append(cbuf, 0, n);
            }
        }
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // JSON field extraction (no external library)
    // -----------------------------------------------------------------------

    /**
     * Extracts the value of a JSON string field from a flat JSON object.
     * Handles {@code \"} and {@code \\} escape sequences within the value.
     * Package-private for unit testing.
     */
    static String extractStringField(String json, String field) {
        String key   = "\"" + field + "\":\"";
        int    start = json.indexOf(key);
        if (start < 0) {
            throw new IllegalArgumentException(
                    "Field '" + field + "' not found in Schema Registry response: " + abbreviate(json));
        }
        start += key.length();
        StringBuilder sb      = new StringBuilder();
        boolean       escaped = false;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escaped) {
                switch (c) {
                    case '"':  sb.append('"');  break;
                    case '\\': sb.append('\\'); break;
                    case 'n':  sb.append('\n'); break;
                    case 'r':  sb.append('\r'); break;
                    case 't':  sb.append('\t'); break;
                    default:   sb.append(c);    break;
                }
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                break;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Extracts the value of a JSON integer field from a flat JSON object.
     * Package-private for unit testing.
     */
    static int extractIntField(String json, String field) {
        String key   = "\"" + field + "\":";
        int    start = json.indexOf(key);
        if (start < 0) {
            throw new IllegalArgumentException(
                    "Field '" + field + "' not found in Schema Registry response: " + abbreviate(json));
        }
        start += key.length();
        while (start < json.length() && json.charAt(start) == ' ') {
            start++;
        }
        int end = start;
        while (end < json.length() && Character.isDigit(json.charAt(end))) {
            end++;
        }
        if (start == end) {
            throw new IllegalArgumentException(
                    "Field '" + field + "' has no integer value in: " + abbreviate(json));
        }
        return Integer.parseInt(json.substring(start, end));
    }

    /** Encodes a JSON string value including surrounding quotes. */
    static String toJsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:   sb.append(c);       break;
            }
        }
        return sb.append('"').toString();
    }

    // -----------------------------------------------------------------------
    // Utilities
    // -----------------------------------------------------------------------

    private static String normalizeUrl(String url) {
        if (url == null) return "";
        url = url.trim();
        while (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        return url;
    }

    private static String urlEncode(String s) {
        try {
            return java.net.URLEncoder.encode(s, "UTF-8").replace("+", "%20");
        } catch (Exception e) {
            return s;
        }
    }

    private static String abbreviate(String s) {
        return s != null && s.length() > 200 ? s.substring(0, 200) + "…" : s;
    }

    // -----------------------------------------------------------------------
    // Exception
    // -----------------------------------------------------------------------

    /** Wraps a non-2xx HTTP response from the Schema Registry. */
    static final class SchemaRegistryException extends RuntimeException {
        private final int httpStatus;

        SchemaRegistryException(String operation, int httpStatus, String body) {
            super("Schema Registry " + operation + " failed: HTTP " + httpStatus + " — " + abbreviate(body));
            this.httpStatus = httpStatus;
        }

        int getHttpStatus() {
            return httpStatus;
        }
    }
}
