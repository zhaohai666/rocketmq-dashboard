/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.dashboard.architecture.impl.cloud;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shared HTTP utility for cloud provider API calls.
 *
 * <p>Provides common HTTP operations, JSON parsing, and cryptographic
 * signing helpers used by all cloud provider adapters. Uses Java's
 * built-in {@link HttpClient} (available since Java 11) to avoid
 * adding heavy external HTTP client dependencies.</p>
 *
 * <p>Per RIP-1 ARCH-01, cloud providers communicate via REST APIs.
 * This class centralizes HTTP handling, response parsing, and
 * error management for all cloud adapters.</p>
 */
public final class CloudApiHttpClient {

    private static final Logger log = LoggerFactory.getLogger(CloudApiHttpClient.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final HttpClient httpClient;

    public CloudApiHttpClient() {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }

    // ==================== HTTP Operations ====================

    /**
     * Execute an HTTP GET request.
     *
     * @param url     the full URL with query parameters
     * @param headers additional headers (key-value pairs)
     * @return the response body as a string
     * @throws CloudApiException if the request fails or returns a non-2xx status
     */
    public String get(String url, Map<String, String> headers) throws CloudApiException {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(DEFAULT_TIMEOUT)
                .GET();
            addHeaders(builder, headers);

            log.debug("HTTP GET: {}", url);
            HttpResponse<String> response = httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());

            checkResponse(response);
            return response.body();
        } catch (CloudApiException e) {
            throw e;
        } catch (Exception e) {
            throw new CloudApiException("HTTP GET failed: " + url, e);
        }
    }

    /**
     * Execute an HTTP POST request with a JSON body.
     *
     * @param url         the full URL
     * @param jsonBody    the JSON request body
     * @param headers     additional headers
     * @return the response body as a string
     * @throws CloudApiException if the request fails
     */
    public String postJson(String url, String jsonBody, Map<String, String> headers) throws CloudApiException {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(DEFAULT_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                .header("Content-Type", "application/json");
            addHeaders(builder, headers);

            log.debug("HTTP POST: {} body={}", url, jsonBody);
            HttpResponse<String> response = httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());

            checkResponse(response);
            return response.body();
        } catch (CloudApiException e) {
            throw e;
        } catch (Exception e) {
            throw new CloudApiException("HTTP POST failed: " + url, e);
        }
    }

    /**
     * Execute an HTTP PUT request with a JSON body.
     */
    public String putJson(String url, String jsonBody, Map<String, String> headers) throws CloudApiException {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(DEFAULT_TIMEOUT)
                .PUT(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                .header("Content-Type", "application/json");
            addHeaders(builder, headers);

            log.debug("HTTP PUT: {} body={}", url, jsonBody);
            HttpResponse<String> response = httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());

            checkResponse(response);
            return response.body();
        } catch (CloudApiException e) {
            throw e;
        } catch (Exception e) {
            throw new CloudApiException("HTTP PUT failed: " + url, e);
        }
    }

    /**
     * Execute an HTTP DELETE request.
     */
    public String delete(String url, Map<String, String> headers) throws CloudApiException {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(DEFAULT_TIMEOUT)
                .DELETE();
            addHeaders(builder, headers);

            log.debug("HTTP DELETE: {}", url);
            HttpResponse<String> response = httpClient.send(builder.build(),
                HttpResponse.BodyHandlers.ofString());

            checkResponse(response);
            return response.body();
        } catch (CloudApiException e) {
            throw e;
        } catch (Exception e) {
            throw new CloudApiException("HTTP DELETE failed: " + url, e);
        }
    }

    // ==================== JSON Helpers ====================

    /**
     * Parse JSON string to a Map.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseJson(String json) throws CloudApiException {
        try {
            return OBJECT_MAPPER.readValue(json, Map.class);
        } catch (Exception e) {
            throw new CloudApiException("Failed to parse JSON response: " + json, e);
        }
    }

    /**
     * Parse JSON string to a typed object.
     */
    public static <T> T parseJson(String json, Class<T> clazz) throws CloudApiException {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            throw new CloudApiException("Failed to parse JSON to " + clazz.getSimpleName(), e);
        }
    }

    /**
     * Parse JSON string to a typed reference (for generic types).
     */
    public static <T> T parseJson(String json, TypeReference<T> typeRef) throws CloudApiException {
        try {
            return OBJECT_MAPPER.readValue(json, typeRef);
        } catch (Exception e) {
            throw new CloudApiException("Failed to parse JSON with type reference", e);
        }
    }

    /**
     * Convert an object to JSON string.
     */
    public static String toJson(Object obj) throws CloudApiException {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new CloudApiException("Failed to serialize object to JSON", e);
        }
    }

    /**
     * Safely extract a string value from a JSON map.
     */
    public static String getString(Map<String, Object> map, String key) {
        Object val = map.get(key);
        return val != null ? val.toString() : null;
    }

    /**
     * Safely extract a long value from a JSON map.
     */
    public static long getLong(Map<String, Object> map, String key, long defaultVal) {
        Object val = map.get(key);
        if (val == null) {
            return defaultVal;
        }
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        try {
            return Long.parseLong(val.toString());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    /**
     * Safely extract an int value from a JSON map.
     */
    public static int getInt(Map<String, Object> map, String key, int defaultVal) {
        Object val = map.get(key);
        if (val == null) {
            return defaultVal;
        }
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }
        try {
            return Integer.parseInt(val.toString());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    /**
     * Safely extract a boolean value from a JSON map.
     */
    public static boolean getBoolean(Map<String, Object> map, String key, boolean defaultVal) {
        Object val = map.get(key);
        if (val == null) {
            return defaultVal;
        }
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        return Boolean.parseBoolean(val.toString());
    }

    // ==================== Cryptographic Helpers ====================

    /**
     * Compute HMAC-SHA1 signature.
     *
     * @param data      the data to sign
     * @param secretKey the secret key
     * @return Base64-encoded signature
     */
    public static String hmacSha1(String data, String secretKey) throws CloudApiException {
        try {
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA1"));
            byte[] hash = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return java.util.Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new CloudApiException("HMAC-SHA1 computation failed", e);
        }
    }

    /**
     * Compute HMAC-SHA256 signature.
     *
     * @param data      the data to sign
     * @param secretKey the secret key as bytes
     * @return raw signature bytes
     */
    public static byte[] hmacSha256(byte[] secretKey, String data) throws CloudApiException {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secretKey, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new CloudApiException("HMAC-SHA256 computation failed", e);
        }
    }

    /**
     * Compute HMAC-SHA256 signature and return as hex string.
     */
    public static String hmacSha256Hex(byte[] secretKey, String data) throws CloudApiException {
        byte[] hash = hmacSha256(secretKey, data);
        return bytesToHex(hash);
    }

    /**
     * Compute SHA-256 hash and return as hex string.
     */
    public static String sha256Hex(String data) throws CloudApiException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new CloudApiException("SHA-256 computation failed", e);
        }
    }

    /**
     * Compute SHA-256 hash of bytes and return as hex string.
     */
    public static String sha256Hex(byte[] data) throws CloudApiException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new CloudApiException("SHA-256 computation failed", e);
        }
    }

    // ==================== URL / Query Helpers ====================

    /**
     * URL-encode a value for use in query parameters (Aliyun style: + → %20, * → %2A, etc.).
     */
    public static String percentEncode(String value) {
        if (value == null) {
            return "";
        }
        return URLEncoder.encode(value, StandardCharsets.UTF_8)
            .replace("+", "%20")
            .replace("*", "%2A")
            .replace("%7E", "~");
    }

    /**
     * Build a query string from a map of parameters.
     */
    public static String buildQueryString(Map<String, String> params) {
        return params.entrySet().stream()
            .map(e -> percentEncode(e.getKey()) + "=" + percentEncode(e.getValue()))
            .collect(Collectors.joining("&"));
    }

    /**
     * Build a full URL from base URL and query parameters.
     */
    public static String buildUrl(String baseUrl, Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return baseUrl;
        }
        String query = buildQueryString(params);
        String separator = baseUrl.contains("?") ? "&" : "?";
        return baseUrl + separator + query;
    }

    /**
     * Convert bytes to lowercase hex string.
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * Create an ordered (LinkedHashMap) for building sorted query parameters.
     */
    public static LinkedHashMap<String, String> orderedParams() {
        return new LinkedHashMap<>();
    }

    // ==================== Internal Helpers ====================

    private void addHeaders(HttpRequest.Builder builder, Map<String, String> headers) {
        if (headers != null) {
            headers.forEach(builder::header);
        }
    }

    private void checkResponse(HttpResponse<String> response) throws CloudApiException {
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            String body = response.body();
            log.error("HTTP {} {} returned status {}: {}",
                response.request().method(),
                response.request().uri(),
                statusCode,
                body != null && body.length() > 500 ? body.substring(0, 500) + "..." : body);
            throw new CloudApiException(
                "HTTP request failed with status " + statusCode + ": " + body, statusCode);
        }
    }

    // ==================== Exception Class ====================

    /**
     * Exception thrown when a cloud API HTTP call fails.
     */
    public static class CloudApiException extends Exception {

        private final int httpStatusCode;

        public CloudApiException(String message, Throwable cause) {
            super(message, cause);
            this.httpStatusCode = -1;
        }

        public CloudApiException(String message, int httpStatusCode) {
            super(message);
            this.httpStatusCode = httpStatusCode;
        }

        public CloudApiException(String message) {
            super(message);
            this.httpStatusCode = -1;
        }

        public int getHttpStatusCode() {
            return httpStatusCode;
        }
    }
}
