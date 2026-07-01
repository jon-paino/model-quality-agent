package com.striim.demo;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.webaction.anno.AdapterType;
import com.webaction.anno.PropertyTemplate;
import com.webaction.anno.PropertyTemplateProperty;
import com.webaction.metaRepository.MDCache;
import com.webaction.runtime.compiler.custom.Nondeterministic;
import com.webaction.runtime.components.openprocessor.StriimOpenProcessor;
import com.webaction.runtime.containers.IBatch;
import com.webaction.runtime.containers.WAEvent;
import com.webaction.security.Password;
import com.webaction.uuid.AuthToken;
import com.webaction.web.api.VaultAPI;

/**
 * FeatureOp -- key-based dynamic feature lookup from a Feast online store.
 *
 * <p>Refactored from the field team's CvsRestOp (App-CVS.java + RestCaller.java).
 * Kept: the HttpClient setup, the retry-with-exponential-backoff transport, the
 * vault-resolved password plumbing, and the property template. Stripped: the
 * ORM / JsonNodeEvent machinery, the per-key worker pool, and the dynamic-header
 * templating -- a Feast read is an idempotent single-shot GET, so events are
 * processed inline and ordering does not matter.
 *
 * <p>For each incoming event the OP reads the geohash entity key from the
 * event's {@code data[]} at the configured column index (FileReader/DSVParser
 * events are untyped, so positional access is used), POSTs to Feast
 * {@code /get-online-features}, parses the column-major response by
 * feature name, and merges the 10 dynamic features into {@code waevent.userdata}
 * keyed by name. The downstream ModelOp reads them back out of userdata. This
 * mirrors {@code model/oracle.py} {@code Oracle.lookup_dynamic}, the executable
 * spec for the Feast HTTP contract.
 *
 * <p>If Feast returns a non-PRESENT status, is missing a feature, or the call
 * fails after all retries, the event is logged as an error and dropped (no
 * {@code send()}); it is never scored against silently-defaulted features.
 */
@PropertyTemplate(name = "FeatureOp", type = AdapterType.process, properties = {
        @PropertyTemplateProperty(name = "FeastUrl", type = String.class, required = false,
                defaultValue = "http://127.0.0.1:6566"),
        @PropertyTemplateProperty(name = "FeatureView", type = String.class, required = false,
                defaultValue = "cell_dynamic_features_v1"),
        @PropertyTemplateProperty(name = "GeohashIndex", type = Integer.class, required = false,
                defaultValue = "0"),
        @PropertyTemplateProperty(name = "TimeoutSec", type = Integer.class, required = false,
                defaultValue = "5"),
        @PropertyTemplateProperty(name = "MaxRetryNum", type = Integer.class, required = false,
                defaultValue = "3"),
        @PropertyTemplateProperty(name = "MaxRetryDelaySec", type = Integer.class, required = false,
                defaultValue = "10"),
        @PropertyTemplateProperty(name = "EnableBasicAuth", type = Boolean.class, required = false,
                defaultValue = "false"),
        @PropertyTemplateProperty(name = "Username", type = String.class, required = false,
                defaultValue = ""),
        @PropertyTemplateProperty(name = "Password", type = Password.class, required = false,
                defaultValue = ""),
        @PropertyTemplateProperty(name = "EnableLogging", type = Boolean.class, required = false,
                defaultValue = "false"),
        // ---- Phase 1b: application-level counter MBean labels (Quality Agent) ----
        // The OP cannot know its own fully-qualified Striim name at runtime, so the
        // namespace/component that label its counter MBean are passed in via TQL.
        // They name the bean only; nothing functional depends on them. The agent
        // discovers the bean with the matching ObjectName pattern.
        @PropertyTemplateProperty(name = "MetricsNamespace", type = String.class, required = false,
                defaultValue = "qualitydemo"),
        @PropertyTemplateProperty(name = "MetricsComponent", type = String.class, required = false,
                defaultValue = "FeatureProc")
}, outputType = com.webaction.proc.events.WAEvent.class,
   inputType = com.webaction.proc.events.WAEvent.class)
public class FeatureOp extends StriimOpenProcessor {

    /**
     * The 10 dynamic feature names served by Feast, in feature_manifest.json
     * order. These names are the OP contract: the request feature refs and the
     * userdata keys both derive from them. Mirrors
     * model/features.py DYNAMIC_FEATURES.
     */
    private static final String[] DYNAMIC_FEATURES = {
            "trip_count_last_10min",
            "trip_count_last_1hr",
            "trip_count_last_4hr",
            "trip_count_last_24hr",
            "avg_fare_last_1hr",
            "unique_dropoff_zones_last_1hr",
            "cell_active_minutes_last_1hr",
            "trip_count_delta_10min",
            "trip_count_same_hour_yesterday",
            "trip_count_same_hour_last_week"
    };

    // Configuration (loaded in start()).
    private String feastUrl;
    private int geohashIndex;
    private int timeoutSec;
    private int maxRetryNum;
    private long maxRetryDelayMillis;
    private boolean enableBasicAuth;
    private String username;
    private String password;
    private boolean enableLogging;

    // Pre-built Feast feature refs, e.g. "cell_dynamic_features_v1:avg_fare_last_1hr".
    private List<String> featureRefs;

    private HttpClient httpClient;
    private ObjectMapper objectMapper;

    // Phase 1b: feature-store-miss counter, exposed to the Quality Agent as a
    // public DynamicMBean under the com.striim.metrics JMX domain. The counter
    // object always exists so counts accrue even if JMX registration fails; only
    // the agent's visibility of it depends on a successful register.
    private static final String JMX_DOMAIN = "com.striim.metrics";
    private static final String COUNTER_NAME = "feature_store_miss";
    private OpCounterMBean missCounter;
    private ObjectName missCounterObjectName;

    // Authentication token for vault property resolution.
    private final AuthToken token = MDCache.getInstance().getWASecurityManagerToken();

    @Override
    public void start() throws Exception {
        super.start();

        final Map<String, Object> props = getProperties();
        feastUrl = stripTrailingSlash(Objects.toString(props.get("FeastUrl"), "http://127.0.0.1:6566"));
        final String featureView = Objects.toString(props.get("FeatureView"), "cell_dynamic_features_v1");
        geohashIndex = parseInt(props.get("GeohashIndex"), 0);
        timeoutSec = parseInt(props.get("TimeoutSec"), 5);
        maxRetryNum = parseInt(props.get("MaxRetryNum"), 3);
        maxRetryDelayMillis = parseInt(props.get("MaxRetryDelaySec"), 10) * 1000L;
        enableBasicAuth = Boolean.parseBoolean(Objects.toString(props.get("EnableBasicAuth"), "false"));
        enableLogging = Boolean.parseBoolean(Objects.toString(props.get("EnableLogging"), "false"));
        username = Objects.toString(props.get("Username"), "");

        final Object pw = props.get("Password");
        password = (pw instanceof Password)
                ? getVaultProperty(((Password) pw).getPlain().toString())
                : Objects.toString(pw, "");

        // Build the Feast feature-ref list (view-prefixed), matching
        // oracle.py Oracle._feast_refs.
        featureRefs = new ArrayList<>(DYNAMIC_FEATURES.length);
        for (final String name : DYNAMIC_FEATURES) {
            featureRefs.add(featureView + ":" + name);
        }

        httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(timeoutSec))
                .build();
        objectMapper = new ObjectMapper();

        // Phase 1b: build and register the feature-store-miss counter MBean.
        final String metricsNs = Objects.toString(props.get("MetricsNamespace"), "qualitydemo");
        final String metricsComponent = Objects.toString(props.get("MetricsComponent"), "FeatureProc");
        registerCounter(metricsNs, metricsComponent);

        log("started: feastUrl=" + feastUrl + " featureView=" + featureView
                + " geohashIndex=" + geohashIndex + " refs=" + featureRefs.size());
    }

    /**
     * Builds the miss counter and registers it as a public DynamicMBean under
     * {@code com.striim.metrics:name=OpCounters.<ns>.<component>,type=OpMetrics},
     * the ObjectName the ModelQualityAgent discovers via a namespace-scoped
     * {@code queryNames}. Registration is idempotent across an UNLOAD/LOAD or
     * redeploy: a stale bean of the same name is unregistered first. A
     * registration failure is logged loudly but never fails start() -- the
     * counter still accrues, the agent simply reports the signal as UNKNOWN.
     */
    private void registerCounter(final String metricsNs, final String metricsComponent) {
        missCounter = new OpCounterMBean(COUNTER_NAME, metricsNs, metricsComponent);
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            missCounterObjectName = new ObjectName(JMX_DOMAIN
                    + ":name=OpCounters." + metricsNs + "." + metricsComponent + ",type=OpMetrics");
            if (mbs.isRegistered(missCounterObjectName)) {
                mbs.unregisterMBean(missCounterObjectName);
            }
            mbs.registerMBean(missCounter, missCounterObjectName);
            log("registered counter MBean " + missCounterObjectName);
        } catch (final Exception e) {
            logError("could not register counter MBean (counts still accrue locally): " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            final IBatch<WAEvent> batch = getAdded();
            if (batch == null) {
                return;
            }

            for (final WAEvent event : batch) {
                // The SDK WAEvent wraps the runtime WAEvent in its data field.
                final com.webaction.proc.events.WAEvent waevent =
                        (com.webaction.proc.events.WAEvent) event.data;
                if (waevent == null || waevent.data == null) {
                    continue;
                }

                // Phase 1b denominator: a real row entering FeatureOp. Counted
                // before extraction so a malformed-geohash drop still counts as an
                // event seen (it is bad input, not a feature-store miss). The miss
                // feed uses only well-formed geohashes, so EventsSeen == N there.
                missCounter.recordEvent();

                final String geohash = extractGeohash(waevent);
                if (geohash == null || geohash.isEmpty()) {
                    logError("dropping event: no geohash value at data[" + geohashIndex + "]");
                    continue;
                }

                final Map<String, Double> dynamic = lookupDynamic(geohash);
                if (dynamic == null) {
                    // extractGeohash / lookupDynamic already logged the cause.
                    // Phase 1b numerator: the feature-store miss. A non-PRESENT
                    // Feast status, a missing feature, or a call failure after
                    // retries all land here; this is the drop the agent counts.
                    missCounter.recordFault();
                    logError("dropping event: dynamic feature lookup failed for geohash=" + geohash);
                    continue;
                }

                // Merge the dynamic vector into userdata, keyed by feature name.
                if (waevent.userdata == null) {
                    waevent.userdata = new HashMap<>();
                }
                waevent.userdata.putAll(dynamic);

                log("merged " + dynamic.size() + " dynamic features for geohash=" + geohash);
                send(waevent);
            }
        } catch (final Exception e) {
            logError("exception in run(): " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Reads the geohash entity key from the event's data array at the
     * configured column index. FileReader / DSVParser events are untyped
     * (positional data[], no field metadata), so a name-based lookup is not
     * available -- the column index is the contract.
     */
    private String extractGeohash(final com.webaction.proc.events.WAEvent waevent) {
        if (geohashIndex < 0 || geohashIndex >= waevent.data.length) {
            logError("GeohashIndex " + geohashIndex + " is out of range for an event with "
                    + waevent.data.length + " column(s)");
            return null;
        }
        final Object value = waevent.data[geohashIndex];
        return value == null ? null : value.toString().trim();
    }

    /**
     * Looks up the dynamic feature vector for one geohash from Feast. Returns a
     * name -> value map of all 10 features, or null on any failure (the event
     * should then be dropped). Mirrors oracle.py Oracle.lookup_dynamic.
     */
    private Map<String, Double> lookupDynamic(final String geohash) throws IOException {
        final String responseBody = callFeast(buildRequestBody(geohash));
        if (responseBody == null) {
            return null;
        }
        return parseResponse(responseBody, geohash);
    }

    /** Builds the {@code /get-online-features} request body. */
    private String buildRequestBody(final String geohash) throws IOException {
        final ObjectNode root = objectMapper.createObjectNode();
        final ArrayNode features = root.putArray("features");
        for (final String ref : featureRefs) {
            features.add(ref);
        }
        final ObjectNode entities = root.putObject("entities");
        entities.putArray("geohash").add(geohash);
        return objectMapper.writeValueAsString(root);
    }

    /**
     * POSTs the request body to Feast with retry and exponential backoff.
     * Returns the response body on HTTP 200, or null once retries are
     * exhausted. Lifted from RestCaller.sendJsonNodeEvent.
     */
    private String callFeast(final String jsonBody) {
        long retryDelayMillis = 1000L;
        for (int attempt = 1; attempt <= maxRetryNum; attempt++) {
            try {
                final HttpRequest.Builder builder = HttpRequest.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .uri(URI.create(feastUrl + "/get-online-features"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(timeoutSec))
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody));
                if (enableBasicAuth) {
                    final String encoded = Base64.getEncoder().encodeToString(
                            (username + ":" + password).getBytes(StandardCharsets.UTF_8));
                    builder.header("Authorization", "Basic " + encoded);
                }

                final HttpResponse<String> response =
                        httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    return response.body();
                }
                logError("Feast returned status " + response.statusCode()
                        + " (attempt " + attempt + "/" + maxRetryNum + "): " + response.body());
            } catch (final IOException e) {
                logError("Feast call failed (attempt " + attempt + "/" + maxRetryNum
                        + "): " + e.getMessage());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logError("Feast call interrupted: " + e.getMessage());
                return null;
            }

            if (attempt < maxRetryNum) {
                try {
                    Thread.sleep(retryDelayMillis);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
                retryDelayMillis = Math.min(retryDelayMillis * 2, maxRetryDelayMillis);
            }
        }
        logError("Feast call failed after " + maxRetryNum + " attempts");
        return null;
    }

    /**
     * Parses the column-major Feast response by feature name. The response
     * carries metadata.feature_names aligned with results[]; the entity column
     * ("geohash") is skipped. Feast does not preserve request order, so values
     * are matched by name, not position. Returns null if any feature is
     * non-PRESENT or absent.
     */
    private Map<String, Double> parseResponse(final String responseBody, final String geohash)
            throws IOException {
        final JsonNode payload = objectMapper.readTree(responseBody);
        final JsonNode names = payload.path("metadata").path("feature_names");
        final JsonNode results = payload.path("results");
        if (!names.isArray() || !results.isArray()) {
            logError("malformed Feast response for geohash=" + geohash);
            return null;
        }

        final Map<String, Double> out = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            final String name = names.get(i).asText();
            if ("geohash".equals(name)) {
                continue;
            }
            final JsonNode result = results.get(i);
            final String status = result.path("statuses").path(0).asText("");
            if (!"PRESENT".equals(status)) {
                logError("Feast non-PRESENT status for geohash=" + geohash
                        + " feature=" + name + " status=" + status);
                return null;
            }
            out.put(name, result.path("values").path(0).asDouble());
        }

        // Every expected dynamic feature must be present.
        for (final String feature : DYNAMIC_FEATURES) {
            if (!out.containsKey(feature)) {
                logError("Feast response missing feature '" + feature + "' for geohash=" + geohash);
                return null;
            }
        }
        return out;
    }

    /**
     * Resolves a vault-backed property of the form {@code [ns.vault.prop]}.
     * Returns the input unchanged if it is not a vault reference. Lifted from
     * App-CVS.java.
     */
    @Nondeterministic
    private String getVaultProperty(final String name) {
        try {
            final String cleanName = name.replaceAll("[\\[\\]]", "");
            if (cleanName.equals(name)) {
                // Not a vault reference -- return the literal value.
                return name;
            }
            final String[] parts = cleanName.split("\\.");
            final String vaultId = String.format("%s.VAULT.%s", parts[0], parts[1]);
            return new VaultAPI().getValue(token, vaultId, parts[2]).value;
        } catch (final Exception e) {
            logError("could not resolve vault property '" + name + "': " + e.getMessage());
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        unregisterCounter();
        log("closed");
    }

    /** Unregisters the counter MBean so a later LOAD re-registers cleanly. */
    private void unregisterCounter() {
        if (missCounterObjectName == null) {
            return;
        }
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (mbs.isRegistered(missCounterObjectName)) {
                mbs.unregisterMBean(missCounterObjectName);
            }
        } catch (final Exception e) {
            logError("could not unregister counter MBean: " + e.getMessage());
        }
    }

    @Override
    public Map getAggVec() {
        return null;
    }

    @Override
    public void setAggVec(final Map aggVec) {
        // No-op: this OP keeps no aggregate state.
    }

    private static String stripTrailingSlash(final String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static int parseInt(final Object value, final int fallback) {
        try {
            return Integer.parseInt(Objects.toString(value, Integer.toString(fallback)));
        } catch (final NumberFormatException e) {
            return fallback;
        }
    }

    private void log(final String message) {
        if (enableLogging) {
            System.out.println("FeatureOp: " + message);
        }
    }

    private void logError(final String message) {
        System.out.println("FeatureOp ERROR: " + message);
    }
}
