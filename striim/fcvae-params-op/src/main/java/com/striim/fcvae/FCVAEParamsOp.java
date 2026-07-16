package com.striim.fcvae;

import com.webaction.anno.AdapterType;
import com.webaction.anno.PropertyTemplate;
import com.webaction.anno.PropertyTemplateProperty;
import com.webaction.runtime.components.openprocessor.StriimOpenProcessor;
import com.webaction.runtime.containers.IBatch;
import com.webaction.runtime.containers.WAEvent;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * FCVAEParamsOp -- Feast scoring-params enrichment for the FCVAE scorer
 * (F2 sibling of FCVAEOnnxScorer).
 *
 * <p>Feast HTTP transport mirrors FeatureOp (the taxi-demo Feast lookup OP):
 * java.net.http.HttpClient built once in start(), a POST to
 * {@code /get-online-features}, and a column-major response parsed by
 * feature name with per-field PRESENT checks. What differs is the stance:
 * FeatureOp is strict (a miss drops the event), this OP is LENIENT by
 * contract -- any miss, timeout, non-200, or malformed response stamps
 * nothing and the event still flows to the scorer, which then falls back to
 * its swap-paired model_config.json parameters.
 *
 * <p>For each incoming event the OP reads the combo_key entity from
 * {@code data[ComboKeyIndex]}, looks up the {@code fcvae_scoring_params_v1}
 * feature view row for that key, and stamps each PRESENT, non-null field
 * onto {@code waevent.userdata}:
 * <pre>
 *   fcvae_scaler_mean            (Double)
 *   fcvae_scaler_scale           (Double)
 *   fcvae_last_point_threshold   (Double)
 *   fcvae_normal_score_mean      (Double, may be absent: null in the store)
 *   fcvae_normal_score_std       (Double, may be absent: null in the store)
 *   fcvae_model_version          (String, "sha256:&lt;12hex&gt;")
 *   fcvae_params_source = "feast" (only when at least one field stamped)
 * </pre>
 * data[] is never touched; the scorer downstream owns the data[5..7] output
 * contract. The scorer only USES the Feast params when the scaler triple is
 * present and finite and fcvae_model_version exactly matches its live model
 * version, so a partial or stale stamp is safe by construction.
 */
@PropertyTemplate(
    name = "FCVAEParamsOp",
    type = AdapterType.process,
    properties = {
        @PropertyTemplateProperty(name = "FeastUrl", type = String.class, required = false,
                defaultValue = "http://127.0.0.1:6567"),
        @PropertyTemplateProperty(name = "ComboKeyIndex", type = Integer.class, required = false,
                defaultValue = "0"),
        @PropertyTemplateProperty(name = "FeatureRefs", type = String.class, required = false,
                defaultValue = FCVAEParamsOp.DEFAULT_FEATURE_REFS),
        @PropertyTemplateProperty(name = "TimeoutSec", type = Integer.class, required = false,
                defaultValue = "3"),
        // Retries per lookup on a failed call or non-200 (FeatureOp precedent;
        // named MaxRetryNum because maxRetries is a TQL reserved word). One
        // retry absorbs the occasional transient 500 a feature server emits
        // under a per-window request burst.
        @PropertyTemplateProperty(name = "MaxRetryNum", type = Integer.class, required = false,
                defaultValue = "1"),
        @PropertyTemplateProperty(name = "EnableLogging", type = Boolean.class, required = false,
                defaultValue = "false")
    },
    outputType = com.webaction.proc.events.WAEvent.class,
    inputType  = com.webaction.proc.events.WAEvent.class
)
public class FCVAEParamsOp extends StriimOpenProcessor {

    private static final Logger logger = LogManager.getLogger(FCVAEParamsOp.class);

    /**
     * The six fcvae_scoring_params_v1 feature refs, in the pinned order:
     * scaler_mean, scaler_scale, last_point_threshold, normal_score_mean,
     * normal_score_std, model_version. Compile-time constant so it can be the
     * annotation defaultValue above.
     */
    static final String DEFAULT_FEATURE_REFS =
            "fcvae_scoring_params_v1:scaler_mean,"
            + "fcvae_scoring_params_v1:scaler_scale,"
            + "fcvae_scoring_params_v1:last_point_threshold,"
            + "fcvae_scoring_params_v1:normal_score_mean,"
            + "fcvae_scoring_params_v1:normal_score_std,"
            + "fcvae_scoring_params_v1:model_version";

    /** The Feast entity join key; also the name to skip in the response echo. */
    private static final String ENTITY_KEY = "combo_key";

    // Configuration (read in start()).
    private String feastUrl;
    private int comboKeyIndex;
    private int timeoutSec;
    private int maxRetryNum;
    private boolean enableLogging;

    // Ordered feature refs for the request body, and the bare field names
    // (ref text after the last ':') the parser is allowed to stamp.
    private List<String> featureRefs;
    private Set<String> requestedFields;

    private HttpClient httpClient;

    @Override
    public void start() throws Exception {
        super.start();

        final Map<String, Object> props = getProperties();
        feastUrl = stripTrailingSlash(
                orDefault(props.get("FeastUrl"), "http://127.0.0.1:6567"));
        comboKeyIndex = parseInt(props.get("ComboKeyIndex"), 0);
        timeoutSec = parseInt(props.get("TimeoutSec"), 3);
        maxRetryNum = Math.max(0, parseInt(props.get("MaxRetryNum"), 1));
        enableLogging = Boolean.parseBoolean(Objects.toString(props.get("EnableLogging"), "false"));

        final String refsCsv = orDefault(props.get("FeatureRefs"), DEFAULT_FEATURE_REFS);
        featureRefs = new ArrayList<>();
        requestedFields = new LinkedHashSet<>();
        for (final String part : refsCsv.split(",")) {
            final String ref = part.trim();
            if (ref.isEmpty()) {
                continue;
            }
            featureRefs.add(ref);
            final int colon = ref.lastIndexOf(':');
            requestedFields.add(colon >= 0 ? ref.substring(colon + 1) : ref);
        }

        httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(timeoutSec))
                .build();

        logger.info("FCVAEParamsOp started: feastUrl={} comboKeyIndex={} timeoutSec={} refs={}",
                feastUrl, comboKeyIndex, timeoutSec, featureRefs);
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
                try {
                    enrich(waevent);
                } catch (final Exception e) {
                    // LENIENT: any lookup/parse failure stamps nothing extra.
                    logError("params enrichment failed (event still flows): " + e.getMessage());
                }
                send(waevent);
            }
        } catch (final Exception e) {
            // Never throw out of run(); a transport bug must not halt the app.
            logError("exception in run(): " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Looks up the scoring params for one event's combo_key and stamps every
     * PRESENT, non-null field onto userdata. Stamps nothing on an empty key,
     * a failed call, a non-200, or a response with no PRESENT fields.
     */
    private void enrich(final com.webaction.proc.events.WAEvent waevent) {
        final String comboKey = extractComboKey(waevent);
        if (comboKey == null || comboKey.isEmpty()) {
            log("no combo_key at data[" + comboKeyIndex + "]; skipping lookup");
            return;
        }

        final String responseBody = callFeast(buildRequestBody(featureRefs, comboKey));
        if (responseBody == null) {
            return; // callFeast already logged the cause.
        }

        final Map<String, Object> params;
        try {
            params = parseParams(responseBody, requestedFields);
        } catch (final RuntimeException e) {
            logError("malformed Feast response for combo_key=" + comboKey
                    + ": " + e.getMessage());
            return;
        }
        if (params.isEmpty()) {
            log("no PRESENT params for combo_key=" + comboKey + "; stamping nothing");
            return;
        }

        if (waevent.userdata == null) {
            waevent.userdata = new HashMap<>();
        }
        waevent.userdata.putAll(params);
        waevent.userdata.put("fcvae_params_source", "feast");
        log("stamped " + params.size() + " param(s) for combo_key=" + comboKey);
    }

    /**
     * Reads the combo_key entity from data[] at the configured index. Events
     * are untyped (positional data[], no field metadata), so the column index
     * is the contract. Returns null when out of range or the slot is null.
     */
    private String extractComboKey(final com.webaction.proc.events.WAEvent waevent) {
        if (comboKeyIndex < 0 || comboKeyIndex >= waevent.data.length) {
            logError("ComboKeyIndex " + comboKeyIndex + " is out of range for an event with "
                    + waevent.data.length + " column(s)");
            return null;
        }
        final Object value = waevent.data[comboKeyIndex];
        return value == null ? null : String.valueOf(value).trim();
    }

    /**
     * Builds the {@code /get-online-features} request body:
     * {"features":[...],"entities":{"combo_key":["&lt;combo&gt;"]},
     * "full_feature_names":false}.
     */
    static String buildRequestBody(final List<String> featureRefs, final String comboKey) {
        final JsonObject root = new JsonObject();
        final JsonArray features = new JsonArray();
        for (final String ref : featureRefs) {
            features.add(ref);
        }
        root.add("features", features);
        final JsonObject entities = new JsonObject();
        final JsonArray keys = new JsonArray();
        keys.add(comboKey);
        entities.add(ENTITY_KEY, keys);
        root.add("entities", entities);
        root.addProperty("full_feature_names", false);
        return root.toString();
    }

    /**
     * POSTs the request body to Feast, single-shot (no retry: the scorer has a
     * config fallback, so a transient miss costs nothing). Returns the response
     * body on HTTP 200, or null on any failure.
     */
    private String callFeast(final String jsonBody) {
        final HttpRequest request = HttpRequest.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(feastUrl + "/get-online-features"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(timeoutSec))
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        // One transient failure per burst is normal for a local feature server;
        // retry up to MaxRetryNum times before falling back to stamping nothing.
        for (int attempt = 0; attempt <= maxRetryNum; attempt++) {
            try {
                final HttpResponse<String> response =
                        httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    return response.body();
                }
                logError("Feast returned status " + response.statusCode()
                        + " (attempt " + (attempt + 1) + "/" + (maxRetryNum + 1) + ")");
            } catch (final IOException e) {
                logError("Feast call failed (attempt " + (attempt + 1) + "/"
                        + (maxRetryNum + 1) + "): " + e.getMessage());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logError("Feast call interrupted; stamping nothing");
                return null;
            }
            if (attempt < maxRetryNum) {
                try {
                    Thread.sleep(150L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Parses the column-major Feast response into the fcvae_* userdata stamp
     * map. metadata.feature_names is aligned with results[]; only requested
     * field names are considered (the combo_key entity echo is skipped by the
     * same rule), and a field is stamped only when its status is PRESENT and
     * its value is non-null: numeric fields as Double under
     * "fcvae_&lt;field&gt;", model_version as String under
     * "fcvae_model_version". Missing or NOT_FOUND fields are skipped, never
     * fatal, so a partial row stamps partially.
     *
     * <p>COPIED VERBATIM into test/ParamsLookupTest.java (which cannot link
     * against this class: the Striim superclass is not in the shaded jar).
     * Keep the two in sync.
     */
    static Map<String, Object> parseParams(final String responseBody,
                                           final Set<String> requestedFields) {
        final Map<String, Object> out = new LinkedHashMap<>();
        final JsonObject payload = JsonParser.parseString(responseBody).getAsJsonObject();
        final JsonObject metadata = payload.getAsJsonObject("metadata");
        final JsonArray names = (metadata == null) ? null : metadata.getAsJsonArray("feature_names");
        final JsonArray results = payload.getAsJsonArray("results");
        if (names == null || results == null) {
            return out;
        }
        final int n = Math.min(names.size(), results.size());
        for (int i = 0; i < n; i++) {
            final String name = names.get(i).getAsString();
            if (!requestedFields.contains(name)) {
                continue; // the combo_key entity echo, or an unrequested extra
            }
            if (!results.get(i).isJsonObject()) {
                continue;
            }
            final JsonObject result = results.get(i).getAsJsonObject();
            final JsonArray statuses = result.getAsJsonArray("statuses");
            final JsonArray values = result.getAsJsonArray("values");
            if (statuses == null || statuses.size() == 0 || values == null || values.size() == 0) {
                continue;
            }
            final JsonElement status = statuses.get(0);
            if (status.isJsonNull() || !"PRESENT".equals(status.getAsString())) {
                continue;
            }
            final JsonElement value = values.get(0);
            if (value == null || value.isJsonNull()) {
                continue;
            }
            if ("model_version".equals(name)) {
                out.put("fcvae_model_version", value.getAsString());
            } else {
                out.put("fcvae_" + name, Double.valueOf(value.getAsDouble()));
            }
        }
        return out;
    }

    @Override
    public void close() throws Exception {
        super.close();
        // HttpClient has no close() on Java 11; drop the reference and let GC
        // reap its connection pool.
        httpClient = null;
        logger.info("FCVAEParamsOp closed.");
    }

    // Stateless: every event is an independent lookup.
    @Override
    public java.util.Map getAggVec() { return null; }

    @Override
    public void setAggVec(final java.util.Map aggVec) { }

    // ------------------------------------------------------------------
    // Small helpers.
    // ------------------------------------------------------------------

    private static String stripTrailingSlash(final String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static String orDefault(final Object v, final String dflt) {
        final String s = Objects.toString(v, dflt);
        return s.isEmpty() ? dflt : s;
    }

    private static int parseInt(final Object v, final int dflt) {
        try {
            return Integer.parseInt(Objects.toString(v, String.valueOf(dflt)).trim());
        } catch (final NumberFormatException e) {
            return dflt;
        }
    }

    private void log(final String message) {
        if (enableLogging) {
            System.out.println("FCVAEParamsOp: " + message);
        }
    }

    private void logError(final String message) {
        System.out.println("FCVAEParamsOp ERROR: " + message);
    }
}
