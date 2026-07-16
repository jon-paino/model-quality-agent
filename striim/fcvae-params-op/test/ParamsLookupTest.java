import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Standalone proof of FCVAEParamsOp's Feast lookup+parse logic against a live
 * (or mocked) get-online-features endpoint. No build system: compile against
 * the shaded FCVAEParamsOp.jar (which supplies com.google.gson.*) and run
 * (test/run_params_test.sh does both).
 *
 * <p>Usage:
 * java ParamsLookupTest &lt;feastUrl&gt; &lt;comboKey&gt; [featureRefsCsv]
 *
 * <p>Performs EXACTLY the OP's lookup+parse sequence: buildRequestBody and
 * parseParams below are copied verbatim from FCVAEParamsOp.java (the test
 * cannot link against that class, since its Striim superclass is not in the
 * shaded jar); keep them in sync. Prints each key=value the OP would stamp
 * onto userdata (including fcvae_params_source) and a final
 * "PARAMS n=&lt;stamped-count&gt; verdict=OK" line. Lenient paths (non-200,
 * NOT_FOUND, partial rows) still exit 0 with a reduced n; only a connection
 * failure (endpoint unreachable) exits nonzero.
 */
public final class ParamsLookupTest {

    /** Same pinned default refs (and order) as FCVAEParamsOp.DEFAULT_FEATURE_REFS. */
    static final String DEFAULT_FEATURE_REFS =
            "fcvae_scoring_params_v1:scaler_mean,"
            + "fcvae_scoring_params_v1:scaler_scale,"
            + "fcvae_scoring_params_v1:last_point_threshold,"
            + "fcvae_scoring_params_v1:normal_score_mean,"
            + "fcvae_scoring_params_v1:normal_score_std,"
            + "fcvae_scoring_params_v1:model_version";

    /** The Feast entity join key; also the name to skip in the response echo. */
    private static final String ENTITY_KEY = "combo_key";

    private static final int TIMEOUT_SEC = 3;

    public static void main(final String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java ParamsLookupTest <feastUrl> <comboKey> [featureRefsCsv]");
            System.exit(2);
        }
        final String feastUrl = stripTrailingSlash(args[0]);
        final String comboKey = args[1].trim();
        final String refsCsv = (args.length > 2) ? args[2] : DEFAULT_FEATURE_REFS;

        // Same FeatureRefs CSV parse as FCVAEParamsOp.start().
        final List<String> featureRefs = new ArrayList<>();
        final Set<String> requestedFields = new LinkedHashSet<>();
        for (final String part : refsCsv.split(",")) {
            final String ref = part.trim();
            if (ref.isEmpty()) {
                continue;
            }
            featureRefs.add(ref);
            final int colon = ref.lastIndexOf(':');
            requestedFields.add(colon >= 0 ? ref.substring(colon + 1) : ref);
        }

        final HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(TIMEOUT_SEC))
                .build();

        // Same single-shot POST as FCVAEParamsOp.callFeast(), except a
        // connection failure is a hard test failure here (nonzero exit)
        // rather than a lenient stamp-nothing.
        final HttpRequest request = HttpRequest.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(feastUrl + "/get-online-features"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(TIMEOUT_SEC))
                .POST(HttpRequest.BodyPublishers.ofString(buildRequestBody(featureRefs, comboKey)))
                .build();
        final HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (final IOException | InterruptedException e) {
            System.out.println("ParamsLookupTest ERROR: connection to " + feastUrl
                    + " failed: " + e);
            System.exit(1);
            return;
        }

        // From here down every path is the OP's lenient path: stamp what
        // parses, never crash.
        final Map<String, Object> stamped = new LinkedHashMap<>();
        if (response.statusCode() == 200) {
            try {
                stamped.putAll(parseParams(response.body(), requestedFields));
            } catch (final RuntimeException e) {
                System.out.println("ParamsLookupTest: malformed response (lenient, stamping"
                        + " nothing): " + e.getMessage());
            }
        } else {
            System.out.println("ParamsLookupTest: HTTP " + response.statusCode()
                    + " (lenient, stamping nothing)");
        }
        if (!stamped.isEmpty()) {
            stamped.put("fcvae_params_source", "feast");
        }

        for (final Map.Entry<String, Object> e : stamped.entrySet()) {
            System.out.println(e.getKey() + "=" + e.getValue());
        }
        System.out.println("PARAMS n=" + stamped.size() + " verdict=OK");
    }

    // ------------------------------------------------------------------
    // COPIED VERBATIM from FCVAEParamsOp.java; keep in sync.
    // ------------------------------------------------------------------

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
     * Parses the column-major Feast response into the fcvae_* userdata stamp
     * map. metadata.feature_names is aligned with results[]; only requested
     * field names are considered (the combo_key entity echo is skipped by the
     * same rule), and a field is stamped only when its status is PRESENT and
     * its value is non-null: numeric fields as Double under
     * "fcvae_&lt;field&gt;", model_version as String under
     * "fcvae_model_version". Missing or NOT_FOUND fields are skipped, never
     * fatal, so a partial row stamps partially.
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

    private static String stripTrailingSlash(final String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
