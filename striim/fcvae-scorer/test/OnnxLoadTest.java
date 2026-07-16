import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Standalone proof that the shaded FCVAEOnnxScorer jar's bundled ONNX Runtime
 * loads the exported opset-18 FCVAE model and reproduces the frozen golden
 * scores. No build system: compile against the shaded jar and run
 * (test/run_load_test.sh does both).
 *
 * <p>Usage:
 * java OnnxLoadTest &lt;model.onnx&gt; &lt;golden_windows.jsonl&gt; [maxAbsTol=1.0] [meanAbsTol=1e-2]
 *
 * <p>Each JSONL line: model_name, normalized_values (24 floats), expected_nll
 * (24 floats), expected_last_point_nll (float), threshold (float),
 * expected_is_anomaly (bool).
 */
public final class OnnxLoadTest {

    private static final class Golden {
        String modelName;
        float[] normalizedValues;
        float[] expectedNll;
        float expectedLastPointNll;
        float threshold;
        boolean expectedIsAnomaly;
    }

    public static void main(final String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java OnnxLoadTest <model.onnx> <golden_windows.jsonl>"
                    + " [maxAbsTol=1.0] [meanAbsTol=1e-2]");
            System.exit(2);
        }
        final String modelPath = new File(args[0]).getAbsolutePath();
        final String goldenPath = args[1];
        final double maxAbsTol = args.length > 2 ? Double.parseDouble(args[2]) : 1.0;
        final double meanAbsTol = args.length > 3 ? Double.parseDouble(args[3]) : 1e-2;

        final List<Golden> goldens = readGoldens(goldenPath);
        final int n = goldens.size();

        final OrtEnvironment env = OrtEnvironment.getEnvironment();
        try (OrtSession session = env.createSession(modelPath)) {
            for (final Map.Entry<String, NodeInfo> e : session.getInputInfo().entrySet()) {
                System.out.println("input  " + e.getKey() + " -> " + e.getValue().getInfo());
            }
            for (final Map.Entry<String, NodeInfo> e : session.getOutputInfo().entrySet()) {
                System.out.println("output " + e.getKey() + " -> " + e.getValue().getInfo());
            }

            // Per-window pass: score each golden window alone as [1][1][W].
            double maxAbs = 0.0;
            double sumAbs = 0.0;
            long points = 0;
            int decisions = 0;
            for (final Golden g : goldens) {
                final float[][][] input = new float[1][1][g.normalizedValues.length];
                input[0][0] = g.normalizedValues;
                try (OnnxTensor tensor = OnnxTensor.createTensor(env, input);
                     OrtSession.Result result =
                             session.run(Collections.singletonMap("input", tensor))) {
                    final float[][] nll = (float[][]) result.get("nll").get().getValue();
                    final float[] got = nll[0];
                    for (int i = 0; i < got.length; i++) {
                        final double diff = Math.abs(got[i] - g.expectedNll[i]);
                        maxAbs = Math.max(maxAbs, diff);
                        sumAbs += diff;
                        points++;
                    }
                    final boolean isAnomaly = got[got.length - 1] < g.threshold;
                    if (isAnomaly == g.expectedIsAnomaly) {
                        decisions++;
                    }
                }
            }
            final double meanAbs = points == 0 ? 0.0 : sumAbs / points;

            // Batched pass: one run with all windows as [N][1][W], proving the
            // dynamic batch axis.
            double batchMaxAbs = 0.0;
            if (n > 0) {
                final int window = goldens.get(0).normalizedValues.length;
                final float[][][] batch = new float[n][1][window];
                for (int i = 0; i < n; i++) {
                    batch[i][0] = goldens.get(i).normalizedValues;
                }
                try (OnnxTensor tensor = OnnxTensor.createTensor(env, batch);
                     OrtSession.Result result =
                             session.run(Collections.singletonMap("input", tensor))) {
                    final float[][] nll = (float[][]) result.get("nll").get().getValue();
                    for (int i = 0; i < n; i++) {
                        final float[] expected = goldens.get(i).expectedNll;
                        for (int j = 0; j < nll[i].length; j++) {
                            batchMaxAbs = Math.max(batchMaxAbs, Math.abs(nll[i][j] - expected[j]));
                        }
                    }
                }
            }

            final boolean pass = n > 0 && decisions == n
                    && maxAbs <= maxAbsTol && meanAbs <= meanAbsTol && batchMaxAbs <= maxAbsTol;
            System.out.printf(Locale.ROOT,
                    "LOADTEST n=%d max_abs=%.6f mean_abs=%.6f batch_max_abs=%.6f"
                            + " decisions=%d/%d verdict=%s%n",
                    n, maxAbs, meanAbs, batchMaxAbs, decisions, n, pass ? "PASS" : "FAIL");
            if (!pass) {
                System.exit(1);
            }
        }
    }

    private static List<Golden> readGoldens(final String path) throws IOException {
        final List<Golden> out = new ArrayList<>();
        try (BufferedReader reader =
                     Files.newBufferedReader(new File(path).toPath(), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                final JsonObject obj = JsonParser.parseString(line).getAsJsonObject();
                final Golden g = new Golden();
                g.modelName = obj.get("model_name").getAsString();
                g.normalizedValues = floats(obj.getAsJsonArray("normalized_values"));
                g.expectedNll = floats(obj.getAsJsonArray("expected_nll"));
                g.expectedLastPointNll = obj.get("expected_last_point_nll").getAsFloat();
                g.threshold = obj.get("threshold").getAsFloat();
                g.expectedIsAnomaly = obj.get("expected_is_anomaly").getAsBoolean();
                out.add(g);
            }
        }
        return out;
    }

    private static float[] floats(final JsonArray arr) {
        final float[] out = new float[arr.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = arr.get(i).getAsFloat();
        }
        return out;
    }

    private OnnxLoadTest() { }
}
