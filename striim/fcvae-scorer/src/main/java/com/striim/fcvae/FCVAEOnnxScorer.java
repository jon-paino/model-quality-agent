package com.striim.fcvae;

import com.webaction.anno.AdapterType;
import com.webaction.anno.PropertyTemplate;
import com.webaction.anno.PropertyTemplateProperty;
import com.webaction.runtime.components.openprocessor.StriimOpenProcessor;
import com.webaction.runtime.containers.IBatch;
import com.webaction.runtime.containers.WAEvent;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxJavaType;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import ai.onnxruntime.ValueInfo;

import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FCVAEOnnxScorer -- in-process FCVAE penny-carding anomaly scoring via ONNX
 * Runtime, with a production-safe runtime model swap.
 *
 * <p>Vendored from fcvae-anomaly-detection/striim/fcvae-onnx-scorer (F0
 * scaffold); swap machinery ported from ModelOp (F1).
 *
 * <p>Replaces the FCVAEScoreCaller HTTP sidecar (POST to a Python FastAPI
 * service) with in-process ONNX inference through ONNX Runtime's Java JNI
 * bindings. No Python dependency, no network hop.
 *
 * <p>This is the PENNY use case: a single pooled model ("Penny_All") that scores
 * a pre-assembled 24-hour window of penny-transaction (amount &lt; $1) hourly
 * counts. The windowing is done UPSTREAM by Striim CQs; the assembled row is
 * converted to a {@code Global.WAEvent} by the WAEUdf {@code createWAEvent} UDF
 * and handed to this pass-through OP. So the OP itself is stateless -- it just
 * scores the 24-vector it receives.
 *
 * <p>This is a WAEvent pass-through Open Processor (the recommended pattern in
 * CLAUDE.md). Input and output are both the runtime WAEvent class, so the OP is
 * wired directly in TQL with {@code CREATE OPEN PROCESSOR ... INSERT INTO ...
 * FROM ...} -- no types JAR, no JAR-removal trick.
 *
 * <p>The incoming WAEvent's data[] (set by the upstream createWAEvent CQ) holds:
 * <pre>
 *   data[0] = combo_key    (String, "Penny_All")
 *   data[1] = values_list  (String, LIST() of 24 hourly penny counts, comma-separated)
 *   data[2] = window_size  (String, "24")
 *   data[3] = window_start (String, timestamp of first hour)
 *   data[4] = window_end   (String, timestamp of last hour)
 * </pre>
 *
 * <p>The OP applies the model's StandardScaler, runs inference to get the
 * per-point NLL, thresholds the last point, and APPENDS the result to data[]
 * (a Striim formatter cannot read userdata, so results must live in data[]):
 * <pre>
 *   data[5] = is_anomaly    (String "true"/"false")
 *   data[6] = anomaly_score (String, last-point NLL)
 *   data[7] = threshold     (String, last_point_threshold)
 * </pre>
 * The same three values are mirrored into userdata for live SysOut. A downstream
 * CQ projects data[0,4,5,6,7] into named fields for the JSONFormatter target.
 *
 * <p><b>Production-safe swap (ported from ModelOp).</b> Training and ONNX export
 * run outside the JVM; a retrained {@code model.onnx} (with its paired
 * {@code model_config.json}) lands in {@code ModelDir} and this OP's only job is
 * to deploy it safely at runtime. The swap state machine
 * ({@link #maybeSwapModel()}, run once per batch) provides three mechanics:
 * <ol>
 *   <li><b>Signature + config validation</b> -- a candidate's input/output
 *       contract (names, rank, dtype, fixed dims; -1 = dynamic) must match the
 *       live model before the flip, and its model_config.json must pass
 *       {@link #validateConfig}: scaler and threshold leaves present and finite
 *       (Gson maps a missing field to null now, never a silent 0.0), and
 *       window_size consistent with the model input's fixed window dim. A
 *       failing candidate is rejected and the pipeline keeps scoring on the
 *       last good model.</li>
 *   <li><b>Atomic swap</b> -- the active model lives behind an AtomicReference;
 *       the reference is flipped to the validated candidate, and old sessions
 *       are only closed AFTER the flip (on history eviction), so no event ever
 *       scores against a half-loaded or closed session. Each event captures the
 *       handle once and finishes on the (session, config) pair it started
 *       with.</li>
 *   <li><b>Content hashing + rollback</b> -- a SHA-256 of the model file is the
 *       identity; an identical re-copy (mtime bumped, bytes unchanged) is
 *       skipped. A small bounded history of recent good models is retained, and
 *       a control file ({@code ControlFile}) holding {@code rollback} /
 *       {@code rollback:<sha12>} reverts to a prior good model on demand. The
 *       model identity (path, sha256, version, loaded-at) is written onto each
 *       event's userdata.</li>
 * </ol>
 *
 * <p>The parsed {@link ModelConfig} rides inside each {@link ModelHandle}, so a
 * swap installs the (weights, scaler, threshold) PAIR atomically and a rollback
 * restores the old pair -- a new model never scores against a stale scaler or
 * threshold. The sha identity covers the model.onnx bytes ONLY: a config-only
 * edit does not trigger a swap (F2 moves the live parameters to Feast).
 */
@PropertyTemplate(
    name = "FCVAEOnnxScorer",
    type = AdapterType.process,
    properties = {
        @PropertyTemplateProperty(name = "ModelDir", type = String.class, required = false,
                defaultValue = "/opt/Striim/fcvae-models/Penny_All"),
        @PropertyTemplateProperty(name = "ControlFile", type = String.class, required = false,
                defaultValue = ""),
        @PropertyTemplateProperty(name = "ValuesIndex", type = Integer.class, required = false,
                defaultValue = "1"),
        @PropertyTemplateProperty(name = "ComboKeyIndex", type = Integer.class, required = false,
                defaultValue = "0"),
        @PropertyTemplateProperty(name = "WindowEndIndex", type = Integer.class, required = false,
                defaultValue = "4"),
        @PropertyTemplateProperty(name = "EnableLogging", type = Boolean.class, required = false,
                defaultValue = "false")
    },
    outputType = com.webaction.proc.events.WAEvent.class,
    inputType  = com.webaction.proc.events.WAEvent.class
)
public class FCVAEOnnxScorer extends StriimOpenProcessor {

    private static final Logger logger = LogManager.getLogger(FCVAEOnnxScorer.class);

    /** Retained good models: the live one plus the most recent priors for rollback. */
    private static final int HISTORY_CAP = 3;

    // Configuration (read in start()).
    private String modelDir;
    private String modelFile;    // <ModelDir>/model.onnx
    private String configFile;   // <ModelDir>/model_config.json
    private String controlFile;  // ControlFile property, or <ModelDir>/fcvae.control when blank
    private int valuesIndex;
    private int comboKeyIndex;
    private int windowEndIndex;
    private boolean enableLogging;

    // ONNX Runtime state. OrtEnvironment is a process-wide singleton.
    private OrtEnvironment ortEnv;

    // The active model. Published behind an AtomicReference so a swap is a single
    // clean reference flip and the scoring path captures one handle per event.
    private final AtomicReference<ModelHandle> sessionRef = new AtomicReference<>();

    // Bounded history of recent good models, most-recent-first. The live handle is
    // always history.peekFirst(). Sessions are closed only when evicted from here
    // (always after the reference has been flipped away from them).
    private final Deque<ModelHandle> history = new ArrayDeque<>();

    // Last-seen file mtimes; the cheap per-batch gate that avoids hashing when
    // nothing changed. mtime is a change hint, never the model identity.
    private long modelFileMtime;
    private long controlFileMtime;

    // Count of candidates rejected by validation (Mechanic 1). In-memory only;
    // logged on each rejection. Read on the single run() thread.
    private long rejectionCount;

    @Override
    public void start() throws Exception {
        super.start();
        loadProperties(getProperties());
        loadInitialModel();
    }

    private void loadProperties(final Map<String, Object> props) {
        modelDir = orDefault(props.get("ModelDir"), "/opt/Striim/fcvae-models/Penny_All");
        modelFile = new File(modelDir, "model.onnx").getPath();
        configFile = new File(modelDir, "model_config.json").getPath();
        // Blank ControlFile resolves to a control file inside the model dir.
        final String controlProp = Objects.toString(props.get("ControlFile"), "").trim();
        controlFile = controlProp.isEmpty() ? new File(modelDir, "fcvae.control").getPath()
                : controlProp;
        valuesIndex = parseInt(props.get("ValuesIndex"), 1);
        comboKeyIndex = parseInt(props.get("ComboKeyIndex"), 0);
        windowEndIndex = parseInt(props.get("WindowEndIndex"), 4);
        enableLogging = Boolean.parseBoolean(Objects.toString(props.get("EnableLogging"), "false"));
    }

    /**
     * Loads the ONNX model + config at startup and seeds the swap state. The file
     * mtimes are stamped here so a model file already present at boot (and a stale
     * control file) are not re-processed on the first batch. A bad model or config
     * here still fails start(): the OP never comes up without a valid pair.
     */
    private void loadInitialModel() throws Exception {
        ortEnv = OrtEnvironment.getEnvironment();
        final ModelHandle h = buildHandle(modelFile);
        sessionRef.set(h);
        pushHistory(h);
        modelFileMtime = safeMtime(modelFile);
        controlFileMtime = safeMtime(controlFile);
        logger.info("FCVAEOnnxScorer loaded penny model from {} (version={}, threshold={}, "
                + "mean={}, scale={}, mtime={})",
            modelDir, h.version, h.config.thresholds.last_point_threshold,
            h.config.scaler.mean, h.config.scaler.scale, modelFileMtime);
    }

    /**
     * Builds a {@link ModelHandle} from the model file: opens a fresh OrtSession,
     * parses model_config.json fresh with Gson, gates it through
     * {@link #validateConfig}, and stamps the content hash / version identity. On
     * any failure the just-opened session is closed here and the failure text is
     * thrown; the caller treats it as a rejected candidate.
     */
    private ModelHandle buildHandle(final String path) throws Exception {
        final File onnxFile = new File(path);
        final File cfgFile = new File(configFile);
        if (!onnxFile.exists() || !cfgFile.exists()) {
            throw new Exception("FCVAE model not found in " + modelDir
                    + " (need model.onnx + model_config.json)");
        }
        // ONNX 2.0 keeps weights in a sibling model.onnx.data file; ORT resolves
        // it relative to the .onnx path, so load by absolute path.
        OrtSession s;
        try (OrtSession.SessionOptions opts = new OrtSession.SessionOptions()) {
            s = ortEnv.createSession(onnxFile.getAbsolutePath(), opts);
        }
        try {
            final ModelConfig cfg;
            try (FileReader reader = new FileReader(cfgFile)) {
                cfg = new Gson().fromJson(reader, ModelConfig.class);
            }
            final String failure = validateConfig(cfg, s);
            if (failure != null) {
                throw new Exception(failure);
            }
            final String sha = sha256(path);
            final String version = "sha256:" + shortSha(sha);
            final Contract contract = readContract(s);
            return new ModelHandle(s, cfg, path, sha, version, System.currentTimeMillis(), contract);
        } catch (final Exception e) {
            try {
                s.close();
            } catch (final Exception ignore) {
                // session never went live; nothing reads it.
            }
            throw e;
        }
    }

    /**
     * Validates a freshly parsed config against structural expectations and the
     * session it will pair with. Returns null when valid, or a human-readable
     * failure. Every numeric leaf in {@link ModelConfig} is boxed, so a field
     * missing from the JSON arrives here as null instead of a silent 0.0; a
     * config missing its threshold must never go live.
     */
    private String validateConfig(final ModelConfig cfg, final OrtSession s) throws OrtException {
        if (cfg == null) {
            return "model_config.json parsed to null";
        }
        if (cfg.onnx == null) {
            return "config missing 'onnx' section";
        }
        if (cfg.scaler == null) {
            return "config missing 'scaler' section";
        }
        if (cfg.thresholds == null) {
            return "config missing 'thresholds' section";
        }
        if (cfg.onnx.input_name == null || cfg.onnx.input_name.isEmpty()) {
            return "config onnx.input_name missing/empty";
        }
        if (cfg.onnx.output_name == null || cfg.onnx.output_name.isEmpty()) {
            return "config onnx.output_name missing/empty";
        }
        if (cfg.scaler.mean == null || !Double.isFinite(cfg.scaler.mean)) {
            return "config scaler.mean missing/non-finite";
        }
        if (cfg.scaler.scale == null || !Double.isFinite(cfg.scaler.scale)
                || cfg.scaler.scale == 0.0) {
            return "config scaler.scale missing/non-finite/zero";
        }
        if (cfg.thresholds.last_point_threshold == null
                || !Double.isFinite(cfg.thresholds.last_point_threshold)) {
            return "config thresholds.last_point_threshold missing/non-finite";
        }
        if (cfg.onnx.window_size == null || cfg.onnx.window_size <= 0) {
            return "config onnx.window_size missing/non-positive";
        }
        // window_size must match the session input's window dim (dim[2]) when that
        // dim is fixed; -1 = dynamic passes.
        final NodeInfo inputInfo = s.getInputInfo().get(cfg.onnx.input_name);
        if (inputInfo == null) {
            return "config onnx.input_name '" + cfg.onnx.input_name + "' not among model inputs "
                    + s.getInputInfo().keySet();
        }
        final ValueInfo vi = inputInfo.getInfo();
        if (vi instanceof TensorInfo) {
            final long[] dims = ((TensorInfo) vi).getShape();
            if (dims.length >= 3) {
                final long windowDim = dims[2];
                if (windowDim >= 0 && windowDim != cfg.onnx.window_size.longValue()) {
                    return "config onnx.window_size " + cfg.onnx.window_size
                            + " != model input dim[2] " + windowDim;
                }
            }
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Swap state machine (run once per Striim batch from run()).
    // ------------------------------------------------------------------

    /**
     * Detects model-file and control-file changes and drives the swap / rollback
     * state machine. The common case (nothing changed) costs two stat() calls.
     *
     * <p>Order: a control-file change (rollback) is handled first, then a model-file
     * change is hashed (identical bytes are skipped), built, validated (config +
     * signature), and atomically swapped in. Any failure keeps the live model
     * running.
     */
    private void maybeSwapModel() {
        final long mMtime = safeMtime(modelFile);
        final long cMtime = safeMtime(controlFile);
        final boolean modelChanged = mMtime != 0L && mMtime > modelFileMtime;
        final boolean controlChanged = cMtime != 0L && cMtime > controlFileMtime;
        if (!modelChanged && !controlChanged) {
            return;
        }

        if (controlChanged) {
            controlFileMtime = cMtime;
            handleControlFile();
        }

        if (!modelChanged) {
            return;
        }
        modelFileMtime = mMtime;

        // Mechanic 3: content hash is the identity. Identical bytes (a touch or a
        // re-copy of the same model) are skipped, decoupling the swap from mtime.
        final String sha;
        try {
            sha = sha256(modelFile);
        } catch (final Exception e) {
            logError("hash failed, keeping current model: " + e.getMessage());
            return;
        }
        final ModelHandle live = sessionRef.get();
        if (live != null && sha.equals(live.sha256)) {
            log("model file touched but bytes identical (sha " + shortSha(sha) + "); skip reload");
            return;
        }

        // Build the candidate (session + fresh config; does NOT flip anything yet).
        // A buildHandle failure (bad model, bad/missing config) and a signature
        // mismatch both funnel into the same rejection below.
        final long t0 = System.currentTimeMillis();
        ModelHandle candidate = null;
        String failure = null;
        try {
            candidate = buildHandle(modelFile);
        } catch (final Exception e) {
            failure = (e.getMessage() != null) ? e.getMessage() : e.toString();
        }

        // Mechanic 1: signature gate. Reject (and keep the live model) on mismatch.
        if (candidate != null) {
            final Contract expected = expectedContract(live);
            failure = (expected == null) ? null : validateSignature(expected, candidate.contract);
        }
        if (failure != null) {
            rejectionCount++;
            System.out.println("FCVAEOnnxScorer: REJECTED candidate model (" + failure
                    + "); keeping " + (live != null ? live.version : "<none>")
                    + ". rejections=" + rejectionCount);
            if (candidate != null) {
                try {
                    candidate.session.close();
                } catch (final Exception ignore) {
                    // candidate never went live; nothing reads it.
                }
            }
            return;
        }

        // Mechanic 2: atomic swap. Flip the reference to the candidate FIRST, then
        // retain it in history. Old sessions are closed only on history eviction,
        // never before the flip, so no in-flight event hits a closed session.
        sessionRef.set(candidate);
        pushHistory(candidate);
        System.out.println("FCVAEOnnxScorer: SWAPPED to " + candidate.version + " (sha "
                + shortSha(candidate.sha256) + ", threshold="
                + candidate.config.thresholds.last_point_threshold + ") in "
                + (System.currentTimeMillis() - t0) + "ms");
    }

    /**
     * The expected IO contract a candidate must match. v1: the currently-loaded
     * model is the authority. Manifest hook: a future training-pipeline sidecar
     * manifest would be read here and override this to allow an intentional schema
     * change. Returns null only before any model is loaded (accept-on-bootstrap).
     */
    private Contract expectedContract(final ModelHandle live) {
        return live == null ? null : live.contract;
    }

    /**
     * Compares a candidate contract against the expected one. Returns null if the
     * candidate is compatible, or a human-readable description of the first failing
     * field (name / rank / dtype / shape). A -1 dim on either side is treated as
     * dynamic, so batch size may vary.
     */
    private String validateSignature(final Contract expected, final Contract candidate) {
        final String inFail = compareSide("input", expected.inputs, candidate.inputs);
        if (inFail != null) {
            return inFail;
        }
        return compareSide("output", expected.outputs, candidate.outputs);
    }

    private String compareSide(final String side,
                               final Map<String, TensorSpec> expected,
                               final Map<String, TensorSpec> candidate) {
        if (!candidate.keySet().equals(expected.keySet())) {
            return side + " names mismatch: expected " + expected.keySet() + " got " + candidate.keySet();
        }
        for (final Map.Entry<String, TensorSpec> e : expected.entrySet()) {
            final String name = e.getKey();
            final TensorSpec exp = e.getValue();
            final TensorSpec cand = candidate.get(name);
            if (exp == null || cand == null) {
                // Non-tensor IO (sequence/map); treated as opaque, name match only.
                continue;
            }
            if (exp.type != cand.type) {
                return side + " '" + name + "' dtype mismatch: expected " + exp.type + " got " + cand.type;
            }
            if (exp.dims.length != cand.dims.length) {
                return side + " '" + name + "' rank mismatch: expected " + exp.dims.length
                        + " got " + cand.dims.length;
            }
            for (int i = 0; i < exp.dims.length; i++) {
                final long ed = exp.dims[i];
                final long cd = cand.dims[i];
                if (ed < 0 || cd < 0) {
                    continue; // dynamic dim, any size allowed
                }
                if (ed != cd) {
                    return side + " '" + name + "' shape mismatch at dim " + i
                            + ": expected " + ed + " got " + cd;
                }
            }
        }
        return null;
    }

    /** Reads a session's input/output contract (names, ranks, dtypes, dims). */
    private Contract readContract(final OrtSession s) throws OrtException {
        return new Contract(specsOf(s.getInputInfo()), specsOf(s.getOutputInfo()));
    }

    private Map<String, TensorSpec> specsOf(final Map<String, NodeInfo> info) {
        final Map<String, TensorSpec> specs = new LinkedHashMap<>();
        for (final Map.Entry<String, NodeInfo> e : info.entrySet()) {
            final ValueInfo vi = e.getValue().getInfo();
            if (vi instanceof TensorInfo) {
                final TensorInfo ti = (TensorInfo) vi;
                specs.put(e.getKey(), new TensorSpec(ti.getShape(), ti.type));
            } else {
                // Non-tensor IO: keep the name as a key, no spec to compare.
                specs.put(e.getKey(), null);
            }
        }
        return specs;
    }

    // ------------------------------------------------------------------
    // Rollback via the control file.
    // ------------------------------------------------------------------

    /** Reads and dispatches a control-file command ({@code rollback[:<sha12>]}). */
    private void handleControlFile() {
        final String cmd;
        try {
            cmd = readControlCommand(controlFile);
        } catch (final Exception e) {
            logError("control file read failed: " + e.getMessage());
            return;
        }
        if (cmd == null || cmd.isEmpty()) {
            return;
        }
        if (cmd.equals("rollback")) {
            rollback(null);
        } else if (cmd.startsWith("rollback:")) {
            rollback(cmd.substring("rollback:".length()).trim());
        } else {
            logError("unrecognized control command: '" + cmd + "'");
        }
    }

    /**
     * Reverts the live model to a prior good one already open in history. With a
     * null target, reverts to the most recent prior model; with a sha12, reverts to
     * the history entry whose hash starts with it. The target session is already
     * open, so the revert is an immediate reference flip -- and because the config
     * rides in the handle, the old (weights, scaler, threshold) pair is restored
     * together.
     */
    private void rollback(final String targetSha12) {
        final ModelHandle live = sessionRef.get();
        final boolean byPrevious = (targetSha12 == null || targetSha12.isEmpty());
        ModelHandle target = null;
        for (final ModelHandle h : history) {
            if (byPrevious) {
                // Most recent prior model (history is most-recent-first).
                if (h != live) {
                    target = h;
                    break;
                }
            } else if (h.sha256.startsWith(targetSha12) || shortSha(h.sha256).equals(targetSha12)) {
                target = h;
                break;
            }
        }
        if (target == null) {
            logError("rollback" + (targetSha12 != null ? ":" + targetSha12 : "")
                    + " - no matching prior model in history");
            return;
        }
        sessionRef.set(target);
        // Promote the target to most-recent so it is the live handle and will not be
        // evicted/closed out from under the pipeline.
        history.remove(target);
        history.addFirst(target);
        System.out.println("FCVAEOnnxScorer: ROLLED BACK from "
                + (live != null ? live.version : "<none>") + " to " + target.version
                + " (" + target.path + ", sha " + shortSha(target.sha256)
                + ", threshold=" + target.config.thresholds.last_point_threshold + ")");
    }

    /** Returns the first non-blank trimmed line of the control file, lowercased. */
    private static String readControlCommand(final String path) throws Exception {
        final List<String> lines = Files.readAllLines(Paths.get(path));
        for (final String line : lines) {
            final String trimmed = line.trim();
            if (!trimmed.isEmpty()) {
                return trimmed.toLowerCase(java.util.Locale.ROOT);
            }
        }
        return null;
    }

    // ------------------------------------------------------------------
    // History management.
    // ------------------------------------------------------------------

    /**
     * Pushes a good model to the front of the bounded history and closes any model
     * evicted past {@link #HISTORY_CAP}. Eviction is the ONLY place a session is
     * closed, and it always happens after the live reference has moved on, so a
     * closed session is never read.
     */
    private void pushHistory(final ModelHandle h) {
        history.remove(h); // avoid duplicates if h is re-promoted
        history.addFirst(h);
        while (history.size() > HISTORY_CAP) {
            final ModelHandle evicted = history.removeLast();
            if (evicted != null && evicted != sessionRef.get()) {
                try {
                    evicted.session.close();
                } catch (final Exception e) {
                    logError("evicted session close failed: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void run() {
        final IBatch<WAEvent> batch = getAdded();
        if (batch == null) {
            return;
        }
        // Swap check: cheap once per batch, at the batch boundary (not mid-event).
        maybeSwapModel();
        for (final WAEvent event : batch) {
            // The SDK WAEvent wraps the runtime WAEvent in its data field.
            final com.webaction.proc.events.WAEvent waevent =
                    (com.webaction.proc.events.WAEvent) event.data;
            if (waevent == null || waevent.data == null) {
                continue;
            }
            try {
                scoreEvent(waevent);
            } catch (final Exception e) {
                logger.error("Error scoring event: {}", e.getMessage(), e);
                continue;   // drop on hard failure rather than emit an unscored window
            }
            send(waevent);
        }
    }

    /**
     * Reads the assembled window from data[], scores it, and appends the result.
     * The active model handle is captured ONCE here, so an in-flight event always
     * finishes on the (session, config) pair it started with even if a swap
     * happens on the next batch.
     */
    private void scoreEvent(final com.webaction.proc.events.WAEvent waevent) throws OrtException {
        final ModelHandle handle = sessionRef.get();
        final ModelConfig config = handle.config;

        final Object[] data = waevent.data;
        final String comboKey = stringAt(data, comboKeyIndex, "Penny_All");
        final String windowEnd = stringAt(data, windowEndIndex, "");
        final float[] rawValues = parseValues(stringAt(data, valuesIndex, ""));

        // StandardScaler: (x - mean) / scale.
        final float mean = config.scaler.mean.floatValue();
        final float scale = config.scaler.scale.floatValue();
        final float[][][] inputArray = new float[1][1][rawValues.length];
        for (int i = 0; i < rawValues.length; i++) {
            inputArray[0][0][i] = (rawValues[i] - mean) / scale;
        }

        final float lastPointScore;
        try (OnnxTensor inputTensor = OnnxTensor.createTensor(ortEnv, inputArray);
             OrtSession.Result result = handle.session.run(
                 Collections.singletonMap(config.onnx.input_name, inputTensor))) {
            final float[][] nll = (float[][]) result.get(config.onnx.output_name).get().getValue();
            lastPointScore = nll[0][nll[0].length - 1];   // last point = scored hour
        }

        final double threshold = config.thresholds.last_point_threshold;
        final boolean isAnomaly = lastPointScore < threshold;

        if (enableLogging) {
            logger.info("Scored combo={} is_anomaly={} score={} threshold={} window_end={}",
                comboKey, isAnomaly, lastPointScore, threshold, windowEnd);
        }
        appendResult(waevent, isAnomaly, lastPointScore, threshold);

        // Stamp the live model identity onto userdata for downstream observability
        // (Mechanic 3): which (model, config) pair produced this score.
        waevent.userdata.put("model_path", handle.path);
        waevent.userdata.put("model_sha256", handle.sha256);
        waevent.userdata.put("model_version", handle.version);
        waevent.userdata.put("model_loaded_at", handle.loadedAtMs);
    }

    /**
     * Appends the scoring result to data[] (data[5..7]) so a downstream CQ and
     * JSONFormatter can capture it, and mirrors it into userdata for SysOut.
     */
    private void appendResult(final com.webaction.proc.events.WAEvent waevent,
                              final boolean isAnomaly,
                              final float anomalyScore,
                              final double threshold) {
        final String isAnomalyStr = String.valueOf(isAnomaly);
        final String scoreStr = String.valueOf(anomalyScore);
        final String thresholdStr = String.valueOf(threshold);

        final Object[] grown = Arrays.copyOf(waevent.data, waevent.data.length + 3);
        grown[grown.length - 3] = isAnomalyStr;
        grown[grown.length - 2] = scoreStr;
        grown[grown.length - 1] = thresholdStr;
        waevent.data = grown;

        if (waevent.userdata == null) {
            waevent.userdata = new HashMap<>();
        }
        waevent.userdata.put("is_anomaly", isAnomalyStr);
        waevent.userdata.put("anomaly_score", scoreStr);
        waevent.userdata.put("threshold", thresholdStr);
    }

    private static String stringAt(final Object[] data, final int idx, final String fallback) {
        if (idx >= 0 && idx < data.length && data[idx] != null) {
            return String.valueOf(data[idx]).trim();
        }
        return fallback;
    }

    /**
     * Parses the LIST() values into a float[]. Striim's LIST() yields a
     * comma-separated string, optionally bracketed and space-padded
     * ("[100, 102, ...]" or "100, 102, ..."); both are handled.
     */
    private static float[] parseValues(final String raw) {
        final String cleaned = raw.replaceAll("[\\[\\]]", "").trim();
        if (cleaned.isEmpty()) {
            return new float[0];
        }
        final String[] parts = cleaned.split(",");
        final float[] values = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            values[i] = Float.parseFloat(parts[i].trim());
        }
        return values;
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

    @Override
    public void close() throws Exception {
        super.close();
        // Close every retained session (the live handle is in history too).
        for (final ModelHandle h : history) {
            try {
                h.session.close();
            } catch (final Exception e) {
                logError("session close failed: " + e.getMessage());
            }
        }
        history.clear();
        // Deliberately NOT closing the OrtEnvironment (a change from F0): it is a
        // process-wide singleton shared with every other ONNX OP in this JVM
        // (e.g. ModelOp), and closing it here would break their live sessions.
        logger.info("FCVAEOnnxScorer closed.");
    }

    // Stateless: the window is assembled upstream in Striim CQs.
    @Override
    public java.util.Map getAggVec() { return null; }

    @Override
    public void setAggVec(final java.util.Map aggVec) { }

    // ------------------------------------------------------------------
    // Small helpers.
    // ------------------------------------------------------------------

    /** File mtime, or 0L if the file is absent / unreadable (never throws). */
    private static long safeMtime(final String path) {
        try {
            return new java.io.File(path).lastModified();
        } catch (final Exception e) {
            return 0L;
        }
    }

    /** Hex SHA-256 of a file's bytes (standard MessageDigest, streamed). */
    private static String sha256(final String path) throws Exception {
        final MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream in = new FileInputStream(path)) {
            final byte[] buf = new byte[1 << 16];
            int n;
            while ((n = in.read(buf)) > 0) {
                md.update(buf, 0, n);
            }
        }
        final byte[] digest = md.digest();
        final StringBuilder sb = new StringBuilder(digest.length * 2);
        for (final byte b : digest) {
            sb.append(Character.forDigit((b >> 4) & 0xf, 16));
            sb.append(Character.forDigit(b & 0xf, 16));
        }
        return sb.toString();
    }

    /** First 12 hex chars of a sha, the human-friendly short identity. */
    private static String shortSha(final String sha) {
        return sha.substring(0, Math.min(12, sha.length()));
    }

    private void log(final String message) {
        if (enableLogging) {
            System.out.println("FCVAEOnnxScorer: " + message);
        }
    }

    private void logError(final String message) {
        System.out.println("FCVAEOnnxScorer ERROR: " + message);
    }

    // ------------------------------------------------------------------
    // Value-holder types. PUBLIC, with PUBLIC fields and constructors, on purpose.
    // Under .scm module isolation the OP main class (FCVAEOnnxScorer) loads via
    // OpenProcessorLoader while every other class in the jar loads via
    // ModuleClassLoader; the OP and these helpers share the package name but NOT
    // the loader, so package-private class/field/ctor access across them throws
    // IllegalAccessError at runtime. Public crosses the two loaders cleanly. (Same
    // rule that bit the quality agent's value holders and OpCounterMBean.) These are
    // created only on the OP's own start()/run() thread and never held directly as a
    // serialized OP field, so the companion public-no-arg-ctor requirement does not
    // apply -- the OP's fields are an AtomicReference and a Deque, not these types.
    // ------------------------------------------------------------------

    /**
     * An immutable, fully-loaded model: its session plus its paired config, its
     * identity, and its IO contract. The parsed {@link ModelConfig} rides in the
     * handle so a swap installs the (weights, scaler, threshold) pair atomically
     * and a rollback restores the old pair together. The sha256/version identity
     * covers the model.onnx bytes only.
     */
    public static final class ModelHandle {
        public final OrtSession session;
        public final ModelConfig config;
        public final String path;
        public final String sha256;
        public final String version;
        public final long loadedAtMs;
        public final Contract contract;

        public ModelHandle(final OrtSession session, final ModelConfig config, final String path,
                           final String sha256, final String version, final long loadedAtMs,
                           final Contract contract) {
            this.session = session;
            this.config = config;
            this.path = path;
            this.sha256 = sha256;
            this.version = version;
            this.loadedAtMs = loadedAtMs;
            this.contract = contract;
        }
    }

    /** A model's IO contract: ordered input and output tensor specs by name. */
    public static final class Contract {
        public final Map<String, TensorSpec> inputs;
        public final Map<String, TensorSpec> outputs;

        public Contract(final Map<String, TensorSpec> inputs, final Map<String, TensorSpec> outputs) {
            this.inputs = inputs;
            this.outputs = outputs;
        }
    }

    /** One tensor's shape (dims; -1 = dynamic) and element type. */
    public static final class TensorSpec {
        public final long[] dims;
        public final OnnxJavaType type;

        public TensorSpec(final long[] dims, final OnnxJavaType type) {
            this.dims = dims;
            this.type = type;
        }
    }
}
