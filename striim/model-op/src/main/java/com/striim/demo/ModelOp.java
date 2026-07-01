package com.striim.demo;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxJavaType;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import ai.onnxruntime.ValueInfo;

import com.webaction.anno.AdapterType;
import com.webaction.anno.PropertyTemplate;
import com.webaction.anno.PropertyTemplateProperty;
import com.webaction.recovery.ImmutableStemma;
import com.webaction.runtime.components.openprocessor.StriimOpenProcessor;
import com.webaction.runtime.containers.IBatch;
import com.webaction.runtime.containers.TaskEvent;
import com.webaction.runtime.containers.WAEvent;

/**
 * ModelOp -- ONNX Runtime scoring of the fare-prediction model, with a
 * production-safe runtime model swap.
 *
 * <p>This OP is a WAEvent pass-through that scores each event with ONNX through
 * ORT-Java. Per event it assembles a 15-element float vector in feature_manifest
 * order: the 5 event features are read from the event's data[] at configured
 * column positions (EventFeatureIndices -- untyped events, so access is
 * positional); the 10 dynamic features are read from waevent.userdata, where the
 * upstream FeatureOp merged them. ONNX carries no feature names, so the vector
 * order is the contract. A missing feature is scored as Float.NaN, which the ONNX
 * TreeEnsembleRegressor treats as missing, matching XGBoost. The prediction is
 * written both to waevent.userdata ("prediction"), the inter-OP contract that
 * SysOut renders, and to an appended data[] column, so a downstream CQ and
 * JSONFormatter can capture it to a file.
 *
 * <p><b>Production-safe swap (Scenario A).</b> Training and ONNX conversion run
 * outside the JVM; a finished model.onnx lands on the watched volume and this OP's
 * only job is to deploy it safely at runtime. The swap state machine
 * ({@link #maybeSwapModel()}, run once per batch) provides three mechanics:
 * <ol>
 *   <li><b>Signature validation</b> -- a candidate's input/output contract
 *       (names, rank, dtype, fixed dims; -1 = dynamic) must match the live model
 *       before the flip. A mismatch is REJECTED and the pipeline keeps scoring on
 *       the last good model. This structural pre-swap gate is SEPARATE from the
 *       per-event Float.NaN leniency above.</li>
 *   <li><b>Atomic swap</b> -- the active model lives behind an AtomicReference; the
 *       reference is flipped to the validated candidate, and old sessions are only
 *       closed AFTER the flip (on history eviction), so no event ever scores
 *       against a half-loaded or closed session. Each event captures the handle
 *       once and finishes on the session it started with.</li>
 *   <li><b>Content hashing + rollback</b> -- a SHA-256 of the model file is the
 *       identity; an identical re-copy (mtime bumped, bytes unchanged) is skipped.
 *       A small bounded history of recent good models is retained, and a control
 *       file ({@code ControlFile}) holding {@code rollback} / {@code rollback:<sha12>}
 *       reverts to a prior good model on demand. The model identity (path, sha256,
 *       version, loaded-at) is written onto each event's userdata.</li>
 * </ol>
 */
@PropertyTemplate(name = "ModelOp", type = AdapterType.process, properties = {
        @PropertyTemplateProperty(name = "ModelFile", type = String.class, required = false,
                defaultValue = "UploadedFiles/model.onnx"),
        @PropertyTemplateProperty(name = "ControlFile", type = String.class, required = false,
                defaultValue = "UploadedFiles/model.control"),
        @PropertyTemplateProperty(name = "EventFeatureIndices", type = String.class, required = false,
                defaultValue = "1,2,3,4,5"),
        @PropertyTemplateProperty(name = "EnableBatching", type = Boolean.class, required = false,
                defaultValue = "false"),
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
                defaultValue = "ModelProc")
}, outputType = com.webaction.proc.events.WAEvent.class,
   inputType = com.webaction.proc.events.WAEvent.class)
public class ModelOp extends StriimOpenProcessor {

    /** The 5 per-event features, carried in the event's data[]. */
    private static final String[] EVENT_FEATURES = {
            "hour_of_day", "day_of_week", "is_weekend", "trip_distance", "passenger_count"
    };

    /** The 10 dynamic features, merged into userdata by the FeatureOp. */
    private static final String[] DYNAMIC_FEATURES = {
            "trip_count_last_10min", "trip_count_last_1hr", "trip_count_last_4hr",
            "trip_count_last_24hr", "avg_fare_last_1hr", "unique_dropoff_zones_last_1hr",
            "cell_active_minutes_last_1hr", "trip_count_delta_10min",
            "trip_count_same_hour_yesterday", "trip_count_same_hour_last_week"
    };

    /**
     * The 15-element model input order (feature_manifest.json feature_order).
     * ONNX scoring is positional, so this order is the contract.
     */
    private static final String[] FEATURE_ORDER = {
            "hour_of_day", "day_of_week", "is_weekend", "trip_distance", "passenger_count",
            "trip_count_last_10min", "trip_count_last_1hr", "trip_count_last_4hr",
            "trip_count_last_24hr", "avg_fare_last_1hr", "unique_dropoff_zones_last_1hr",
            "cell_active_minutes_last_1hr", "trip_count_delta_10min",
            "trip_count_same_hour_yesterday", "trip_count_same_hour_last_week"
    };

    private static final String PREDICTION_KEY = "prediction";

    /** Retained good models: the live one plus the most recent priors for rollback. */
    private static final int HISTORY_CAP = 3;

    // Configuration (loaded in start()).
    private String modelFile;
    private String controlFile;
    private int[] eventFeatureIndices;
    private boolean enableBatching;
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

    // Count of candidates rejected by signature validation (Mechanic 1). In-memory
    // only this round; logged on each rejection. Read on the single run() thread.
    private long rejectionCount;

    // Phase 1b: NaN-score counter, exposed to the Quality Agent as a public
    // DynamicMBean under the com.striim.metrics JMX domain. The counter object
    // always exists so counts accrue even if JMX registration fails; only the
    // agent's visibility of it depends on a successful register.
    private static final String JMX_DOMAIN = "com.striim.metrics";
    private static final String COUNTER_NAME = "nan_score";
    private OpCounterMBean nanCounter;
    private ObjectName nanCounterObjectName;

    @Override
    public void start() throws Exception {
        super.start();
        final Map<String, Object> props = getProperties();
        loadProperties(props);

        // Phase 1b: build and register the NaN-score counter MBean. Done in
        // start() (not loadProperties) so a standalone parity harness, which also
        // calls loadProperties, never touches JMX.
        final String metricsNs = Objects.toString(props.get("MetricsNamespace"), "qualitydemo");
        final String metricsComponent = Objects.toString(props.get("MetricsComponent"), "ModelProc");
        registerCounter(metricsNs, metricsComponent);
    }

    /**
     * Builds the NaN-score counter and registers it as a public DynamicMBean
     * under {@code com.striim.metrics:name=OpCounters.<ns>.<component>,type=OpMetrics},
     * the ObjectName the ModelQualityAgent discovers via a namespace-scoped
     * {@code queryNames}. Registration is idempotent across an UNLOAD/LOAD or
     * redeploy (a stale bean of the same name is unregistered first) and never
     * fails start(): the counter still accrues, the agent simply reports the
     * signal as UNKNOWN.
     */
    private void registerCounter(final String metricsNs, final String metricsComponent) {
        nanCounter = new OpCounterMBean(COUNTER_NAME, metricsNs, metricsComponent);
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            nanCounterObjectName = new ObjectName(JMX_DOMAIN
                    + ":name=OpCounters." + metricsNs + "." + metricsComponent + ",type=OpMetrics");
            if (mbs.isRegistered(nanCounterObjectName)) {
                mbs.unregisterMBean(nanCounterObjectName);
            }
            mbs.registerMBean(nanCounter, nanCounterObjectName);
            log("registered counter MBean " + nanCounterObjectName);
        } catch (final Exception e) {
            logError("could not register counter MBean (counts still accrue locally): " + e.getMessage());
        }
    }

    /** Unregisters the counter MBean so a later LOAD re-registers cleanly. */
    private void unregisterCounter() {
        if (nanCounterObjectName == null) {
            return;
        }
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (mbs.isRegistered(nanCounterObjectName)) {
                mbs.unregisterMBean(nanCounterObjectName);
            }
        } catch (final Exception e) {
            logError("could not unregister counter MBean: " + e.getMessage());
        }
    }

    public void loadProperties(final Map<String, Object> props) throws Exception {
        modelFile = Objects.toString(props.get("ModelFile"), "UploadedFiles/model.onnx");
        if (!modelFile.endsWith(".onnx")) {
            throw new Exception("Invalid ModelFile '" + modelFile + "' - must be a .onnx file");
        }
        controlFile = Objects.toString(props.get("ControlFile"), "UploadedFiles/model.control");
        enableBatching = Boolean.parseBoolean(Objects.toString(props.get("EnableBatching"), "false"));
        enableLogging = Boolean.parseBoolean(Objects.toString(props.get("EnableLogging"), "false"));

        // Event features are read positionally from data[]; this property maps
        // each EVENT_FEATURES entry, in order, to its data[] column index.
        final String indicesProp = Objects.toString(props.get("EventFeatureIndices"), "1,2,3,4,5");
        final String[] indexParts = indicesProp.split(",");
        if (indexParts.length != EVENT_FEATURES.length) {
            throw new Exception("EventFeatureIndices must list " + EVENT_FEATURES.length
                    + " data[] indices (one per event feature, in manifest order); got: " + indicesProp);
        }
        eventFeatureIndices = new int[indexParts.length];
        for (int i = 0; i < indexParts.length; i++) {
            eventFeatureIndices[i] = Integer.parseInt(indexParts[i].trim());
        }

        loadInitialModel();
    }

    /**
     * Loads the ONNX model at startup and seeds the swap state. The file mtimes
     * are stamped here so a model file already present at boot (and a stale
     * control file) are not re-processed on the first batch.
     */
    private void loadInitialModel() throws Exception {
        ortEnv = OrtEnvironment.getEnvironment();
        final ModelHandle h = buildHandle(modelFile);
        sessionRef.set(h);
        pushHistory(h);
        modelFileMtime = safeMtime(modelFile);
        controlFileMtime = safeMtime(controlFile);
        log("loaded ONNX model: " + h.path + " (input='" + h.inputName + "', version=" + h.version
                + ", features=" + FEATURE_ORDER.length + ", mtime=" + modelFileMtime + ")");
    }

    /**
     * Builds a {@link ModelHandle} from a model file: opens a fresh OrtSession,
     * reads its IO contract, and stamps the content hash / version identity. The
     * caller owns closing the session on any later failure.
     */
    private ModelHandle buildHandle(final String path) throws Exception {
        OrtSession s;
        try (OrtSession.SessionOptions opts = new OrtSession.SessionOptions()) {
            s = ortEnv.createSession(path, opts);
        }
        final String inName = s.getInputNames().iterator().next();
        final String sha = sha256(path);
        final String version = "sha256:" + shortSha(sha);
        final Contract contract = readContract(s);
        return new ModelHandle(s, inName, path, sha, version, System.currentTimeMillis(), contract);
    }

    // ------------------------------------------------------------------
    // Swap state machine (run once per Striim batch from run()).
    // ------------------------------------------------------------------

    /**
     * Detects model-file and control-file changes and drives the swap / rollback
     * state machine. The common case (nothing changed) costs two stat() calls.
     *
     * <p>Order: a control-file change (rollback) is handled first, then a model-file
     * change is hashed (identical bytes are skipped), built, signature-validated,
     * and atomically swapped in. Any failure keeps the live model running.
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

        // Build the candidate session (does NOT flip anything yet).
        final long t0 = System.currentTimeMillis();
        final ModelHandle candidate;
        try {
            candidate = buildHandle(modelFile);
        } catch (final Exception e) {
            logError("candidate build failed, keeping current model: " + e.getMessage());
            return;
        }

        // Mechanic 1: signature gate. Reject (and keep the live model) on mismatch.
        final Contract expected = expectedContract(live);
        final String failure = (expected == null) ? null : validateSignature(expected, candidate.contract);
        if (failure != null) {
            rejectionCount++;
            logError("REJECTED candidate model (" + failure + "); keeping "
                    + (live != null ? live.version : "<none>") + ". rejections=" + rejectionCount);
            try {
                candidate.session.close();
            } catch (final Exception ignore) {
                // candidate never went live; nothing reads it.
            }
            return;
        }

        // Mechanic 2: atomic swap. Flip the reference to the candidate FIRST, then
        // retain it in history. Old sessions are closed only on history eviction,
        // never before the flip, so no in-flight event hits a closed session.
        sessionRef.set(candidate);
        pushHistory(candidate);
        System.out.println("ModelOp: SWAPPED to " + candidate.version + " (" + candidate.path
                + ", sha " + shortSha(candidate.sha256) + ") in " + (System.currentTimeMillis() - t0) + "ms");
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
     * open, so the revert is an immediate reference flip.
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
        System.out.println("ModelOp: ROLLED BACK from " + (live != null ? live.version : "<none>")
                + " to " + target.version + " (" + target.path + ", sha " + shortSha(target.sha256) + ")");
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
        final List<WAEvent> outputBatch = enableBatching ? new ArrayList<>() : null;
        ImmutableStemma lowPosition = null;

        try {
            for (final WAEvent event : batch) {
                lowPosition = updateLowPosition(lowPosition, event);

                // The SDK WAEvent wraps the runtime WAEvent in its data field.
                final com.webaction.proc.events.WAEvent waevent =
                        (com.webaction.proc.events.WAEvent) event.data;
                if (waevent == null) {
                    continue;
                }
                if (waevent.data != null) {
                    // Phase 1b denominator: an event scored by the model.
                    nanCounter.recordEvent();
                    processEvent(waevent);
                }

                if (enableBatching) {
                    outputBatch.add(new WAEvent(waevent, lowPosition));
                } else {
                    send(waevent);
                }
            }

            if (enableBatching && outputBatch != null && !outputBatch.isEmpty()) {
                send(TaskEvent.createStreamEvent(outputBatch));
            }
        } catch (final Exception e) {
            logError("exception in run(): " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Assembles the 15-feature vector, scores it through ONNX, and writes the
     * prediction into the event. The active model handle is captured ONCE here, so
     * an in-flight event always finishes on the session it started with even if a
     * swap happens on the next batch.
     */
    private void processEvent(final com.webaction.proc.events.WAEvent waevent) {
        final ModelHandle handle = sessionRef.get();
        float prediction = Float.NaN;
        if (handle == null) {
            logError("no model loaded; scoring event as NaN");
            emitPrediction(waevent, prediction, null);
            return;
        }
        try {
            final Map<String, Object> values = new HashMap<>();

            // Event features: read from data[] at the configured positions.
            // FileReader / DSVParser events are untyped, so access is positional.
            for (int i = 0; i < EVENT_FEATURES.length; i++) {
                final int idx = eventFeatureIndices[i];
                if (idx >= 0 && idx < waevent.data.length && waevent.data[idx] != null) {
                    values.put(EVENT_FEATURES[i], waevent.data[idx]);
                }
            }

            // Dynamic features: read from userdata, merged by the FeatureOp.
            if (waevent.userdata != null) {
                for (final String name : DYNAMIC_FEATURES) {
                    final Object value = waevent.userdata.get(name);
                    if (value != null) {
                        values.put(name, value);
                    }
                }
            }

            // Assemble in manifest order; a missing feature is scored as NaN
            // (ONNX TreeEnsembleRegressor treats NaN as missing, like XGBoost).
            final float[] features = new float[FEATURE_ORDER.length];
            int missing = 0;
            for (int i = 0; i < FEATURE_ORDER.length; i++) {
                final Object value = values.get(FEATURE_ORDER[i]);
                if (value == null) {
                    features[i] = Float.NaN;
                    missing++;
                } else {
                    features[i] = toFloat(value);
                }
            }
            if (missing > 0) {
                // Phase 1b numerator: an event scored with at least one missing
                // feature (NaN). The agent reads this as the NaN-score rate.
                nanCounter.recordFault();
                logError("scoring event with " + missing + " missing feature(s) as NaN");
            }

            prediction = scoreVector(handle, features);

        } catch (final OrtException e) {
            logError("ONNX scoring failed: " + e.getMessage());
        } catch (final NumberFormatException e) {
            logError("non-numeric feature value: " + e.getMessage());
        }

        emitPrediction(waevent, prediction, handle);
    }

    /**
     * Writes the prediction into the event two ways, and stamps the live model
     * identity onto userdata for downstream observability.
     *
     * <ul>
     *   <li>{@code userdata[PREDICTION_KEY]} -- the inter-OP contract, also what
     *       SysOut renders.</li>
     *   <li>An appended {@code data[]} column -- a Striim formatter cannot reach
     *       userdata, so the prediction is also copied into data[] at the event's
     *       original width; the pipeline's format CQ reads exactly that index.</li>
     *   <li>{@code userdata[model_path|model_sha256|model_version|model_loaded_at]}
     *       -- which model produced this score (Mechanic 3 identity).</li>
     * </ul>
     */
    private void emitPrediction(final com.webaction.proc.events.WAEvent waevent,
                                final float prediction,
                                final ModelHandle handle) {
        if (waevent.userdata == null) {
            waevent.userdata = new HashMap<>();
        }
        waevent.userdata.put(PREDICTION_KEY, prediction);
        if (handle != null) {
            waevent.userdata.put("model_path", handle.path);
            waevent.userdata.put("model_sha256", handle.sha256);
            waevent.userdata.put("model_version", handle.version);
            waevent.userdata.put("model_loaded_at", handle.loadedAtMs);
        }

        // Grow data[] by one and place the prediction in the new last slot.
        // FileReader/DSVParser events are untyped, so a positional append is
        // safe; the downstream CQ reads the prediction by index.
        final Object[] grown = Arrays.copyOf(waevent.data, waevent.data.length + 1);
        grown[grown.length - 1] = prediction;
        waevent.data = grown;

        log("prediction=" + prediction);
    }

    /** Scores one 15-element vector through the given model handle's session. */
    private float scoreVector(final ModelHandle handle, final float[] features) throws OrtException {
        try (OnnxTensor tensor = OnnxTensor.createTensor(ortEnv, new float[][] {features});
             OrtSession.Result result = handle.session.run(
                     Collections.singletonMap(handle.inputName, tensor))) {
            final Object raw = result.get(0).getValue();
            if (raw instanceof float[][]) {
                return ((float[][]) raw)[0][0];
            }
            if (raw instanceof float[]) {
                return ((float[]) raw)[0];
            }
            throw new IllegalStateException(
                    "unexpected ONNX output type: " + raw.getClass().getName());
        }
    }

    private ImmutableStemma updateLowPosition(final ImmutableStemma current, final WAEvent event) {
        return enableBatching && current == null ? event.position : current;
    }

    @Override
    public void close() throws Exception {
        super.close();
        unregisterCounter();
        // Close every retained session (the live handle is in history too).
        for (final ModelHandle h : history) {
            try {
                h.session.close();
            } catch (final Exception e) {
                logError("session close failed: " + e.getMessage());
            }
        }
        history.clear();
        log("closed");
    }

    @Override
    public Map getAggVec() {
        return null;
    }

    @Override
    public void setAggVec(final Map aggVec) {
        // No-op: this OP keeps no aggregate state.
    }

    // ------------------------------------------------------------------
    // Small helpers.
    // ------------------------------------------------------------------

    /** Coerces a feature value (String from data[], Number from userdata) to float. */
    private static float toFloat(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString().trim());
    }

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
            System.out.println("ModelOp: " + message);
        }
    }

    private void logError(final String message) {
        System.out.println("ModelOp ERROR: " + message);
    }

    // ------------------------------------------------------------------
    // Value-holder types. PUBLIC, with PUBLIC fields and constructors, on purpose.
    // Under .scm module isolation the OP main class (ModelOp) loads via
    // OpenProcessorLoader while every other class in the jar loads via
    // ModuleClassLoader; ModelOp and these helpers share the package name but NOT
    // the loader, so package-private class/field/ctor access across them throws
    // IllegalAccessError at runtime. Public crosses the two loaders cleanly. (Same
    // rule that bit the quality agent's value holders and OpCounterMBean.) These are
    // created only on the OP's own start()/run() thread and never held directly as a
    // serialized OP field, so the companion public-no-arg-ctor requirement does not
    // apply -- the OP's fields are an AtomicReference and a Deque, not these types.
    // ------------------------------------------------------------------

    /** An immutable, fully-loaded model: its session plus its identity and contract. */
    public static final class ModelHandle {
        public final OrtSession session;
        public final String inputName;
        public final String path;
        public final String sha256;
        public final String version;
        public final long loadedAtMs;
        public final Contract contract;

        public ModelHandle(final OrtSession session, final String inputName, final String path,
                           final String sha256, final String version, final long loadedAtMs,
                           final Contract contract) {
            this.session = session;
            this.inputName = inputName;
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
