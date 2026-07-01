package com.striim.demo;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.webaction.anno.AdapterType;
import com.webaction.anno.PropertyTemplate;
import com.webaction.anno.PropertyTemplateProperty;
import com.webaction.runtime.components.openprocessor.StriimOpenProcessor;
import com.webaction.runtime.containers.IBatch;
import com.webaction.runtime.containers.WAEvent;

/**
 * ModelQualityAgent -- Layer 1 (Operational Health) of the Quality Monitoring
 * Agent for the real-time ML inference pipeline.
 *
 * <p>This is an Open Processor implementing an agentic monitoring loop, exposed
 * to the customer as the "Striim Health Monitor agent." Unlike every other OP in
 * this repo, its primary input is NOT an upstream stream: it senses platform
 * state by reading JMX health MBeans from the in-JVM platform MBeanServer (domain
 * {@code com.striim.metrics}), on its own timer, and acts on its own initiative.
 * It is wired to branch off ScoredStream only so the agent application as a whole
 * consumes that branch for Layers 2 and 3; Layer 1 ignores the scored-event
 * contents.
 *
 * <p>The loop is an explicit perceive -> assess -> act cycle:
 * <ul>
 *   <li>PERCEIVE ({@link #perceive}): read the Layer 1 JMX signals for the target
 *       application, discovering every source/target via namespace-scoped
 *       ObjectName patterns rather than hardcoded component names.</li>
 *   <li>ASSESS ({@link #assess}): evaluate each signal against its configured
 *       threshold (the agent's "policy," exposed as properties) and roll up to an
 *       overall {@code ops_ok} verdict (GREEN / YELLOW / RED) plus an
 *       {@code ops_healthy} boolean.</li>
 *   <li>ACT ({@link #act}): emit a structured, self-describing, explainable
 *       assessment as a WAEvent and set the circuit-breaker boolean that gates
 *       Layers 2/3 and the POLICY retrain-authorization node.</li>
 * </ul>
 *
 * <p>The emitted assessment is designed to be MCP-ready (see the output contract
 * in {@link #act}): it carries a per-signal breakdown, the threshold each signal
 * was judged against, and a natural-language rationale, so an LLM orchestrator
 * could consume it as a tool result. The MCP server itself is a later phase; see
 * the extension points at the bottom of this file.
 *
 * <p>Defensive by construction: a missing MBean or attribute is "unknown," never
 * a crash and never a false RED. Any failure in the timer tick is caught and
 * logged, and the agent keeps running with its last-known verdict.
 *
 * <p>Phasing: Phase 1a (JMX-only platform-health signals; zero changes to
 * FeatureOp/ModelOp) and Phase 1b (feature-store miss rate, NaN-score rate, read
 * from FeatureOp/ModelOp DynamicMBeans) make up Layer 1. Layer 2 Phase 1 adds the
 * {@code schema_evolution} signal: the source's per-tick DDL-count delta (from the
 * app ROLLUP StriimMBean's CDC_OPERATION metric), capped at WARN. It is the same
 * cumulative-counter -> delta pattern as discarded_events, requires a CDC source
 * with CDDLCapture on, and is UNKNOWN on a file/non-CDC source. See the extension
 * points at the bottom for the Layer 2 Phase 2 design-ahead.
 */
@PropertyTemplate(name = "ModelQualityAgent", type = AdapterType.process, properties = {
        // ---- target + cadence ----
        @PropertyTemplateProperty(name = "TargetNamespace", type = String.class, required = false,
                defaultValue = "qualitydemo"),
        @PropertyTemplateProperty(name = "TargetAppName", type = String.class, required = false,
                defaultValue = "FareInference"),
        @PropertyTemplateProperty(name = "TickIntervalSec", type = Integer.class, required = false,
                defaultValue = "30"),
        @PropertyTemplateProperty(name = "JmxDomain", type = String.class, required = false,
                defaultValue = "com.striim.metrics"),
        // ---- policy: app status ----
        @PropertyTemplateProperty(name = "HealthyStatus", type = String.class, required = false,
                defaultValue = "RUNNING"),
        @PropertyTemplateProperty(name = "FailStatuses", type = String.class, required = false,
                defaultValue = "HALT,HALTED,TERMINATED,CRASH,CRASHED,EXCEPTION"),
        // ---- policy: source / target freshness (seconds) ----
        @PropertyTemplateProperty(name = "SourceFreshnessWarnSec", type = Integer.class, required = false,
                defaultValue = "60"),
        @PropertyTemplateProperty(name = "SourceFreshnessFailSec", type = Integer.class, required = false,
                defaultValue = "300"),
        @PropertyTemplateProperty(name = "TargetWriteWarnSec", type = Integer.class, required = false,
                defaultValue = "60"),
        @PropertyTemplateProperty(name = "TargetWriteFailSec", type = Integer.class, required = false,
                defaultValue = "300"),
        // ---- policy: end-to-end lag (milliseconds) ----
        @PropertyTemplateProperty(name = "LagWarnMs", type = Integer.class, required = false,
                defaultValue = "5000"),
        @PropertyTemplateProperty(name = "LagFailMs", type = Integer.class, required = false,
                defaultValue = "30000"),
        // ---- policy: node resources (percent) ----
        @PropertyTemplateProperty(name = "MemoryWarnPct", type = Integer.class, required = false,
                defaultValue = "85"),
        @PropertyTemplateProperty(name = "MemoryFailPct", type = Integer.class, required = false,
                defaultValue = "95"),
        @PropertyTemplateProperty(name = "CpuWarnPct", type = Integer.class, required = false,
                defaultValue = "85"),
        @PropertyTemplateProperty(name = "CpuFailPct", type = Integer.class, required = false,
                defaultValue = "95"),
        // ---- policy: discarded events (count delta per tick) ----
        @PropertyTemplateProperty(name = "DiscardedWarnDelta", type = Integer.class, required = false,
                defaultValue = "1"),
        @PropertyTemplateProperty(name = "DiscardedFailDelta", type = Integer.class, required = false,
                defaultValue = "100"),
        // ---- policy: OP-counter application signals, evaluated on the per-tick
        // windowed rate as a percent (Phase 1b: feature-store miss, NaN score) ----
        @PropertyTemplateProperty(name = "FeatureMissRateWarnPct", type = Integer.class, required = false,
                defaultValue = "5"),
        @PropertyTemplateProperty(name = "FeatureMissRateFailPct", type = Integer.class, required = false,
                defaultValue = "20"),
        @PropertyTemplateProperty(name = "NanScoreRateWarnPct", type = Integer.class, required = false,
                defaultValue = "5"),
        @PropertyTemplateProperty(name = "NanScoreRateFailPct", type = Integer.class, required = false,
                defaultValue = "20"),
        // ---- Layer 2 Phase 1 policy: upstream schema-evolution (DDL). WARN when
        // the per-tick delta of the source's DDL count crosses this threshold.
        // Phase 1 caps the signal at WARN (alert + verdict only, no circuit
        // breaker); the FAIL/breaker escalation (DdlFailDelta,
        // SchemaBreakOpensCircuitBreaker) is Phase 2 design-ahead. ----
        @PropertyTemplateProperty(name = "DdlWarnDelta", type = Integer.class, required = false,
                defaultValue = "1"),
        // ---- policy: backpressure severity + verdict roll-up ----
        @PropertyTemplateProperty(name = "BackpressureIsFail", type = Boolean.class, required = false,
                defaultValue = "false"),
        @PropertyTemplateProperty(name = "TreatYellowAsHealthy", type = Boolean.class, required = false,
                defaultValue = "true"),
        @PropertyTemplateProperty(name = "EnableLogging", type = Boolean.class, required = false,
                defaultValue = "true")
}, outputType = com.webaction.proc.events.WAEvent.class,
   inputType = com.webaction.proc.events.WAEvent.class)
public class ModelQualityAgent extends StriimOpenProcessor {

    private static final String AGENT_NAME = "ModelQualityAgent";

    // ---- verdict / per-signal vocabulary ----
    // PUBLIC on purpose: under the .scm module isolation the main OP class loads
    // via OpenProcessorLoader while every other class in the jar loads via
    // ModuleClassLoader; non-public types/members are inaccessible across those
    // two loaders (IllegalAccessError). Same rule the probe confirmed for MBeans.
    public enum Verdict { GREEN, YELLOW, RED, UNKNOWN }
    public enum SignalState { PASS, WARN, FAIL, UNKNOWN }

    // ---- configuration (the agent's policy), loaded in start() ----
    private String ns;
    private String appName;
    private String fqApp;            // "<ns>.<app>"
    private int tickIntervalSec;
    private String jmxDomain;
    private String healthyStatus;
    private Set<String> failStatuses;
    private long sourceWarnMs, sourceFailMs, targetWarnMs, targetFailMs;
    private long lagWarnMs, lagFailMs;
    private int memWarnPct, memFailPct, cpuWarnPct, cpuFailPct;
    private long discardedWarnDelta, discardedFailDelta;
    private int featureMissWarnPct, featureMissFailPct, nanScoreWarnPct, nanScoreFailPct;
    private long ddlWarnDelta;       // Layer 2 Phase 1: DDL-count delta WARN threshold
    private boolean backpressureIsFail;
    private boolean treatYellowAsHealthy;
    private boolean enableLogging;

    // ---- runtime state ----
    private MBeanServer mbs;
    private ObjectMapper mapper;
    private ScheduledExecutorService scheduler;
    private final Object emitLock = new Object();

    // Cumulative discarded-event counts per component, for tick-over-tick delta.
    private final Map<String, Long> lastDiscarded = new LinkedHashMap<>();

    // Phase 1b: last-seen cumulative {eventsSeen, faults} per OP counter, keyed by
    // CounterName, for computing the per-tick windowed rate (the same delta pattern
    // as lastDiscarded). The MBean stays cumulative; the windowing lives here.
    private final Map<String, long[]> lastOpCounter = new LinkedHashMap<>();

    // Layer 2 Phase 1: last-seen cumulative source DDL count ("No of DDLs"), for the
    // per-tick delta (same pattern as lastDiscarded). Sentinel -1 = no baseline yet,
    // so the first observation never alarms on a pre-existing count; a source restart
    // resets the counter, which the max(0, ...) clamp absorbs as 0.
    private long lastDdlCount = -1L;

    // Last successful assessment; emitted again if a whole tick fails, so the
    // agent never goes silent (a silent agent reads as healthy, which is unsafe).
    private volatile Assessment lastAssessment;

    @Override
    public void start() throws Exception {
        super.start();
        final Map<String, Object> p = getProperties();
        ns = Objects.toString(p.get("TargetNamespace"), "inference");
        appName = Objects.toString(p.get("TargetAppName"), "FareInference");
        fqApp = ns + "." + appName;
        tickIntervalSec = Math.max(1, parseInt(p.get("TickIntervalSec"), 30));
        jmxDomain = Objects.toString(p.get("JmxDomain"), "com.striim.metrics");
        healthyStatus = Objects.toString(p.get("HealthyStatus"), "RUNNING").trim();
        failStatuses = toUpperSet(Objects.toString(p.get("FailStatuses"),
                "HALT,HALTED,TERMINATED,CRASH,CRASHED,EXCEPTION"));
        sourceWarnMs = parseInt(p.get("SourceFreshnessWarnSec"), 60) * 1000L;
        sourceFailMs = parseInt(p.get("SourceFreshnessFailSec"), 300) * 1000L;
        targetWarnMs = parseInt(p.get("TargetWriteWarnSec"), 60) * 1000L;
        targetFailMs = parseInt(p.get("TargetWriteFailSec"), 300) * 1000L;
        lagWarnMs = parseInt(p.get("LagWarnMs"), 5000);
        lagFailMs = parseInt(p.get("LagFailMs"), 30000);
        memWarnPct = parseInt(p.get("MemoryWarnPct"), 85);
        memFailPct = parseInt(p.get("MemoryFailPct"), 95);
        cpuWarnPct = parseInt(p.get("CpuWarnPct"), 85);
        cpuFailPct = parseInt(p.get("CpuFailPct"), 95);
        discardedWarnDelta = parseInt(p.get("DiscardedWarnDelta"), 1);
        discardedFailDelta = parseInt(p.get("DiscardedFailDelta"), 100);
        featureMissWarnPct = parseInt(p.get("FeatureMissRateWarnPct"), 5);
        featureMissFailPct = parseInt(p.get("FeatureMissRateFailPct"), 20);
        nanScoreWarnPct = parseInt(p.get("NanScoreRateWarnPct"), 5);
        nanScoreFailPct = parseInt(p.get("NanScoreRateFailPct"), 20);
        ddlWarnDelta = parseInt(p.get("DdlWarnDelta"), 1);
        backpressureIsFail = parseBool(p.get("BackpressureIsFail"), false);
        treatYellowAsHealthy = parseBool(p.get("TreatYellowAsHealthy"), true);
        enableLogging = parseBool(p.get("EnableLogging"), true);

        mbs = ManagementFactory.getPlatformMBeanServer();
        mapper = new ObjectMapper();

        // The agent runs its perceive -> assess -> act loop on its OWN timer, not
        // off run(). A single-threaded scheduler means ticks never overlap, so the
        // timer is the sole caller of send() (run() never emits) -- one writer,
        // no concurrency on the output channel.
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, AGENT_NAME + "-tick");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::safeTick, 2, tickIntervalSec, TimeUnit.SECONDS);

        log("started: watching " + fqApp + " via JMX domain " + jmxDomain
                + " every " + tickIntervalSec + "s");
    }

    /** One perceive -> assess -> act cycle, guarded so the loop never dies. */
    private void safeTick() {
        try {
            final HealthSnapshot snapshot = perceive();
            final Assessment assessment = assess(snapshot);
            lastAssessment = assessment;
            act(assessment);
        } catch (final Throwable t) {
            // Log loudly (always) and keep the loop alive. Re-emit the last-known
            // assessment if we have one, so downstream gating is not left stale.
            logError("tick failed: " + t);
            final Assessment last = lastAssessment;
            if (last != null) {
                try {
                    act(last);
                } catch (final Throwable t2) {
                    logError("re-emit of last assessment also failed: " + t2);
                }
            }
        }
    }

    // =====================================================================
    // PERCEIVE: read the Layer 1 JMX signals for the target application.
    // =====================================================================
    private HealthSnapshot perceive() {
        final HealthSnapshot s = new HealthSnapshot();
        final long now = System.currentTimeMillis();
        s.tickTs = now;

        // app_status: AppHealth.Status on the app's HealthReport bean.
        final ObjectName appBean = name("name=App." + fqApp + ",type=HealthReport");
        s.appStatus = asString(getAttr(appBean, "Status"));

        // source_freshness: LastEventTime (epoch ms) on each Source HealthReport,
        // discovered by namespace-scoped pattern (no hardcoded component names).
        for (final ObjectName on : query("name=Source." + ns + ".*,type=HealthReport")) {
            final String fq = asString(getAttr(on, "FqSourceName"));
            final Long last = asLong(getAttr(on, "LastEventTime"));
            s.sources.add(new ComponentTime(fq != null ? fq : on.getKeyProperty("name"), last));
        }

        // target_freshness + lag: LastWriteTime and LagEnd2EndJson on each Target.
        for (final ObjectName on : query("name=Target." + ns + ".*,type=HealthReport")) {
            final String fq = asString(getAttr(on, "FqTargetName"));
            final Long lastWrite = asLong(getAttr(on, "LastWriteTime"));
            s.targets.add(new ComponentTime(fq != null ? fq : on.getKeyProperty("name"), lastWrite));
        }

        // lag_end2end: cleanest numeric form is MAX_LEE (Double) on the sink ROLLUP
        // StriimMBean; fall back across the app's rollups for the max observed.
        Double maxLag = null;
        for (final ObjectName on : query("name=ROLLUP." + ns + ".*,type=StriimMBean")) {
            final Double lee = asDouble(getAttr(on, "MAX_LEE"));
            if (lee != null) {
                maxLag = (maxLag == null) ? lee : Math.max(maxLag, lee);
            }
            // backpressure: STREAM_FULL (Boolean) on any component in the app.
            final Boolean full = asBool(getAttr(on, "STREAM_FULL"));
            if (Boolean.TRUE.equals(full)) {
                s.backpressuredComponents.add(on.getKeyProperty("name"));
            }
            // discarded events: DISCARDED_EVENT_COUNT / DISCARDED_RECORDS (cumulative).
            Long disc = asLong(getAttr(on, "DISCARDED_EVENT_COUNT"));
            if (disc == null) {
                disc = asLong(getAttr(on, "DISCARDED_RECORDS"));
            }
            if (disc != null) {
                s.discardedByComponent.put(on.getKeyProperty("name"), disc);
            }
        }
        s.maxLagMs = maxLag;

        // node resources: MEMORY_USED_PERCENT / CPU_PER_NODE on the node self bean
        // (name S<node>.Global.S<node>). Node health is cluster-wide, not app-scoped.
        for (final ObjectName on : query("name=S*.Global.S*,type=StriimMBean")) {
            final Float mem = asFloat(getAttr(on, "MEMORY_USED_PERCENT"));
            final Float cpu = asFloat(getAttr(on, "CPU_PER_NODE"));
            if (mem != null) {
                s.memoryUsedPct = mem;
            }
            if (cpu != null) {
                s.cpuPct = cpu;
            }
            final String disk = asString(getAttr(on, "DISK_FREE"));
            if (disk != null) {
                s.diskFree = disk;
            }
            break; // single local node in this deployment
        }

        // Phase 1b: application-level OP counters (feature-store miss rate, NaN
        // score rate). FeatureOp/ModelOp register public DynamicMBeans under this
        // same JMX domain (com.striim.metrics:name=OpCounters.<ns>.<comp>,type=
        // OpMetrics), so they are read uniformly here, by the same defensive
        // queryNames path as every other signal. The bean is self-describing: its
        // CounterName attribute (feature_store_miss / nan_score) is the
        // discriminator, so the agent does not hardcode component names. The beans
        // expose only cumulative monotonic totals; the windowed rate is derived in
        // assess() from tick-over-tick deltas.
        //
        // Classification note: this signal group sits on the Layer 1 / Layer 2
        // boundary. It is kept as one structured list (opCounters) precisely so it
        // could be re-pointed to Layer 2 later without reworking the 1a signals.
        for (final ObjectName on : query("name=OpCounters." + ns + ".*,type=OpMetrics")) {
            final String counterName = asString(getAttr(on, "CounterName"));
            if (counterName == null) {
                continue;
            }
            final Long eventsSeen = asLong(getAttr(on, "EventsSeen"));
            final Long faults = asLong(getAttr(on, "Faults"));
            final String component = asString(getAttr(on, "Component"));
            s.opCounters.add(new OpCounter(counterName,
                    component != null ? component : on.getKeyProperty("name"),
                    eventsSeen, faults));
        }

        // Layer 2 Phase 1: upstream schema-evolution (DDL), read off the app ROLLUP
        // StriimMBean (ROLLUP.<ns>.<app>). The DDL count is SPLIT across attributes
        // by disposition: CDC_OPERATION "No of DDLs" counts DDLs that were PROCESSED
        // (CDDLAction Process), while DDL_METRICS "Ignored DDL Count" / "Filtered DDL
        // Count" count the rest. We SUM all three so the signal is correct under any
        // CDDLAction. Our pipeline uses CDDLAction Ignore (the documented setting for
        // a schemaless FileWriter target, which also keeps DDL events off the
        // inference OPs), so the DDLs land in "Ignored DDL Count" -- confirmed live.
        // The literal statement is DDL_METRICS "Last Received DDL". A file / non-CDC
        // source has none of these attributes, so this stays null -> the signal is
        // UNKNOWN, never a false alarm (same posture as discarded_events on a file
        // pipeline). NOTE: on this 5.2.0.4 build the count is in CDC_OPERATION /
        // DDL_METRICS JSON, NOT a NUM_OF_DDLS_EXECUTED attribute.
        final ObjectName rollup = name("name=ROLLUP." + fqApp + ",type=StriimMBean");
        final String cdcOp = asString(getAttr(rollup, "CDC_OPERATION"));
        final String ddlMetrics = asString(getAttr(rollup, "DDL_METRICS"));
        s.ddlCount = sumDdl(jsonLong(cdcOp, "No of DDLs"),
                            jsonLong(ddlMetrics, "Ignored DDL Count"),
                            jsonLong(ddlMetrics, "Filtered DDL Count"));
        s.lastDdl = jsonFirstText(ddlMetrics, "Last Received DDL");

        return s;
    }

    // =====================================================================
    // ASSESS: evaluate each signal against policy, roll up to a verdict.
    // =====================================================================
    private Assessment assess(final HealthSnapshot s) {
        final List<SignalAssessment> signals = new ArrayList<>();

        // app_status
        signals.add(assessAppStatus(s.appStatus));

        // source freshness (one signal per source; worst rolls up)
        for (final ComponentTime c : s.sources) {
            signals.add(assessFreshness("source_freshness[" + c.name + "]", c.lastMs,
                    s.tickTs, sourceWarnMs, sourceFailMs, "last event"));
        }
        if (s.sources.isEmpty()) {
            signals.add(unknown("source_freshness", "no Source health beans found for " + ns));
        }

        // target write freshness
        for (final ComponentTime c : s.targets) {
            signals.add(assessFreshness("target_write_age[" + c.name + "]", c.lastMs,
                    s.tickTs, targetWarnMs, targetFailMs, "last write"));
        }
        if (s.targets.isEmpty()) {
            signals.add(unknown("target_write_age", "no Target health beans found for " + ns));
        }

        // end-to-end lag
        signals.add(assessLag(s.maxLagMs));

        // backpressure
        signals.add(assessBackpressure(s.backpressuredComponents));

        // discarded events (delta since last tick)
        signals.add(assessDiscarded(s.discardedByComponent));

        // node resources
        signals.add(assessPercent("node_memory_pct", s.memoryUsedPct, memWarnPct, memFailPct, "%"));
        signals.add(assessPercent("node_cpu_pct", s.cpuPct, cpuWarnPct, cpuFailPct, "%"));

        // Phase 1b: OP-counter application signals. Each is assessed on its
        // per-tick windowed rate (the operational signal: is the miss/NaN rate
        // spiking now), while the cumulative totals ride along in the signal for
        // the acceptance test to assert on exactly. A bean that is absent yields
        // UNKNOWN, never a false fault, exactly like the 1a signals.
        signals.add(assessOpCounter("feature_miss_rate", "feature_store_miss", s,
                featureMissWarnPct, featureMissFailPct));
        signals.add(assessOpCounter("nan_score_rate", "nan_score", s,
                nanScoreWarnPct, nanScoreFailPct));

        // Layer 2 Phase 1: upstream schema-evolution (DDL). Same cumulative-counter
        // -> per-tick delta pattern as discarded_events, applied to the source's
        // DDL count, capped at WARN (alert + verdict only; no circuit breaker).
        signals.add(assessSchemaEvolution(s));

        // Roll up: RED if any FAIL, else YELLOW if any WARN, else GREEN. UNKNOWN
        // signals never force RED (defensive: missing data is not a fault).
        Verdict verdict = Verdict.GREEN;
        int fail = 0, warn = 0;
        boolean anyKnown = false;
        for (final SignalAssessment a : signals) {
            if (a.state == SignalState.FAIL) {
                fail++;
            } else if (a.state == SignalState.WARN) {
                warn++;
            }
            if (a.state != SignalState.UNKNOWN) {
                anyKnown = true;
            }
        }
        if (fail > 0) {
            verdict = Verdict.RED;
        } else if (warn > 0) {
            verdict = Verdict.YELLOW;
        } else if (!anyKnown) {
            verdict = Verdict.UNKNOWN;
        }

        final boolean opsHealthy = (verdict == Verdict.GREEN)
                || (verdict == Verdict.YELLOW && treatYellowAsHealthy)
                // UNKNOWN must not gate downstream off: treat as "not unhealthy"
                // but the circuit breaker stays closed only when we have signal.
                || (verdict == Verdict.UNKNOWN);
        final boolean circuitBreakerOpen = !opsHealthy;

        final Assessment a = new Assessment();
        a.tickTs = s.tickTs;
        a.verdict = verdict;
        a.opsHealthy = opsHealthy;
        a.circuitBreakerOpen = circuitBreakerOpen;
        a.failCount = fail;
        a.warnCount = warn;
        a.signals = signals;
        a.rationale = buildRationale(verdict, signals, fail, warn);
        return a;
    }

    private SignalAssessment assessAppStatus(final String status) {
        if (status == null) {
            return unknown("app_status", "App health bean or Status attribute not found for " + fqApp);
        }
        final String up = status.toUpperCase();
        if (up.equals(healthyStatus.toUpperCase())) {
            return signal("app_status", status, "== " + healthyStatus, SignalState.PASS,
                    "application is " + status);
        }
        if (failStatuses.contains(up)) {
            return signal("app_status", status, "not in {" + String.join(",", failStatuses) + "}",
                    SignalState.FAIL, "application status is " + status);
        }
        return signal("app_status", status, "== " + healthyStatus, SignalState.WARN,
                "application status is " + status + " (transitional or non-running)");
    }

    private SignalAssessment assessFreshness(final String nm, final Long lastMs, final long now,
                                             final long warnMs, final long failMs, final String what) {
        if (lastMs == null || lastMs <= 0L) {
            return unknown(nm, "no " + what + " timestamp available");
        }
        final long age = now - lastMs;
        final String observed = (age / 1000) + "s since " + what;
        if (age >= failMs) {
            return signal(nm, observed, ">= " + (failMs / 1000) + "s", SignalState.FAIL,
                    what + " is stale by " + (age / 1000) + "s");
        }
        if (age >= warnMs) {
            return signal(nm, observed, ">= " + (warnMs / 1000) + "s", SignalState.WARN,
                    what + " aging (" + (age / 1000) + "s)");
        }
        return signal(nm, observed, "< " + (warnMs / 1000) + "s", SignalState.PASS, what + " is fresh");
    }

    private SignalAssessment assessLag(final Double lagMs) {
        if (lagMs == null) {
            return unknown("lag_end2end", "no MAX_LEE lag metric available");
        }
        final String observed = Math.round(lagMs) + "ms";
        if (lagMs >= lagFailMs) {
            return signal("lag_end2end", observed, ">= " + lagFailMs + "ms", SignalState.FAIL,
                    "end-to-end lag is " + Math.round(lagMs) + "ms");
        }
        if (lagMs >= lagWarnMs) {
            return signal("lag_end2end", observed, ">= " + lagWarnMs + "ms", SignalState.WARN,
                    "end-to-end lag elevated (" + Math.round(lagMs) + "ms)");
        }
        return signal("lag_end2end", observed, "< " + lagWarnMs + "ms", SignalState.PASS, "lag nominal");
    }

    private SignalAssessment assessBackpressure(final List<String> full) {
        if (full.isEmpty()) {
            return signal("backpressure", "none", "no STREAM_FULL", SignalState.PASS,
                    "no backpressured components");
        }
        final SignalState state = backpressureIsFail ? SignalState.FAIL : SignalState.WARN;
        return signal("backpressure", full.size() + " component(s)", "STREAM_FULL=false", state,
                "backpressure on: " + String.join(", ", full));
    }

    private SignalAssessment assessDiscarded(final Map<String, Long> current) {
        long delta = 0L;
        final List<String> movers = new ArrayList<>();
        for (final Map.Entry<String, Long> e : current.entrySet()) {
            final Long prev = lastDiscarded.get(e.getKey());
            final long d = (prev == null) ? 0L : Math.max(0L, e.getValue() - prev);
            if (d > 0) {
                delta += d;
                movers.add(e.getKey() + "+" + d);
            }
        }
        // Update baseline for next tick.
        lastDiscarded.clear();
        lastDiscarded.putAll(current);

        if (current.isEmpty()) {
            return unknown("discarded_events", "no discarded-count metrics present");
        }
        final String observed = "+" + delta + " this tick";
        if (delta >= discardedFailDelta) {
            return signal("discarded_events", observed, ">= " + discardedFailDelta, SignalState.FAIL,
                    "events discarded: " + String.join(", ", movers));
        }
        if (delta >= discardedWarnDelta) {
            return signal("discarded_events", observed, ">= " + discardedWarnDelta, SignalState.WARN,
                    "events discarded: " + String.join(", ", movers));
        }
        return signal("discarded_events", observed, "< " + discardedWarnDelta, SignalState.PASS,
                "no new discards");
    }

    private SignalAssessment assessPercent(final String nm, final Float pct,
                                           final int warnPct, final int failPct, final String unit) {
        if (pct == null) {
            return unknown(nm, "metric not available");
        }
        final String observed = String.format("%.1f%s", pct, unit);
        if (pct >= failPct) {
            return signal(nm, observed, ">= " + failPct + unit, SignalState.FAIL, nm + " critical");
        }
        if (pct >= warnPct) {
            return signal(nm, observed, ">= " + warnPct + unit, SignalState.WARN, nm + " elevated");
        }
        return signal(nm, observed, "< " + warnPct + unit, SignalState.PASS, nm + " nominal");
    }

    /**
     * Assesses one OP-counter signal (feature-store miss / NaN score). The
     * verdict is judged on the per-tick WINDOWED rate (faults/events since the
     * last tick) as a percent, so a spike registers immediately and a long-run
     * baseline does not permanently pin the signal. The CUMULATIVE totals
     * (eventsSeen, faults) are carried on the SignalAssessment unchanged, so the
     * acceptance test can assert exact counts off the emitted assessment without
     * fighting the tick window. A missing bean is UNKNOWN, never a false fault.
     */
    private SignalAssessment assessOpCounter(final String signalName, final String counterName,
                                             final HealthSnapshot s, final int warnPct, final int failPct) {
        OpCounter c = null;
        for (final OpCounter oc : s.opCounters) {
            if (counterName.equals(oc.counterName)) {
                c = oc;
                break;
            }
        }
        if (c == null || c.eventsSeen == null) {
            return new SignalAssessment(signalName, "unknown", "n/a", SignalState.UNKNOWN,
                    "no " + counterName + " counter MBean found for " + ns, null, null);
        }

        final long events = c.eventsSeen;
        final long faults = (c.faults == null) ? 0L : c.faults;

        // Per-tick window from cumulative deltas (baseline keyed by CounterName).
        // Negatives are clamped so a counter reset across a redeploy reads as 0.
        final long[] prev = lastOpCounter.get(counterName);
        final long dEvents = (prev == null) ? events : Math.max(0L, events - prev[0]);
        final long dFaults = (prev == null) ? faults : Math.max(0L, faults - prev[1]);
        lastOpCounter.put(counterName, new long[] { events, faults });

        final double windowPct = (dEvents > 0L) ? (double) dFaults / (double) dEvents * 100.0d : 0.0d;
        final double cumulativePct = (events > 0L) ? (double) faults / (double) events * 100.0d : 0.0d;

        final String observed = String.format("%.1f%% this tick (%d/%d)", windowPct, dFaults, dEvents);
        final String detail = String.format(
                "%s window %d/%d (%.1f%%); cumulative %d/%d (%.1f%%)",
                counterName, dFaults, dEvents, windowPct, faults, events, cumulativePct);

        final SignalState state;
        final String threshold;
        if (windowPct >= failPct) {
            state = SignalState.FAIL;
            threshold = ">= " + failPct + "%";
        } else if (windowPct >= warnPct) {
            state = SignalState.WARN;
            threshold = ">= " + warnPct + "%";
        } else {
            state = SignalState.PASS;
            threshold = "< " + warnPct + "%";
        }
        return new SignalAssessment(signalName, observed, threshold, state, detail, events, faults);
    }

    /**
     * Layer 2 Phase 1: upstream schema-evolution (DDL) signal. Reads the source's
     * cumulative DDL count (CDC_OPERATION "No of DDLs"), computes the per-tick delta
     * against a baseline (the same pattern as {@link #assessDiscarded}), and WARNs
     * when the delta crosses {@code DdlWarnDelta}. Phase 1 deliberately caps this at
     * WARN: a DDL is surfaced in the verdict (YELLOW) and the rationale, with the raw
     * last statement, but it does NOT open the circuit breaker. Escalating a schema
     * break to RED / retrain-suppression is the Phase 2 design (DdlFailDelta +
     * SchemaBreakOpensCircuitBreaker + per-change classification). The cumulative
     * total and last DDL ride along on the signal so the acceptance test asserts the
     * exact count off the emitted assessment. A non-CDC source has no DDL metric, so
     * the signal is UNKNOWN, never a false alarm.
     */
    private SignalAssessment assessSchemaEvolution(final HealthSnapshot s) {
        if (s.ddlCount == null) {
            return new SignalAssessment("schema_evolution", "unknown", "n/a", SignalState.UNKNOWN,
                    "no CDC DDL metric for " + fqApp + " (source is not a CDC reader, or CDDLCapture is off)",
                    null, null, null, null);
        }
        final long total = s.ddlCount;
        // First observation establishes the baseline (delta 0, no alarm on a
        // pre-existing count); a source restart resets the counter, absorbed as 0.
        final long delta = (lastDdlCount < 0L) ? 0L : Math.max(0L, total - lastDdlCount);
        lastDdlCount = total;

        final String lastDdl = (s.lastDdl == null || s.lastDdl.isEmpty()) ? "n/a" : s.lastDdl;
        final String observed = "+" + delta + " this tick (cumulative " + total + ")";
        final String detail = "DDL ops +" + delta + " this tick; cumulative " + total
                + "; last DDL: " + lastDdl;

        final SignalState state;
        final String threshold;
        if (delta >= ddlWarnDelta) {
            // Phase 1 ceiling is WARN: detect + alert, do not gate. The FAIL /
            // circuit-breaker escalation is Phase 2.
            state = SignalState.WARN;
            threshold = ">= " + ddlWarnDelta + " DDL/tick";
        } else {
            state = SignalState.PASS;
            threshold = "< " + ddlWarnDelta + " DDL/tick";
        }
        return new SignalAssessment("schema_evolution", observed, threshold, state, detail,
                null, null, total, lastDdl);
    }

    private String buildRationale(final Verdict v, final List<SignalAssessment> signals,
                                  final int fail, final int warn) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Operational health for ").append(fqApp).append(" is ").append(v).append(". ");
        if (v == Verdict.GREEN) {
            sb.append("All ").append(signals.size()).append(" signals within policy.");
            return sb.toString();
        }
        if (v == Verdict.UNKNOWN) {
            sb.append("No signals could be read (JMX may be disabled or the app is not deployed).");
            return sb.toString();
        }
        sb.append(fail).append(" failing, ").append(warn).append(" warning. ");
        final List<String> reasons = new ArrayList<>();
        for (final SignalAssessment a : signals) {
            if (a.state == SignalState.FAIL || a.state == SignalState.WARN) {
                reasons.add(a.name + " (" + a.state + ": " + a.detail + ")");
            }
        }
        sb.append(String.join("; ", reasons)).append('.');
        if (v == Verdict.RED) {
            sb.append(" Layers 2/3 are suppressed and retraining is not authorized while ops are RED.");
        }
        return sb.toString();
    }

    // =====================================================================
    // ACT: emit the structured assessment + set the circuit-breaker.
    // =====================================================================
    private void act(final Assessment a) {
        final com.webaction.proc.events.WAEvent ev = new com.webaction.proc.events.WAEvent();

        // Structured, self-describing, MCP-ready assessment object. Built first so
        // it can be serialized into data[] (formatter-reachable) AND placed in
        // userdata (inter-agent handoff / SysOut / future MCP tool result).
        final Map<String, Object> assessment = toAssessmentMap(a);
        final String assessmentJson = toJson(assessment);

        // data[] output contract (positional; a downstream CQ projects these to
        // named fields for a JSONFormatter, since formatters cannot read userdata):
        //   [0] tick_ts (epoch ms)        [5] signal_count
        //   [1] verdict (GREEN/YELLOW/RED)[6] fail_count
        //   [2] ops_healthy (bool)        [7] warn_count
        //   [3] ops_circuit_breaker_open  [8] assessment_json (full structured)
        //   [4] rationale (NL)            [9] target_app (ns.app)
        ev.data = new Object[] {
                a.tickTs,
                a.verdict.name(),
                a.opsHealthy,
                a.circuitBreakerOpen,
                a.rationale,
                a.signals.size(),
                a.failCount,
                a.warnCount,
                assessmentJson,
                fqApp
        };

        ev.userdata = new java.util.HashMap<>();
        ev.userdata.put("agent", AGENT_NAME);
        ev.userdata.put("layer", 1);
        ev.userdata.put("target_app", fqApp);
        ev.userdata.put("tick_ts", a.tickTs);
        ev.userdata.put("verdict", a.verdict.name());
        ev.userdata.put("ops_healthy", a.opsHealthy);
        // The circuit-breaker boolean: the gate Layers 2/3 and the POLICY node read.
        ev.userdata.put("ops_circuit_breaker_open", a.circuitBreakerOpen);
        ev.userdata.put("rationale", a.rationale);
        ev.userdata.put("assessment", assessment);

        synchronized (emitLock) {
            send(ev);
        }
        log(a.verdict + " ops_healthy=" + a.opsHealthy + " breaker_open=" + a.circuitBreakerOpen
                + " (" + a.failCount + " fail, " + a.warnCount + " warn) :: " + a.rationale);
    }

    /** Builds the MCP-ready structured assessment as a plain Map (JSON-friendly). */
    private Map<String, Object> toAssessmentMap(final Assessment a) {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("agent", AGENT_NAME);
        m.put("layer", 1);
        m.put("layer_name", "operational_health");
        m.put("target_app", fqApp);
        m.put("tick_ts", a.tickTs);
        m.put("verdict", a.verdict.name());
        m.put("ops_healthy", a.opsHealthy);
        m.put("ops_circuit_breaker_open", a.circuitBreakerOpen);
        m.put("fail_count", a.failCount);
        m.put("warn_count", a.warnCount);
        m.put("rationale", a.rationale);
        final List<Map<String, Object>> sigs = new ArrayList<>();
        for (final SignalAssessment sa : a.signals) {
            final Map<String, Object> sm = new LinkedHashMap<>();
            sm.put("name", sa.name);
            sm.put("observed", sa.observed);
            sm.put("threshold", sa.threshold);
            sm.put("state", sa.state.name());
            sm.put("detail", sa.detail);
            // Phase 1b: cumulative OP-counter totals (the acceptance test asserts
            // on these exact numbers). Present only on OP-counter signals.
            if (sa.eventsSeen != null) {
                sm.put("events_seen", sa.eventsSeen);
                sm.put("faults", sa.faults == null ? 0L : sa.faults);
            }
            // Layer 2 Phase 1: cumulative DDL total + last statement (the DDL
            // acceptance test asserts on ddls_total). Present only on schema_evolution.
            if (sa.ddlsTotal != null) {
                sm.put("ddls_total", sa.ddlsTotal);
                if (sa.lastDdl != null) {
                    sm.put("last_ddl", sa.lastDdl);
                }
            }
            sigs.add(sm);
        }
        m.put("signals", sigs);
        return m;
    }

    private String toJson(final Map<String, Object> m) {
        try {
            return mapper.writeValueAsString(m);
        } catch (final Exception e) {
            return "{\"error\":\"json serialization failed: " + e.getMessage() + "\"}";
        }
    }

    // =====================================================================
    // run(): Layer 1 ignores ScoredStream contents. The branch exists for
    // Layers 2/3. We drain the batch and never emit here (the timer is the
    // sole emitter). EXTENSION POINT: Layers 2/3 would consume scored events
    // here (drift on feature/fare distributions; performance vs late labels).
    // =====================================================================
    @Override
    public void run() {
        final IBatch<WAEvent> batch = getAdded();
        if (batch == null) {
            return;
        }
        for (final WAEvent ignored : batch) {
            // intentionally no-op: Layer 1 senses platform state via JMX on its
            // own timer, not from scored events.
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log("closed");
    }

    @Override
    public Map getAggVec() {
        return null;
    }

    @Override
    public void setAggVec(final Map aggVec) {
        // No-op: the agent re-perceives platform state every tick; nothing to recover.
    }

    // =====================================================================
    // JMX read helpers -- all defensive: never throw, return null on any miss.
    // =====================================================================
    private ObjectName name(final String keyProps) {
        try {
            return new ObjectName(jmxDomain + ":" + keyProps);
        } catch (final Exception e) {
            return null;
        }
    }

    private Set<ObjectName> query(final String pattern) {
        try {
            return mbs.queryNames(new ObjectName(jmxDomain + ":" + pattern), null);
        } catch (final Throwable t) {
            logError("queryNames failed for " + pattern + ": " + t);
            return java.util.Collections.emptySet();
        }
    }

    private Object getAttr(final ObjectName on, final String attr) {
        if (on == null) {
            return null;
        }
        try {
            if (!mbs.isRegistered(on)) {
                return null;
            }
            return mbs.getAttribute(on, attr);
        } catch (final Throwable t) {
            // Missing attribute / not applicable is "unknown," not an error worth shouting.
            return null;
        }
    }

    /** Total DDLs across all dispositions (processed + ignored + filtered). Returns
     *  null only when ALL inputs are null (no CDC DDL surface at all -> UNKNOWN);
     *  otherwise treats nulls as 0 and sums the present values. */
    private static Long sumDdl(final Long... parts) {
        Long total = null;
        for (final Long p : parts) {
            if (p != null) {
                total = (total == null) ? p : total + p;
            }
        }
        return total;
    }

    // ---- JSON helpers for the StriimMBean string attributes (CDC_OPERATION /
    // DDL_METRICS are JSON-valued strings). Defensive: any miss returns null. ----
    private Long jsonLong(final String json, final String field) {
        if (json == null) {
            return null;
        }
        try {
            final JsonNode n = mapper.readTree(json).get(field);
            return (n != null && n.isNumber()) ? n.asLong() : null;
        } catch (final Throwable t) {
            return null;
        }
    }

    /** First text element of a JSON array field (e.g. "Last Received DDL": ["ALTER ..."]),
     *  or the field itself if it is a plain string. Null on any miss. */
    private String jsonFirstText(final String json, final String field) {
        if (json == null) {
            return null;
        }
        try {
            final JsonNode n = mapper.readTree(json).get(field);
            if (n == null) {
                return null;
            }
            if (n.isArray()) {
                return n.size() > 0 ? n.get(0).asText() : null;
            }
            return n.isTextual() ? n.asText() : null;
        } catch (final Throwable t) {
            return null;
        }
    }

    // ---- type coercions (sentinels and nulls -> null/"unknown") ----
    private static String asString(final Object o) {
        return o == null ? null : o.toString();
    }

    private static Long asLong(final Object o) {
        if (o instanceof Number) {
            final long v = ((Number) o).longValue();
            return (v == Long.MIN_VALUE || v == -1L) ? null : v;
        }
        return null;
    }

    private static Double asDouble(final Object o) {
        if (o instanceof Number) {
            final double v = ((Number) o).doubleValue();
            return (v < 0) ? null : v;
        }
        return null;
    }

    private static Float asFloat(final Object o) {
        if (o instanceof Number) {
            final float v = ((Number) o).floatValue();
            return (v < 0f) ? null : v;
        }
        return null;
    }

    private static Boolean asBool(final Object o) {
        return (o instanceof Boolean) ? (Boolean) o : null;
    }

    private SignalAssessment signal(final String nm, final String observed, final String threshold,
                                    final SignalState state, final String detail) {
        return new SignalAssessment(nm, observed, threshold, state, detail);
    }

    private SignalAssessment unknown(final String nm, final String detail) {
        return new SignalAssessment(nm, "unknown", "n/a", SignalState.UNKNOWN, detail);
    }

    private static int parseInt(final Object v, final int dflt) {
        try {
            return Integer.parseInt(Objects.toString(v, Integer.toString(dflt)).trim());
        } catch (final NumberFormatException e) {
            return dflt;
        }
    }

    private static boolean parseBool(final Object v, final boolean dflt) {
        return Boolean.parseBoolean(Objects.toString(v, Boolean.toString(dflt)));
    }

    private static Set<String> toUpperSet(final String csv) {
        final Set<String> out = new java.util.HashSet<>();
        for (final String s : csv.split(",")) {
            final String t = s.trim().toUpperCase();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    private void log(final String m) {
        if (enableLogging) {
            System.out.println(AGENT_NAME + ": " + m);
        }
    }

    private void logError(final String m) {
        System.out.println(AGENT_NAME + " ERROR: " + m);
    }

    // ---- value holders ----
    // All PUBLIC (types, fields, constructors): these classes load via
    // ModuleClassLoader while the OP loads via OpenProcessorLoader, so the OP can
    // only touch public members across that loader boundary. Package-private here
    // is what caused the IllegalAccessError on the first deploy.
    public static final class HealthSnapshot {
        public long tickTs;
        public String appStatus;
        public final List<ComponentTime> sources = new ArrayList<>();
        public final List<ComponentTime> targets = new ArrayList<>();
        public Double maxLagMs;
        public final List<String> backpressuredComponents = new ArrayList<>();
        public final Map<String, Long> discardedByComponent = new LinkedHashMap<>();
        public Float memoryUsedPct;
        public Float cpuPct;
        public String diskFree;
        // Phase 1b: application-level OP counters (feature-store miss, NaN score),
        // one entry per OpCounters.* bean discovered. Kept as one list so this
        // signal group could be re-pointed to Layer 2 later without rework.
        public final List<OpCounter> opCounters = new ArrayList<>();
        // Layer 2 Phase 1: cumulative source DDL count ("No of DDLs") and the last
        // literal DDL statement, from the app ROLLUP StriimMBean. Null on a
        // non-CDC source (no CDC_OPERATION attribute) -> signal is UNKNOWN.
        public Long ddlCount;
        public String lastDdl;
    }

    public static final class ComponentTime {
        public final String name;
        public final Long lastMs;
        public ComponentTime(final String name, final Long lastMs) {
            this.name = name;
            this.lastMs = lastMs;
        }
    }

    public static final class SignalAssessment {
        public final String name;
        public final String observed;
        public final String threshold;
        public final SignalState state;
        public final String detail;
        // Phase 1b: cumulative OP-counter totals carried on the signal so the
        // acceptance test can assert exact counts off the emitted assessment.
        // Null for non-counter signals (omitted from the serialized output).
        public final Long eventsSeen;
        public final Long faults;
        // Layer 2 Phase 1: cumulative DDL count + last literal DDL statement,
        // carried on the schema_evolution signal so the acceptance test asserts the
        // exact count. Null for non-schema signals (omitted from serialized output).
        public final Long ddlsTotal;
        public final String lastDdl;
        public SignalAssessment(final String name, final String observed, final String threshold,
                                final SignalState state, final String detail) {
            this(name, observed, threshold, state, detail, null, null, null, null);
        }
        public SignalAssessment(final String name, final String observed, final String threshold,
                                final SignalState state, final String detail,
                                final Long eventsSeen, final Long faults) {
            this(name, observed, threshold, state, detail, eventsSeen, faults, null, null);
        }
        public SignalAssessment(final String name, final String observed, final String threshold,
                                final SignalState state, final String detail,
                                final Long eventsSeen, final Long faults,
                                final Long ddlsTotal, final String lastDdl) {
            this.name = name;
            this.observed = observed;
            this.threshold = threshold;
            this.state = state;
            this.detail = detail;
            this.eventsSeen = eventsSeen;
            this.faults = faults;
            this.ddlsTotal = ddlsTotal;
            this.lastDdl = lastDdl;
        }
    }

    /** Phase 1b: one OP-counter reading (cumulative), keyed by self-describing CounterName. */
    public static final class OpCounter {
        public final String counterName;
        public final String component;
        public final Long eventsSeen;
        public final Long faults;
        public OpCounter(final String counterName, final String component,
                         final Long eventsSeen, final Long faults) {
            this.counterName = counterName;
            this.component = component;
            this.eventsSeen = eventsSeen;
            this.faults = faults;
        }
    }

    public static final class Assessment {
        public long tickTs;
        public Verdict verdict;
        public boolean opsHealthy;
        public boolean circuitBreakerOpen;
        public int failCount;
        public int warnCount;
        public String rationale;
        public List<SignalAssessment> signals;
    }

    // =====================================================================
    // EXTENSION POINTS (out of scope for this task, marked per the plan):
    //
    // * LAYER 2 PHASE 1 (schema evolution, DDL) -- BUILT. The schema_evolution
    //   signal reads the source's cumulative DDL count from the app ROLLUP
    //   StriimMBean (CDC_OPERATION "No of DDLs"), deltas it per tick vs DdlWarnDelta,
    //   and WARNs (verdict YELLOW, no breaker). Requires a CDC source with
    //   CDDLCapture on (MySQL CDC); UNKNOWN on a file/non-CDC source.
    //   LAYER 2 PHASE 2 (design-ahead, NOT built): per-change classification
    //   (ADD/DROP/RENAME/MODIFY) parsed from the DDL_METRICS "Last Received DDL"
    //   statement, mapping to the model feature contract, severity tiers, and the
    //   DdlFailDelta + SchemaBreakOpensCircuitBreaker escalation (a CRITICAL schema
    //   break opens the breaker -> suppresses retrain), plus the LLM/MCP operator-
    //   remediation surface. The Phase 1 output already carries ddls_total + last_ddl
    //   so Phase 2 fields slot in without reworking the contract.
    //
    // * LAYER 2 (data drift): consume ScoredStream in run() and compute KS/PSI on
    //   feature and scored-fare distributions. Gate on ops_circuit_breaker_open:
    //   skip drift when Layer 1 is RED (drift over a sick pipeline is meaningless).
    //
    // * LAYER 3 (model performance): join predictions to late-arriving actual
    //   fares, roll up MAE/RMSE vs the training baseline. Same Layer 1 gate.
    //
    // * POLICY node (retrain authorization): reads this agent's ops_healthy. Its
    //   condition is: retrain if (perf breached OR (drift AND data-available))
    //   AND ops_healthy. So ops_healthy is used twice -- to gate evaluation
    //   (via ops_circuit_breaker_open) and as a precondition on authorization.
    //
    // * MCP TOOL SURFACE: the structured assessment emitted here (the
    //   toAssessmentMap object / its JSON) is the tool result an MCP server would
    //   return for a "get_operational_health" tool. A future MCP server would
    //   expose the latest Assessment (held in lastAssessment) as that tool's
    //   response, letting an LLM orchestrator (e.g. CrewAI) plan against live
    //   operational state. No MCP server or LLM integration is built here.
    //
    // * PHASE 1b (OP counters): feature-store miss rate + NaN-score rate, read in
    //   perceive() from FeatureOp/ModelOp-registered DynamicMBeans (see the
    //   extension point in perceive()).
    // =====================================================================
}
