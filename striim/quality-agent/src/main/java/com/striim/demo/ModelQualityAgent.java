package com.striim.demo;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.webaction.anno.AdapterType;
import com.webaction.anno.PropertyTemplate;
import com.webaction.anno.PropertyTemplateProperty;
import com.webaction.metaRepository.MDCache;
import com.webaction.runtime.components.openprocessor.StriimOpenProcessor;
import com.webaction.runtime.containers.IBatch;
import com.webaction.runtime.containers.WAEvent;
import com.webaction.security.Password;
import com.webaction.uuid.AuthToken;
import com.webaction.web.api.VaultAPI;

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
 *   <li>PERCEIVE ({@link #perceive}): collect the Layer 1 signals for the target
 *       application from the configured HealthSource -- MON_REST (default, the
 *       SaaS-safe mon-command + REST API) or JMX (the in-JVM platform MBeanServer,
 *       for self-managed clusters) -- both populating the same HealthSnapshot.</li>
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
        // ---- transport: how platform health is collected. MON_REST (default) uses
        // Striim's mon-command + REST API (the only SaaS-safe path; the JMX platform
        // health beans require striim.node.jmx.enabled, unavailable on SaaS). JMX
        // keeps the legacy in-JVM MBean path for self-managed clusters. ----
        @PropertyTemplateProperty(name = "HealthSource", type = String.class, required = false,
                defaultValue = "MON_REST"),
        @PropertyTemplateProperty(name = "MonRestBaseUrl", type = String.class, required = false,
                defaultValue = "http://localhost:9080"),
        @PropertyTemplateProperty(name = "MonRestUser", type = String.class, required = false,
                defaultValue = "admin"),
        @PropertyTemplateProperty(name = "MonRestPassword", type = Password.class, required = false,
                defaultValue = ""),
        @PropertyTemplateProperty(name = "MonRestTimeoutSec", type = Integer.class, required = false,
                defaultValue = "5"),
        @PropertyTemplateProperty(name = "MonRestMaxRetryNum", type = Integer.class, required = false,
                defaultValue = "2"),
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
        // ---- policy: node free-memory floor (GB). Used by the MON_REST source,
        // which exposes freeMemory (absolute) not a used-percent: WARN/FAIL when free
        // memory drops to/below the floor. The JMX source keeps MemoryWarnPct/
        // MemoryFailPct (used-%); whichever representation the source provides is
        // the one assessed. ----
        @PropertyTemplateProperty(name = "MemoryFreeWarnGb", type = Integer.class, required = false,
                defaultValue = "2"),
        @PropertyTemplateProperty(name = "MemoryFreeFailGb", type = Integer.class, required = false,
                defaultValue = "1"),
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
        // ---- ML metrics source: STREAM (default) reads the miss/NaN counters off the
        // scored stream's userdata (JMX-free); MBEAN reads the in-JVM OP-counter beans.
        // In STREAM mode the MBean is still read as a per-tick fallback when the stream
        // has no data (e.g. a 100%-miss tick emits no scored event). ----
        @PropertyTemplateProperty(name = "MlMetricsSource", type = String.class, required = false,
                defaultValue = "STREAM"),
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
    private int memFreeWarnGb, memFreeFailGb;   // MON_REST free-memory floor (GB)
    private long discardedWarnDelta, discardedFailDelta;
    private int featureMissWarnPct, featureMissFailPct, nanScoreWarnPct, nanScoreFailPct;
    private String mlMetricsSource;          // "STREAM" | "MBEAN"
    private long ddlWarnDelta;       // Layer 2 Phase 1: DDL-count delta WARN threshold
    private boolean backpressureIsFail;
    private boolean treatYellowAsHealthy;
    private boolean enableLogging;

    // ---- transport (mon/REST) config ----
    private String healthSource;             // "JMX" | "MON_REST"
    private String monRestBaseUrl;
    private String monRestUser;
    private String monRestPassword;          // vault-resolved (or literal)
    private int monRestTimeoutSec;
    private int monRestMaxRetryNum;

    // ---- runtime state ----
    private MBeanServer mbs;
    private ObjectMapper mapper;
    private ScheduledExecutorService scheduler;
    private final Object emitLock = new Object();

    // ---- mon/REST transport runtime (built in start() only for HealthSource=MON_REST) ----
    private HttpClient httpClient;
    private ExecutorService httpExecutor;    // bounds each HTTP call off the tick thread
    private volatile String monToken;        // cached STRIIM-TOKEN; re-auth on 401
    private final Object tokenLock = new Object();
    // AuthToken for vault resolution of MonRestPassword (same pattern as FeatureOp).
    private final AuthToken vaultToken = MDCache.getInstance().getWASecurityManagerToken();
    private static final DateTimeFormatter MON_TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Cumulative discarded-event counts per component, for tick-over-tick delta.
    private final Map<String, Long> lastDiscarded = new LinkedHashMap<>();

    // Phase 1b: last-seen cumulative {eventsSeen, faults} per OP counter, keyed by
    // CounterName, for computing the per-tick windowed rate (the same delta pattern
    // as lastDiscarded). The MBean stays cumulative; the windowing lives here.
    private final Map<String, long[]> lastOpCounter = new LinkedHashMap<>();

    // Week 1: cumulative ML counters aggregated off the SCORED STREAM (userdata),
    // written by run() (event thread) and read by assess() (tick thread). The
    // monotonic-max merge makes concurrent/duplicate batch reads idempotent. Its own
    // per-tick baseline (separate from lastOpCounter) drives the windowed rate.
    private final MlStreamCounters streamCounters = new MlStreamCounters();
    private final Map<String, long[]> lastStreamCounter = new LinkedHashMap<>();

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
        mlMetricsSource = Objects.toString(p.get("MlMetricsSource"), "STREAM").trim().toUpperCase();
        ddlWarnDelta = parseInt(p.get("DdlWarnDelta"), 1);
        backpressureIsFail = parseBool(p.get("BackpressureIsFail"), false);
        treatYellowAsHealthy = parseBool(p.get("TreatYellowAsHealthy"), true);
        enableLogging = parseBool(p.get("EnableLogging"), true);

        // ---- transport (mon/REST) ----
        healthSource = Objects.toString(p.get("HealthSource"), "MON_REST").trim().toUpperCase();
        monRestBaseUrl = stripTrailingSlash(
                Objects.toString(p.get("MonRestBaseUrl"), "http://localhost:9080"));
        monRestUser = Objects.toString(p.get("MonRestUser"), "admin");
        final Object monPw = p.get("MonRestPassword");
        final String monPwResolved = (monPw instanceof Password)
                ? getVaultProperty(((Password) monPw).getPlain().toString())
                : Objects.toString(monPw, "");
        monRestPassword = (monPwResolved == null) ? "" : monPwResolved;
        monRestTimeoutSec = Math.max(1, parseInt(p.get("MonRestTimeoutSec"), 5));
        monRestMaxRetryNum = Math.max(0, parseInt(p.get("MonRestMaxRetryNum"), 2));
        memFreeWarnGb = parseInt(p.get("MemoryFreeWarnGb"), 2);
        memFreeFailGb = parseInt(p.get("MemoryFreeFailGb"), 1);

        mbs = ManagementFactory.getPlatformMBeanServer();
        mapper = new ObjectMapper();

        if ("MON_REST".equals(healthSource)) {
            httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds(monRestTimeoutSec))
                    .build();
            // Small bounded pool so each HTTP call is capped by future.get(timeout)
            // and a hung request never freezes the tick thread (the sole emitter).
            httpExecutor = Executors.newFixedThreadPool(2, r -> {
                final Thread t = new Thread(r, AGENT_NAME + "-http");
                t.setDaemon(true);
                return t;
            });
        }

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

        log("started: watching " + fqApp + " via " + healthSource + " ("
                + ("MON_REST".equals(healthSource) ? monRestBaseUrl : jmxDomain)
                + ") every " + tickIntervalSec + "s");
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
    // PERCEIVE: collect Layer 1 signals into a HealthSnapshot (the seam), from
    // the configured source. MON_REST (default) uses Striim's mon-command + REST
    // API (SaaS-safe); JMX reads the in-JVM platform MBeanServer (self-managed).
    // =====================================================================
    private HealthSnapshot perceive() {
        return "MON_REST".equals(healthSource) ? perceiveMonRest() : perceiveJmx();
    }

    // ---- JMX source: read Layer 1 signals from the in-JVM platform MBeanServer. ----
    private HealthSnapshot perceiveJmx() {
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

        // OP-counter application signals (feature-store miss, NaN score) + Layer 2
        // schema-evolution DDL: both live on the in-JVM MBeanServer and are read the
        // same way regardless of source (see the shared helpers below).
        readOpCounterMbeans(s);
        readSchemaEvolutionMbean(s);
        return s;
    }

    // ---- MON_REST source: read Layer 1 signals from Striim's mon-command + REST
    // API (the SaaS-safe transport). Every HTTP call is bounded off the tick thread
    // (see runMonCommand), so a hung request degrades to UNKNOWN, never a freeze. ----
    private HealthSnapshot perceiveMonRest() {
        final HealthSnapshot s = new HealthSnapshot();
        s.tickTs = System.currentTimeMillis();
        // Backpressure and discarded_events have no clean mon/REST field (confirmed
        // by the Task 0 probe), so they stay UNKNOWN rather than reporting a false
        // PASS. discarded stays UNKNOWN via an empty map; backpressure needs a flag
        // (an empty list would otherwise assess as PASS).
        s.backpressureKnown = false;

        // mon <fqApp>; -> app_status (output.statusChange) + per-component freshness
        // (applicationComponents[].latestActivity, matched on entityType SOURCE/TARGET).
        final JsonNode appOut = monOutput("mon " + fqApp + ";");
        if (appOut != null) {
            s.appStatus = text(appOut.get("statusChange"));
            final JsonNode comps = appOut.get("applicationComponents");
            if (comps != null && comps.isArray()) {
                for (final JsonNode c : comps) {
                    final String etype = text(c.get("entityType"));
                    final String fq = text(c.get("fullName"));
                    final Long lastMs = parseMonTs(text(c.get("latestActivity")));
                    final String nm = (fq != null) ? fq : "?";
                    if ("SOURCE".equalsIgnoreCase(etype)) {
                        s.sources.add(new ComponentTime(nm, lastMs));
                    } else if ("TARGET".equalsIgnoreCase(etype)) {
                        s.targets.add(new ComponentTime(nm, lastMs));
                    }
                }
            }
        }

        // report lee; -> lagEndToEnd per source->target pair (SECONDS); take the max,
        // converted to ms for the existing threshold math (assessLag reads ms).
        final JsonNode leeOut = monOutput("report lee;");
        if (leeOut != null && leeOut.isArray()) {
            Double maxMs = null;
            for (final JsonNode row : leeOut) {
                final Double sec = parseSeconds(text(row.get("lagEndToEnd")));
                if (sec != null) {
                    final double ms = sec * 1000.0;
                    maxMs = (maxMs == null) ? ms : Math.max(maxMs, ms);
                }
            }
            s.maxLagMs = maxMs;
        }

        // mon; -> node cpu% (cpuRate) + free memory (freeMemory, absolute; assessed as
        // a free-memory floor since no used-% / total is exposed over REST).
        final JsonNode allOut = monOutput("mon;");
        if (allOut != null) {
            final JsonNode nodes = allOut.get("striimClusterNodes");
            if (nodes != null && nodes.isArray() && nodes.size() > 0) {
                final JsonNode node = nodes.get(0);
                s.cpuPct = parsePercentFloat(text(node.get("cpuRate")));
                s.memoryFreeGb = parseMemGb(text(node.get("freeMemory")));
            }
        }

        // OP counters + schema-evolution DDL are OP-/platform-registered on the in-JVM
        // MBeanServer, independent of the platform JMX health gating, so they are still
        // read where present (degrading to UNKNOWN where absent, e.g. on SaaS). Task 5
        // adds the on-stream ML path with this MBean read as the toggled fallback.
        readOpCounterMbeans(s);
        readSchemaEvolutionMbean(s);
        return s;
    }

    /** Reads the OP-counter DynamicMBeans (feature_store_miss, nan_score) from the
     *  in-JVM MBeanServer into the snapshot. Self-describing via CounterName; a
     *  missing bean simply contributes nothing (-> UNKNOWN in assess). Shared by both
     *  perceive sources: these beans are OP-self-registered, so they are readable even
     *  when the platform JMX health beans are not. */
    private void readOpCounterMbeans(final HealthSnapshot s) {
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
    }

    /** Reads the source's cumulative DDL count (schema_evolution) off the app ROLLUP
     *  StriimMBean. The count is split by disposition across CDC_OPERATION /
     *  DDL_METRICS JSON attributes, so all three are summed. Null (all absent) ->
     *  UNKNOWN, never a false alarm (non-CDC source, or the bean is absent on SaaS). */
    private void readSchemaEvolutionMbean(final HealthSnapshot s) {
        final ObjectName rollup = name("name=ROLLUP." + fqApp + ",type=StriimMBean");
        final String cdcOp = asString(getAttr(rollup, "CDC_OPERATION"));
        final String ddlMetrics = asString(getAttr(rollup, "DDL_METRICS"));
        s.ddlCount = sumDdl(jsonLong(cdcOp, "No of DDLs"),
                            jsonLong(ddlMetrics, "Ignored DDL Count"),
                            jsonLong(ddlMetrics, "Filtered DDL Count"));
        s.lastDdl = jsonFirstText(ddlMetrics, "Last Received DDL");
    }

    // =====================================================================
    // mon/REST transport: authenticate + run Tungsten console commands, each
    // bounded off the tick thread so a hung HTTP call never freezes emission.
    // =====================================================================

    /** Runs a Tungsten console command over REST and returns its {@code output} JSON
     *  node (an object for {@code mon}, an array for {@code report lee}), or null on
     *  any failure/timeout. */
    private JsonNode monOutput(final String command) {
        final String body = runMonCommand(command);
        if (body == null) {
            return null;
        }
        try {
            final JsonNode root = mapper.readTree(body);
            final JsonNode first = (root.isArray() && root.size() > 0) ? root.get(0) : root;
            if (!"Success".equalsIgnoreCase(text(first.get("executionStatus")))) {
                logError("mon command not Success: " + command
                        + " -> " + text(first.get("executionStatus")));
                return null;
            }
            return first.get("output");
        } catch (final Throwable t) {
            logError("failed to parse mon response for " + command + ": " + t);
            return null;
        }
    }

    /** Submits the HTTP call to the bounded pool and waits at most MonRestTimeoutSec,
     *  retrying up to MonRestMaxRetryNum times. A timeout cancels the worker and yields
     *  null, so the tick thread (the sole send() emitter) is never blocked. */
    private String runMonCommand(final String command) {
        if (httpExecutor == null) {
            return null;
        }
        for (int attempt = 0; attempt <= monRestMaxRetryNum; attempt++) {
            final Future<String> f = httpExecutor.submit(() -> httpTungsten(command));
            try {
                final String body = f.get(monRestTimeoutSec, TimeUnit.SECONDS);
                if (body != null) {
                    return body;
                }
            } catch (final TimeoutException te) {
                f.cancel(true);
                logError("mon/REST timed out after " + monRestTimeoutSec + "s: " + command);
            } catch (final Throwable t) {
                f.cancel(true);
                logError("mon/REST failed: " + command + " -> " + t);
            }
        }
        return null;
    }

    /** POSTs the raw command to /api/v2/tungsten with the STRIIM-TOKEN header,
     *  authenticating on demand and re-authenticating once on a 401/403. Runs on the
     *  bounded HTTP worker (never the tick thread). */
    private String httpTungsten(final String command) throws Exception {
        for (int attempt = 0; attempt < 2; attempt++) {
            final String tok = ensureToken();
            if (tok == null) {
                return null;
            }
            final HttpRequest req = HttpRequest.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .uri(URI.create(monRestBaseUrl + "/api/v2/tungsten"))
                    .header("authorization", "STRIIM-TOKEN " + tok)
                    .header("content-type", "text/plain")
                    .timeout(Duration.ofSeconds(monRestTimeoutSec))
                    .POST(HttpRequest.BodyPublishers.ofString(command))
                    .build();
            final HttpResponse<String> resp =
                    httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 401 || resp.statusCode() == 403) {
                monToken = null;   // stale token -> force re-auth on the retry
                continue;
            }
            if (resp.statusCode() != 200) {
                logError("tungsten HTTP " + resp.statusCode() + " for: " + command);
                return null;
            }
            return resp.body();
        }
        return null;
    }

    /** Returns the cached STRIIM-TOKEN, authenticating once if absent. */
    private String ensureToken() throws Exception {
        final String cached = monToken;
        if (cached != null) {
            return cached;
        }
        synchronized (tokenLock) {
            if (monToken == null) {
                monToken = authenticate();
            }
            return monToken;
        }
    }

    /** POST /security/authenticate (form-encoded); returns the JSON {@code token}
     *  field (per Striim's official rest-api-samples). Null on non-200 / no token. */
    private String authenticate() throws Exception {
        final String form = "username=" + URLEncoder.encode(monRestUser, StandardCharsets.UTF_8)
                + "&password=" + URLEncoder.encode(monRestPassword, StandardCharsets.UTF_8);
        final HttpRequest req = HttpRequest.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(monRestBaseUrl + "/security/authenticate"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(Duration.ofSeconds(monRestTimeoutSec))
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build();
        final HttpResponse<String> resp =
                httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            logError("mon/REST auth failed: HTTP " + resp.statusCode() + " at " + monRestBaseUrl);
            return null;
        }
        final JsonNode tok = mapper.readTree(resp.body()).get("token");
        return (tok != null && tok.isTextual()) ? tok.asText() : null;
    }

    // ---- mon/REST value parsers (values arrive as unit-bearing strings). ----

    private static String text(final JsonNode n) {
        return (n == null || n.isNull()) ? null : n.asText();
    }

    /** Parse "yyyy-MM-dd HH:mm:ss" (server-local zone) to epoch ms; null on empty/bad.
     *  Cross-zone robustness (cluster vs agent) is deferred; see the plan. */
    private static Long parseMonTs(final String ts) {
        if (ts == null || ts.trim().isEmpty()) {
            return null;
        }
        try {
            return LocalDateTime.parse(ts.trim(), MON_TS_FMT)
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (final Throwable t) {
            return null;
        }
    }

    /** Parse a seconds value like "0.006" (or "0.008 sec"); null on empty/"Idle"/bad. */
    private static Double parseSeconds(final String v) {
        if (v == null) {
            return null;
        }
        final String t = v.replace("sec", "").replace(",", "").trim();
        if (t.isEmpty() || "Idle".equalsIgnoreCase(t)) {
            return null;
        }
        try {
            return Double.parseDouble(t);
        } catch (final NumberFormatException e) {
            return null;
        }
    }

    /** Parse "16%" -> 16.0f; null on empty/bad. */
    private static Float parsePercentFloat(final String v) {
        if (v == null) {
            return null;
        }
        final String t = v.replace("%", "").replace(",", "").trim();
        if (t.isEmpty()) {
            return null;
        }
        try {
            return Float.parseFloat(t);
        } catch (final NumberFormatException e) {
            return null;
        }
    }

    /** Parse "3.31Gb" / "512Mb" / "2Tb" -> free memory in GB; null on empty/bad.
     *  Unitless input is assumed GB. */
    private static Float parseMemGb(final String v) {
        if (v == null) {
            return null;
        }
        final String t = v.replace(",", "").trim();
        if (t.isEmpty()) {
            return null;
        }
        try {
            final String lower = t.toLowerCase();
            final double mult;
            final String num;
            if (lower.endsWith("tb")) {
                mult = 1024.0; num = lower.substring(0, lower.length() - 2);
            } else if (lower.endsWith("gb")) {
                mult = 1.0; num = lower.substring(0, lower.length() - 2);
            } else if (lower.endsWith("mb")) {
                mult = 1.0 / 1024.0; num = lower.substring(0, lower.length() - 2);
            } else if (lower.endsWith("kb")) {
                mult = 1.0 / 1024.0 / 1024.0; num = lower.substring(0, lower.length() - 2);
            } else if (lower.endsWith("b")) {
                mult = 1.0 / 1024.0 / 1024.0 / 1024.0; num = lower.substring(0, lower.length() - 1);
            } else {
                mult = 1.0; num = lower;
            }
            return (float) (Double.parseDouble(num.trim()) * mult);
        } catch (final NumberFormatException e) {
            return null;
        }
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
        signals.add(assessBackpressure(s.backpressuredComponents, s.backpressureKnown));

        // discarded events (delta since last tick)
        signals.add(assessDiscarded(s.discardedByComponent));

        // node resources
        signals.add(assessMemory(s));
        signals.add(assessPercent("node_cpu_pct", s.cpuPct, cpuWarnPct, cpuFailPct, "%"));

        // Phase 1b: OP-counter application signals. Each is assessed on its
        // per-tick windowed rate (the operational signal: is the miss/NaN rate
        // spiking now), while the cumulative totals ride along in the signal for
        // the acceptance test to assert on exactly. A bean that is absent yields
        // UNKNOWN, never a false fault, exactly like the 1a signals.
        signals.add(assessMlSignal("feature_miss_rate", "feature_store_miss", s,
                streamCounters.featureMissEvents.get(), streamCounters.featureMissFaults.get(),
                featureMissWarnPct, featureMissFailPct));
        signals.add(assessMlSignal("nan_score_rate", "nan_score", s,
                streamCounters.nanScoreEvents.get(), streamCounters.nanScoreFaults.get(),
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

    private SignalAssessment assessBackpressure(final List<String> full, final boolean known) {
        if (!known) {
            // Not observable over the mon/REST source (no STREAM_FULL field): report
            // UNKNOWN, never a false PASS (never infer health from what we did not check).
            return unknown("backpressure", "not observable over the mon/REST source");
        }
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
     * Node memory. The JMX source provides a used-percent (assessed by threshold);
     * the MON_REST source provides only free memory (GB), assessed as a floor (low
     * free memory is bad, so the WARN threshold is higher than FAIL). Whichever
     * representation the active source populated is used; neither present -> UNKNOWN.
     */
    private SignalAssessment assessMemory(final HealthSnapshot s) {
        if (s.memoryUsedPct != null) {
            return assessPercent("node_memory_pct", s.memoryUsedPct, memWarnPct, memFailPct, "%");
        }
        if (s.memoryFreeGb != null) {
            final float freeGb = s.memoryFreeGb;
            final String observed = String.format("%.2fGb free", freeGb);
            if (freeGb <= memFreeFailGb) {
                return signal("node_memory_free", observed, "<= " + memFreeFailGb + "Gb free",
                        SignalState.FAIL, "node free memory critically low (" + observed + ")");
            }
            if (freeGb <= memFreeWarnGb) {
                return signal("node_memory_free", observed, "<= " + memFreeWarnGb + "Gb free",
                        SignalState.WARN, "node free memory low (" + observed + ")");
            }
            return signal("node_memory_free", observed, "> " + memFreeWarnGb + "Gb free",
                    SignalState.PASS, "node free memory ok");
        }
        return unknown("node_memory", "no node memory metric available");
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
     * ML signal (feature-store miss / NaN score). Week 1 moves these onto the scored
     * stream: the STREAM-aggregated cumulative is assessed first, falling back to the
     * in-JVM OP-counter MBean when the stream has no data this tick (e.g. a 100%-miss
     * tick emits no scored event, so the stream counter cannot advance). MlMetricsSource
     * = MBEAN forces the MBean. Both baselines are advanced every tick so the fallback
     * is never stale; the chosen source is named in the signal detail.
     */
    private SignalAssessment assessMlSignal(final String signalName, final String counterName,
            final HealthSnapshot s, final long streamEvents, final long streamFaults,
            final int warnPct, final int failPct) {
        final SignalAssessment fromStream = assessStreamCounter(signalName, counterName,
                streamEvents, streamFaults, warnPct, failPct);
        final SignalAssessment fromMbean = assessOpCounter(signalName, counterName, s, warnPct, failPct);
        if ("MBEAN".equals(mlMetricsSource)) {
            return fromMbean;
        }
        // STREAM (default): prefer the stream, fall back to the MBean when UNKNOWN.
        if (fromStream.state != SignalState.UNKNOWN) {
            return fromStream;
        }
        return (fromMbean.state != SignalState.UNKNOWN) ? fromMbean : fromStream;
    }

    /**
     * Windowed rate off the stream-aggregated cumulative counters, with its own
     * per-tick baseline. UNKNOWN when no stamped event has been seen at all, or when
     * no NEW events advanced this tick (a total-miss tick advances nothing -> never a
     * false PASS). Otherwise mirrors {@link #assessOpCounter}'s delta math.
     */
    private SignalAssessment assessStreamCounter(final String signalName, final String counterName,
            final long events, final long faults, final int warnPct, final int failPct) {
        if (events <= 0L) {
            lastStreamCounter.put(counterName, new long[] { 0L, 0L });
            return new SignalAssessment(signalName, "unknown", "n/a", SignalState.UNKNOWN,
                    "no " + counterName + " data on the scored stream yet", null, null);
        }
        final long[] prev = lastStreamCounter.get(counterName);
        final long dEvents = (prev == null) ? events : Math.max(0L, events - prev[0]);
        final long dFaults = (prev == null) ? faults : Math.max(0L, faults - prev[1]);
        lastStreamCounter.put(counterName, new long[] { events, faults });
        if (dEvents <= 0L) {
            return new SignalAssessment(signalName, "unknown", "n/a", SignalState.UNKNOWN,
                    "no new " + counterName + " events on the scored stream this tick", events, faults);
        }
        final double windowPct = (double) dFaults / (double) dEvents * 100.0d;
        final double cumulativePct = (double) faults / (double) events * 100.0d;
        final String observed = String.format("%.1f%% this tick (%d/%d) [stream]",
                windowPct, dFaults, dEvents);
        final String detail = String.format(
                "%s window %d/%d (%.1f%%); cumulative %d/%d (%.1f%%) [source=stream]",
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
        // Week 1: aggregate the cumulative ML counters that ModelOp / FeatureOp stamp
        // onto each scored event's userdata into the cross-thread holder. run() NEVER
        // emits -- the timer is the sole send() caller (the one-writer invariant holds).
        // The monotonic-max merge is idempotent across a batch's point-in-time stamps.
        for (final WAEvent event : batch) {
            final com.webaction.proc.events.WAEvent waevent =
                    (com.webaction.proc.events.WAEvent) event.data;
            if (waevent == null || waevent.userdata == null) {
                continue;
            }
            final Long fmE = asStampLong(waevent.userdata.get("feature_miss_events_seen"));
            final Long fmF = asStampLong(waevent.userdata.get("feature_miss_faults"));
            if (fmE != null && fmF != null) {
                streamCounters.mergeFeatureMiss(fmE, fmF);
            }
            final Long nsE = asStampLong(waevent.userdata.get("nan_score_events_seen"));
            final Long nsF = asStampLong(waevent.userdata.get("nan_score_faults"));
            if (nsE != null && nsF != null) {
                streamCounters.mergeNanScore(nsE, nsF);
            }
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
        if (httpExecutor != null) {
            httpExecutor.shutdownNow();
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

    /** Coerce a userdata stamp (Number or numeric String) to Long; null otherwise. */
    private static Long asStampLong(final Object o) {
        if (o instanceof Number) {
            return ((Number) o).longValue();
        }
        if (o instanceof String) {
            try {
                return Long.parseLong(((String) o).trim());
            } catch (final NumberFormatException e) {
                return null;
            }
        }
        return null;
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

    private static String stripTrailingSlash(final String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    /** Resolves a vault-backed property of the form {@code [ns.vault.prop]}; returns
     *  the input unchanged if it is not a vault reference. Same pattern as FeatureOp. */
    private String getVaultProperty(final String name) {
        try {
            final String cleanName = name.replaceAll("[\\[\\]]", "");
            if (cleanName.equals(name)) {
                return name;   // not a vault reference -> literal value
            }
            final String[] parts = cleanName.split("\\.");
            final String vaultId = String.format("%s.VAULT.%s", parts[0], parts[1]);
            return new VaultAPI().getValue(vaultToken, vaultId, parts[2]).value;
        } catch (final Exception e) {
            logError("could not resolve vault property '" + name + "': " + e.getMessage());
            return null;
        }
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
        public Float memoryFreeGb;         // MON_REST: free memory (GB), floor-assessed
        public Float cpuPct;
        public String diskFree;
        // Whether backpressure was observable this tick. False on the MON_REST source
        // (no STREAM_FULL field), so backpressure reports UNKNOWN, not a false PASS.
        public boolean backpressureKnown = true;
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

    /**
     * Week 1: cumulative ML counters carried on the scored stream (userdata),
     * aggregated by run(). PUBLIC holder per the .scm class-loader rule (accessed
     * across the OpenProcessorLoader / ModuleClassLoader boundary). No declared
     * constructor, so the implicit public no-arg ctor exists for Striim's reflective
     * instantiation of OP field classes at start. The monotonic-max merge is
     * idempotent under out-of-order / duplicated batch reads and the run()/assess()
     * thread split.
     */
    public static final class MlStreamCounters {
        public final AtomicLong featureMissEvents = new AtomicLong();
        public final AtomicLong featureMissFaults = new AtomicLong();
        public final AtomicLong nanScoreEvents = new AtomicLong();
        public final AtomicLong nanScoreFaults = new AtomicLong();

        public void mergeFeatureMiss(final long events, final long faults) {
            featureMissEvents.updateAndGet(prev -> Math.max(prev, events));
            featureMissFaults.updateAndGet(prev -> Math.max(prev, faults));
        }

        public void mergeNanScore(final long events, final long faults) {
            nanScoreEvents.updateAndGet(prev -> Math.max(prev, events));
            nanScoreFaults.updateAndGet(prev -> Math.max(prev, faults));
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
