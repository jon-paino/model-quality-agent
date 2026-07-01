package com.striim.demo;

import java.util.concurrent.atomic.AtomicLong;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

/**
 * OpCounterMBean -- a self-describing application-level counter an Open Processor
 * registers under the {@code com.striim.metrics} JMX domain so the
 * ModelQualityAgent can read it the same way it reads every other health signal:
 * pure JMX, no stream, no network hop. This is Phase 1b "source (2)": the two
 * most ML-meaningful Layer 1 signals (feature-store miss rate, NaN-score rate)
 * are NOT Striim platform metrics and do not exist in JMX by default; this bean
 * is how an OP exposes them.
 *
 * <p>PUBLIC and TOP-LEVEL on purpose. Under {@code .scm} module isolation the OP
 * main class loads via {@code OpenProcessorLoader} while every other class in the
 * jar loads via {@code ModuleClassLoader}; a package-private or nested MBean
 * throws {@code IllegalAccessError} across those two loaders at runtime (this bit
 * the probe and the agent earlier). A public top-level class is accessible across
 * loaders, and a {@link DynamicMBean} that returns only JDK types
 * (String/long/double) crosses cleanly. This is the exact shape the JMX probe
 * proved registrable.
 *
 * <p>Semantics: this bean holds two CUMULATIVE, MONOTONIC counters,
 * {@code eventsSeen} (the denominator -- events the OP processed) and
 * {@code faults} (the numerator -- the fault this OP detects: a feature-store
 * miss in FeatureOp, a NaN score in ModelOp). It deliberately does NOT maintain a
 * windowed rate: the agent reads attributes one at a time, so an in-bean
 * read-and-reset window would be inconsistent across reads. The agent computes
 * the per-tick windowed rate from tick-over-tick deltas of these cumulative
 * counters (the same way it already windows {@code discarded_events}), and the
 * acceptance test asserts on the exact cumulative totals, which never reset.
 *
 * <p>Counters are written on the OP's {@code run()} thread and read on the
 * agent's timer thread, so both are {@link AtomicLong}. {@code FaultRate} is a
 * convenience cumulative ratio for inspection / a future MCP tool; the agent does
 * not depend on it (it recomputes from {@code EventsSeen} / {@code Faults}).
 *
 * <p>{@code CounterName} is the self-describing discriminator the agent keys on
 * ({@code feature_store_miss} vs {@code nan_score}), so both OPs can register
 * structurally identical beans and the agent tells them apart without hardcoding
 * component names.
 */
public class OpCounterMBean implements DynamicMBean {

    private final String counterName;   // e.g. "feature_store_miss" or "nan_score"
    private final String namespace;     // the MetricsNamespace label (display/scoping)
    private final String component;     // the MetricsComponent label (display/scoping)

    private final AtomicLong eventsSeen = new AtomicLong(0L);
    private final AtomicLong faults = new AtomicLong(0L);

    /**
     * Public no-arg constructor. REQUIRED: Striim reflectively instantiates the
     * classes an OP holds as fields (recovery/serialization) at deploy/start, and
     * that path needs a no-arg {@code <init>()} -- without one, START fails with
     * {@code NoSuchMethodException: com.striim.demo.OpCounterMBean.<init>()}. The
     * probe's MBean only worked because it declared no constructor, so the
     * implicit default was present. Declaring the 3-arg constructor below
     * suppresses that default, so this one is added back explicitly. The OP never
     * calls it; the runtime counter is always built via the 3-arg form in start().
     */
    public OpCounterMBean() {
        this("unknown", "", "");
    }

    public OpCounterMBean(final String counterName, final String namespace, final String component) {
        this.counterName = counterName;
        this.namespace = namespace;
        this.component = component;
    }

    /** One event processed by the OP (the rate denominator). Called on run(). */
    public void recordEvent() {
        eventsSeen.incrementAndGet();
    }

    /** One fault detected (miss / NaN score; the rate numerator). Called on run(). */
    public void recordFault() {
        faults.incrementAndGet();
    }

    public long getEventsSeen() {
        return eventsSeen.get();
    }

    public long getFaults() {
        return faults.get();
    }

    @Override
    public Object getAttribute(final String attribute) {
        if (attribute == null) {
            return null;
        }
        switch (attribute) {
            case "CounterName":
                return counterName;
            case "Namespace":
                return namespace;
            case "Component":
                return component;
            case "EventsSeen":
                return eventsSeen.get();
            case "Faults":
                return faults.get();
            case "FaultRate": {
                final long e = eventsSeen.get();
                final long f = faults.get();
                return e > 0L ? (double) f / (double) e : 0.0d;
            }
            default:
                return null;
        }
    }

    @Override
    public void setAttribute(final Attribute attribute) {
        // Read-only counter: nothing is settable over JMX.
    }

    @Override
    public AttributeList getAttributes(final String[] attributes) {
        final AttributeList list = new AttributeList();
        if (attributes == null) {
            return list;
        }
        for (final String name : attributes) {
            final Object value = getAttribute(name);
            if (value != null) {
                list.add(new Attribute(name, value));
            }
        }
        return list;
    }

    @Override
    public AttributeList setAttributes(final AttributeList attributes) {
        return new AttributeList();
    }

    @Override
    public Object invoke(final String actionName, final Object[] params, final String[] signature) {
        return null;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        final MBeanAttributeInfo[] attrs = new MBeanAttributeInfo[] {
                new MBeanAttributeInfo("CounterName", "java.lang.String",
                        "self-describing counter id (feature_store_miss / nan_score)", true, false, false),
                new MBeanAttributeInfo("Namespace", "java.lang.String",
                        "metrics namespace label", true, false, false),
                new MBeanAttributeInfo("Component", "java.lang.String",
                        "metrics component label", true, false, false),
                new MBeanAttributeInfo("EventsSeen", "long",
                        "cumulative events processed (rate denominator)", true, false, false),
                new MBeanAttributeInfo("Faults", "long",
                        "cumulative faults detected (rate numerator)", true, false, false),
                new MBeanAttributeInfo("FaultRate", "double",
                        "cumulative faults / eventsSeen (convenience)", true, false, false)
        };
        return new MBeanInfo(OpCounterMBean.class.getName(),
                "Open Processor application-level counter (Quality Agent Phase 1b)",
                attrs, null, null, null);
    }
}
