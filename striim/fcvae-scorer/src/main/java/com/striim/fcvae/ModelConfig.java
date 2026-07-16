package com.striim.fcvae;

/**
 * POJO for model_config.json, parsed via Gson.
 *
 * <p>Vendored from fcvae-anomaly-detection/striim/fcvae-onnx-scorer (F0 scaffold).
 * F1: every numeric leaf is BOXED (Double/Integer) on purpose, so Gson maps a
 * missing JSON field to null instead of a silent primitive 0.0/0. The
 * validateConfig gate in FCVAEOnnxScorer.buildHandle rejects a null or
 * non-finite leaf before the config can go live, so a config missing its
 * threshold can never silently score with threshold 0.0.
 */
public class ModelConfig {
    public String model_name;
    public OnnxConfig onnx;
    public ScalerConfig scaler;
    public ThresholdConfig thresholds;
    public ScorerStats scorer;

    public static class OnnxConfig {
        public Integer opset_version;
        public Integer window_size;
        public String input_name;
        public String output_name;
    }

    public static class ScalerConfig {
        public String type;
        public Double mean;
        public Double scale;
    }

    public static class ThresholdConfig {
        public Double last_point_threshold;
        public Double point_threshold;
        public Double window_threshold;
    }

    public static class ScorerStats {
        public Double normal_score_mean;
        public Double normal_score_std;
    }
}
