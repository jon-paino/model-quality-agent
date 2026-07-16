"""
Point-Adjusted Evaluation Metrics for FCVAE

find_contiguous_segments() and point_adjusted_f1() extracted verbatim from
fcvae-anomaly-detection code/2_evaluate_model.py (commit 9575ba5).

Point-adjusted P/R/F1 is the DONUT/FCVAE-paper metric: any detection inside a
contiguous ground-truth anomaly segment marks the whole segment as detected;
false positives are raw predicted points outside ground-truth segments.
"""
import numpy as np


def find_contiguous_segments(labels: np.ndarray):
    """Find contiguous segments of True/1 values in a binary array.

    Returns list of (start_idx, end_idx) tuples (inclusive on both ends).
    """
    segments = []
    in_segment = False
    start = 0

    for i in range(len(labels)):
        if labels[i] and not in_segment:
            start = i
            in_segment = True
        elif not labels[i] and in_segment:
            segments.append((start, i - 1))
            in_segment = False

    if in_segment:
        segments.append((start, len(labels) - 1))

    return segments


def point_adjusted_f1(predictions: np.ndarray, ground_truth: np.ndarray) -> dict:
    """Compute point-adjusted F1 score (Best F1 from DONUT/FCVAE paper).

    If ANY point within a contiguous anomaly segment is correctly detected,
    the ENTIRE segment is counted as a true positive. This is the standard
    evaluation metric in time series anomaly detection literature.
    """
    predictions = np.asarray(predictions, dtype=bool)
    ground_truth = np.asarray(ground_truth, dtype=bool)

    gt_segments = find_contiguous_segments(ground_truth)

    tp_segments = 0
    fn_segments = 0

    for seg_start, seg_end in gt_segments:
        if np.any(predictions[seg_start:seg_end + 1]):
            tp_segments += 1
        else:
            fn_segments += 1

    # Adjust predictions: detected segments have all points as TP
    adjusted_predictions = predictions.copy()
    for seg_start, seg_end in gt_segments:
        if np.any(predictions[seg_start:seg_end + 1]):
            adjusted_predictions[seg_start:seg_end + 1] = True

    tp = int(np.sum(adjusted_predictions & ground_truth))
    fp = int(np.sum(adjusted_predictions & ~ground_truth))
    fn = int(np.sum(~adjusted_predictions & ground_truth))

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return {
        "precision": precision, "recall": recall, "f1": f1,
        "tp": tp, "fp": fp, "fn": fn,
        "tp_segments": tp_segments, "fn_segments": fn_segments,
        "total_segments": len(gt_segments),
    }
