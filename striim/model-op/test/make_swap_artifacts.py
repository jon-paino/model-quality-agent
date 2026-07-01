#!/usr/bin/env python3
"""Emit ONNX test artifacts that exercise ModelOp's production-safe swap.

Scenario A: training and ONNX conversion happen here, OUTSIDE the JVM. Each
artifact is a tiny deterministic linear model y = X @ W + b so its predictions
are observable and differ from the live model and from each other. ModelOp's
signature gate derives the expected contract from the currently-loaded model:

    input  name "input",    dtype float32, shape [-1, 15]
    output name "variable", dtype float32, shape [-1,  1]

Four artifacts are written:

  swap_good_v2.onnx  -- matches the live contract exactly. The valid swap and the
                        first rollback target. Predicts distinctively (W=2*idx, b=50).
  swap_bad_name.onnx -- input renamed "features" (wrong input NAME -> rejected).
  swap_bad_shape.onnx-- input shape [-1, 12] (wrong fixed DIM -> rejected).
  swap_bad_dtype.onnx-- input dtype float64 (wrong DTYPE -> rejected).

Usage:
    .venv/bin/python striim/model-op/test/make_swap_artifacts.py [--out-dir DIR]

The companion run_swap_acceptance.sh copies these onto UploadedFiles/model.onnx.
"""
import argparse
import os

import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper


def make_linear(path, *, input_name="input", n_features=15,
                dtype=TensorProto.FLOAT, w_scale=1.0, bias=0.0):
    """Write a minimal linear ONNX model: variable = input @ W + b."""
    np_dtype = np.float32 if dtype == TensorProto.FLOAT else np.float64

    # W[n_features, 1] = w_scale * [1, 2, ..., n_features]; b[1] = bias.
    weights = (np.arange(1, n_features + 1, dtype=np_dtype).reshape(n_features, 1) * w_scale)
    bias_arr = np.array([bias], dtype=np_dtype)
    w_init = numpy_helper.from_array(weights, name="W")
    b_init = numpy_helper.from_array(bias_arr, name="b")

    inp = helper.make_tensor_value_info(input_name, dtype, [None, n_features])
    out = helper.make_tensor_value_info("variable", dtype, [None, 1])
    matmul = helper.make_node("MatMul", [input_name, "W"], ["mm"])
    add = helper.make_node("Add", ["mm", "b"], ["variable"])
    graph = helper.make_graph([matmul, add], "linear", [inp], [out],
                              initializer=[w_init, b_init])
    model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
    # IR 9 is broadly supported by the onnxruntime ModelOp bundles (1.x).
    model.ir_version = 9
    onnx.checker.check_model(model)
    onnx.save(model, path)
    return path


def describe(path):
    """Print the IO contract of an ONNX file, the way ModelOp reads it."""
    model = onnx.load(path)

    def fmt(value_infos):
        parts = []
        for vi in value_infos:
            t = vi.type.tensor_type
            dims = [d.dim_value if d.HasField("dim_value") else -1 for d in t.shape.dim]
            elem = TensorProto.DataType.Name(t.elem_type)
            parts.append(f"{vi.name}:{elem}{dims}")
        return ", ".join(parts)

    print(f"  {os.path.basename(path):20s} in[{fmt(model.graph.input)}] "
          f"out[{fmt(model.graph.output)}]")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    default_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "artifacts")
    ap.add_argument("--out-dir", default=default_dir,
                    help=f"directory to write the artifacts (default: {default_dir})")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    out = lambda name: os.path.join(args.out_dir, name)

    written = [
        # Valid: matches the live contract; distinctive predictions (W=2*idx, b=50).
        make_linear(out("swap_good_v2.onnx"), w_scale=2.0, bias=50.0),
        # Wrong input NAME.
        make_linear(out("swap_bad_name.onnx"), input_name="features", w_scale=1.0, bias=0.0),
        # Wrong fixed DIM (12 features instead of 15).
        make_linear(out("swap_bad_shape.onnx"), n_features=12, w_scale=1.0, bias=0.0),
        # Wrong DTYPE (float64 instead of float32).
        make_linear(out("swap_bad_dtype.onnx"), dtype=TensorProto.DOUBLE, w_scale=1.0, bias=0.0),
    ]

    print(f"Wrote {len(written)} artifacts to {args.out_dir}:")
    for path in written:
        describe(path)
    print("\nThe live model's expected contract is "
          "in[input:FLOAT[-1, 15]] out[variable:FLOAT[-1, 1]];")
    print("only swap_good_v2.onnx matches it. The other three must be REJECTED by ModelOp.")


if __name__ == "__main__":
    main()
