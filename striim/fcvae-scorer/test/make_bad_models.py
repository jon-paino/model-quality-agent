#!/usr/bin/env python3
"""Emit ONNX artifacts that violate the FCVAE scorer's signature gate, one way each.

The live FCVAE contract (python/fcvae/config.py, mirrored by the F1 scorer gate):

    input  name "input", dtype float32, shape [-1, 1, 24]
    output name "nll",   dtype float32, shape [-1, 24]

Three VALID, LOADABLE models are written, each breaking exactly ONE contract
dimension, so a live rejection is provably the signature gate and not a load
failure:

  bad_input_name.onnx  input renamed "window"; dtype/shapes correct.
  bad_shape.onnx       window size 12 instead of 24; names/dtype correct.
  bad_dtype.onnx       float64 on both sides; names/shapes correct.

Each graph is tiny and deterministic: Reshape [-1,1,W] -> [-1,W], then MatMul
with a WxW identity initializer, following make_swap_artifacts.py conventions
(opset 13, ir_version 9, onnx.checker on each). After writing, every artifact
is loaded through onnxruntime.InferenceSession and its IO metadata printed to
prove loadability.

Usage:
    uv run python striim/fcvae-scorer/test/make_bad_models.py [--out-dir DIR]
"""
import os

import click
import numpy as np
import onnx
import onnxruntime as ort
from onnx import TensorProto, helper, numpy_helper

DEFAULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "artifacts")


def make_fcvae_like(path, *, input_name="input", window=24, elem=TensorProto.FLOAT):
    """Write a minimal FCVAE-shaped model: nll = reshape(input, [-1,W]) @ I_W."""
    np_dtype = np.float32 if elem == TensorProto.FLOAT else np.float64
    inp = helper.make_tensor_value_info(input_name, elem, [None, 1, window])
    out = helper.make_tensor_value_info("nll", elem, [None, window])
    shape_init = numpy_helper.from_array(np.array([-1, window], dtype=np.int64),
                                         name="tgt_shape")
    w_init = numpy_helper.from_array(np.eye(window, dtype=np_dtype), name="W")
    reshape = helper.make_node("Reshape", [input_name, "tgt_shape"], ["flat"])
    matmul = helper.make_node("MatMul", ["flat", "W"], ["nll"])
    graph = helper.make_graph([reshape, matmul], "fcvae_bad", [inp], [out],
                              initializer=[shape_init, w_init])
    model = helper.make_model(graph, opset_imports=[helper.make_opsetid("", 13)])
    model.ir_version = 9
    onnx.checker.check_model(model)
    onnx.save(model, path)
    return path


def describe_ort(path):
    """LOAD through onnxruntime and print the IO metadata, proving loadability."""
    sess = ort.InferenceSession(path, providers=["CPUExecutionProvider"])

    def fmt(nodes):
        return ", ".join(f"{n.name}:{n.type}{n.shape}" for n in nodes)

    print(f"  {os.path.basename(path):22s} ORT-LOADED "
          f"in[{fmt(sess.get_inputs())}] out[{fmt(sess.get_outputs())}]")


@click.command(help=__doc__)
@click.option("--out-dir", default=DEFAULT_DIR, show_default=True,
              type=click.Path(file_okay=False),
              help="Directory to write the artifacts.")
def main(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    out = lambda name: os.path.join(out_dir, name)  # noqa: E731

    written = [
        # Wrong input NAME: "window" instead of "input"; everything else correct.
        make_fcvae_like(out("bad_input_name.onnx"), input_name="window"),
        # Wrong SHAPE: window 12 instead of 24 ([-1,1,12] -> [-1,12]); names/dtype correct.
        make_fcvae_like(out("bad_shape.onnx"), window=12),
        # Wrong DTYPE: float64 both sides; names/shapes correct.
        make_fcvae_like(out("bad_dtype.onnx"), elem=TensorProto.DOUBLE),
    ]

    print(f"Wrote {len(written)} artifacts to {out_dir}:")
    for path in written:
        describe_ort(path)
    print("\nThe live FCVAE contract is in[input:float32[-1,1,24]] out[nll:float32[-1,24]];")
    print("all three artifacts load fine but each violates exactly one contract dimension,")
    print("so the scorer's signature gate must REJECT every one of them.")


if __name__ == "__main__":
    main()
