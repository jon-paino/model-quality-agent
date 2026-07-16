"""Pickle-compatibility shims for artifacts created in the sibling FCVAE repo.

The sibling repo's model.pt embeds a pickled FCVAEConfig instance (module path
`src.model`), scorer.pkl embeds an FCVAEScorerConfig (`src.scorer`), and older
checkpoints were pickled under `app.fcvae_model` / bare-module paths. Unpickling
them here requires those module names to resolve to our vendored modules.

Call install_pickle_shims() before any torch.load / pickle.load of
sibling-repo artifacts. Registration is idempotent (setdefault) and never
overrides a genuinely importable module.
"""

import sys
import types


def install_pickle_shims() -> None:
    from fcvae import model as _model
    from fcvae import scorer as _scorer

    # Parent packages must exist in sys.modules for pickle's
    # __import__("src.model") to succeed when only the submodule is aliased.
    for parent in ("src", "app"):
        sys.modules.setdefault(parent, types.ModuleType(parent))

    aliases = {
        "src.model": _model,
        "src.scorer": _scorer,
        "app.fcvae_model": _model,
        "app.fcvae_scorer": _scorer,
        "app.attention": _model,
        "fcvae_model": _model,
        "fcvae_scorer": _scorer,
        "attention": _model,
    }
    for name, module in aliases.items():
        sys.modules.setdefault(name, module)
