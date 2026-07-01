"""NYC Taxi real-time ML inference demo -- offline foundation package."""

import logging
import warnings

__version__ = "0.1.0"

# Feast on Python 3.12 prints noisy but harmless messages on every sqlite
# connection: DeprecationWarnings for the sqlite datetime adapter and
# timestamp converter, plus a WARNING:root for missing sqlite_vec (we do
# not do vector search). Silence them so every `uv run python -m model.<...>`
# is quiet.
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, message=r"The default"
)

# Feast also prepends its own ("once", DeprecationWarning) filter at index 0
# when imported, which sits above the filterwarnings entry above and defeats
# it. Drop the sqlite deprecations at the showwarning() display step where
# filter ordering does not matter.
_orig_showwarning = warnings.showwarning


def _showwarning(message, category, filename, lineno, file=None, line=None):
    if (issubclass(category, DeprecationWarning)
            and str(message).startswith("The default")):
        return
    return _orig_showwarning(message, category, filename, lineno, file, line)


warnings.showwarning = _showwarning

logging.getLogger().addFilter(
    lambda rec: "sqlite_vec" not in rec.getMessage()
)
