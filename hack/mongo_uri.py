#!/usr/bin/env python3
"""
MongoDB URI helpers.

Connection strings carry credentials, so they should not be typed on the command
line (shell history, visible process arguments) or printed verbatim. Single-cluster
scripts read MONGO_URI; dual-cluster scripts use SRC_URI and TGT_URI.

Usage:
    export MONGO_URI="mongodb://localhost:27017"
    hack/generator.py -r 100

    MONGO_URI="$TGT_URI" hack/change_stream.py
"""

import os
import re
import sys

URI_ENV_VAR = "MONGO_URI"

_CREDENTIALS = re.compile(r"://[^@/]*@")


def redact_uri(uri: str) -> str:
    """Mask credentials in a connection string so it is safe to display."""
    return _CREDENTIALS.sub("://***@", uri)


def resolve_uri(uri: str | None) -> str:
    """Return the URI from the --uri argument or MONGO_URI, whichever is set.

    Prints an error and exits when neither is available.
    """
    resolved = uri or os.environ.get(URI_ENV_VAR)
    if not resolved:
        print(f"ERROR: no MongoDB URI: pass --uri or set {URI_ENV_VAR}")
        sys.exit(1)

    return resolved
