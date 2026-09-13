#!/usr/bin/env python3
from pathlib import Path

path = Path(__file__).with_name("apply_ra1_account_validation_generation_fence.py")
text = path.read_text(encoding="utf-8")
old = '''# No stale, generation-less production entry point may remain.\nif "validateSynchronizationAccount()" in source:\n    raise RuntimeError("generation-less validateSynchronizationAccount call remains")\n\n'''
if text.count(old) != 1:
    raise RuntimeError("expected obsolete broad assertion exactly once")
text = text.replace(old, '''# Only parameterized production invocations are legal; the DEBUG helper's own\n# name intentionally contains the same suffix, so do not use a raw substring\n# check that mistakes the helper declaration for a call site.\nif "try await validateSynchronizationAccount()" in source:\n    raise RuntimeError("generation-less validateSynchronizationAccount invocation remains")\n\n''')
path.write_text(text, encoding="utf-8")
