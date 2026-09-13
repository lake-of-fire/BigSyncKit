from pathlib import Path

path = Path('.github/apply-authority-poison-followup.py')
code = path.read_text()
old = '''old_run = "\\n".join([
    II + "guard activeRunContext == context,",
    III + "synchronizationAttemptID == context.attemptID,",
    III + "synchronizationRunID == context.runID,",
    III + "!cancelSync else {",
])
new_run = "\\n".join([
    II + "guard activeRunContext == context,",
    III + "synchronizationAttemptID == context.attemptID,",
    III + "synchronizationRunID == context.runID,",
    III + "!accountScopeAuthorityFence.rejectsAuthority,",
    III + "!cancelSync else {",
])'''
new = '''HANG = "              "
old_run = "\\n".join([
    II + "guard activeRunContext == context,",
    HANG + "synchronizationAttemptID == context.attemptID,",
    HANG + "synchronizationRunID == context.runID,",
    HANG + "!cancelSync else {",
])
new_run = "\\n".join([
    II + "guard activeRunContext == context,",
    HANG + "synchronizationAttemptID == context.attemptID,",
    HANG + "synchronizationRunID == context.runID,",
    HANG + "!accountScopeAuthorityFence.rejectsAuthority,",
    HANG + "!cancelSync else {",
])'''
count = code.count(old)
if count != 1:
    raise SystemExit(f'composer hanging-indent block changed: {count}')
code = code.replace(old, new, 1)
exec(compile(code, str(path), 'exec'))
