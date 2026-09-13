from pathlib import Path

path = Path('.github/apply-authority-poison-followup.py')
code = path.read_text()

old_indent = '''old_run = "\\n".join([
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
new_indent = '''HANG = "              "
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
if code.count(old_indent) != 1:
    raise SystemExit('composer hanging-indent block changed')
code = code.replace(old_indent, new_indent, 1)

old_helper = '''    II + "guard try accountScopeAuthorityFence.withAdmissibleOperationGeneration(",
    III + "fence.authorityGeneration,",
    III + "{ try body(); return true }",
    II + ") == true else {",
    III + "throw CancellationError()",
    II + "}",'''
new_helper = '''    II + "let committed = try accountScopeAuthorityFence",
    III + ".withAdmissibleOperationGeneration(",
    III + I + "fence.authorityGeneration,",
    III + I + "{ try body(); return true }",
    III + ")",
    II + "guard committed == true else {",
    III + "throw CancellationError()",
    II + "}",'''
if code.count(old_helper) != 1:
    raise SystemExit('composer metadata-commit helper changed')
code = code.replace(old_helper, new_helper, 1)

exec(compile(code, str(path), 'exec'))
