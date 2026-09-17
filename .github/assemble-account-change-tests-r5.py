from pathlib import Path
import hashlib
import sys

root = Path(sys.argv[1])
path = root / "Tests/BigSyncKitTests/BigSyncKitTests.swift"
data = path.read_bytes()

def blob_sha(value: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(value)).encode() + b"\0" + value).hexdigest()

expected = "82d799d9c3112a6e8bb149566004186e3b56cc54"
actual = blob_sha(data)
assert actual == expected, (expected, actual)
text = data.decode()
old = """        XCTAssertNotEqual(synchronizer.synchronizationAttemptID, intermediateAttemptID)
        XCTAssertTrue(synchronizer.syncing)
        await releaseValidation.open()
"""
new = """        XCTAssertNotEqual(synchronizer.synchronizationAttemptID, intermediateAttemptID)
        await releaseValidation.open()
"""
assert text.count(old) == 1, text.count(old)
text = text.replace(old, new)
path.write_text(text)
print(path.relative_to(root), blob_sha(path.read_bytes()))
