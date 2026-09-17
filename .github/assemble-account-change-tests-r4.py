from pathlib import Path
import hashlib
import sys

root = Path(sys.argv[1])


def blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()

path = root / "Tests/BigSyncKitTests/CloudKitSynchronizerAccountFencingTests.swift"
data = path.read_bytes()
actual = blob_sha(data)
expected = "d9a0939425ed5011c1489babdf7d29c460526065"
assert actual == expected, (expected, actual)
text = data.decode()
old = '''        let recorder = AccountScopeInvalidationRecorder()
        synchronizer.accountScopeInvalidationHandler = { reason in
            let lease: BigSyncAccountScopeLease?
            do {
                lease = try synchronizer.accountScopeLease()
            } catch {
                XCTFail("Could not read invalidated account lease: \\(error)")
                return
            }
            XCTAssertNil(lease)
            await recorder.record(reason)
        }
'''
new = '''        let recorder = AccountScopeInvalidationRecorder()
        synchronizer.accountScopeInvalidationHandler = { [weak synchronizer] reason in
            guard let synchronizer else {
                XCTFail("Synchronizer was released during account invalidation")
                return
            }
            let lease: BigSyncAccountScopeLease?
            do {
                lease = try synchronizer.accountScopeLease()
            } catch {
                XCTFail("Could not read invalidated account lease: \\(error)")
                return
            }
            XCTAssertNil(lease)
            await recorder.record(reason)
        }
'''
assert text.count(old) == 1, text.count(old)
text = text.replace(old, new)
path.write_text(text)
print(path.relative_to(root), blob_sha(path.read_bytes()))
