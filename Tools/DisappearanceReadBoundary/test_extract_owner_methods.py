import importlib.util
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest

TOOL = Path(__file__).with_name('extract-owner-methods.py')
spec = importlib.util.spec_from_file_location('owner_extractor', TOOL)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

FIXTURE = '''extension RealmSwiftAdapter {
    @BigSyncBackgroundActor
    func currentRecordEvidenceCut() throws -> BigSyncRecordEvidenceCut {
        if ready {
            return cut
        }
        throw CancellationError()
    }

    @BigSyncBackgroundActor
    func validateRecordEvidenceCut(_ cut: BigSyncRecordEvidenceCut, in realm: Realm) throws {
        try validate(cut)
    }
}
'''


class OwnerExtractorTests(unittest.TestCase):
    def test_copies_both_complete_declarations_verbatim(self):
        result = module.extract(FIXTURE)
        self.assertIn(FIXTURE.strip(), result)
        self.assertTrue(result.startswith('import Foundation\nimport RealmSwift\n'))

    def test_rejects_missing_method(self):
        with self.assertRaises(ValueError):
            module.extract(FIXTURE.replace('func validateRecordEvidenceCut', 'func unrelated'))

    def test_rejects_duplicate_method(self):
        with self.assertRaises(ValueError):
            module.extract(FIXTURE + FIXTURE)

    def test_rejects_changed_access_or_isolation(self):
        for source in (FIXTURE.replace('    func current', '    private func current'),
                       FIXTURE.replace('@BigSyncBackgroundActor', '@MainActor', 1)):
            with self.assertRaises(ValueError):
                module.extract(source)

    def test_rejects_unbalanced_body(self):
        with self.assertRaises(ValueError):
            module.extract(FIXTURE.replace('        if ready {', '        if ready {{'))

    def test_rejects_ambiguous_lexical_body(self):
        for replacement in ('        print("{ brace")', '        // } comment', '        /* } */'):
            with self.assertRaises(ValueError):
                module.extract(FIXTURE.replace('        try validate(cut)', replacement))

    def test_cli_never_overwrites_existing_destination(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source, target = root/'input.swift', root/'output.swift'
            source.write_text(FIXTURE)
            target.write_text('original sentinel')
            outcome = subprocess.run([sys.executable, str(TOOL), str(source), str(target)],
                                     capture_output=True, text=True)
            self.assertNotEqual(outcome.returncode, 0)
            self.assertEqual(target.read_text(), 'original sentinel')

    def test_invalid_input_never_creates_destination(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source, target = root/'input.swift', root/'output.swift'
            source.write_text('unrelated input')
            outcome = subprocess.run([sys.executable, str(TOOL), str(source), str(target)],
                                     capture_output=True, text=True)
            self.assertNotEqual(outcome.returncode, 0)
            self.assertFalse(target.exists())


if __name__ == '__main__':
    unittest.main()
