import Foundation

/// A transport name must resolve to the same key used by the target journal.
/// Parsing a numeric/UUID/ObjectId alias without this round trip lets two
/// CloudKit names address one Realm object while bypassing each other's work.
/// String primary keys remain opaque; they are not case-folded or normalized.
enum BigSyncRecordIdentifier {
    /// Deletions without tracking have only the record name. Match the ASCII
    /// separator itself, even when the opaque suffix starts with a combining mark.
    static func entityType(from recordName: String) -> String? {
        let bytes = recordName.utf8
        guard let separator = bytes.firstIndex(of: 46), separator != bytes.startIndex
        else { return nil }
        return String(decoding: bytes[..<separator], as: UTF8.self)
    }

    static func objectIdentifier(
        from recordName: String,
        entityType: String
    ) -> String? {
        guard !entityType.isEmpty else { return nil }
        let prefix = entityType + "."
        guard recordName.utf8.starts(with: prefix.utf8) else { return nil }
        // Slice bytes, not Characters: a leading combining mark in an opaque
        // string key may form a grapheme cluster with the separator itself.
        let suffix = recordName.utf8.dropFirst(prefix.utf8.count)
        return suffix.isEmpty ? nil : String(decoding: suffix, as: UTF8.self)
    }

    static func canonicalValue<Value: CustomStringConvertible>(
        from representation: String,
        parse: (String) -> Value?
    ) -> Value? {
        guard let value = parse(representation),
              String(describing: value).utf8.elementsEqual(representation.utf8)
        else { return nil }
        return value
    }
}
