import Foundation
import SwiftParser
import SwiftSyntax

// Syntax, not a source-text grep: comments and strings are not identifiers;
// interpolation, aliases and inactive #if branches still contain real tokens.
// Reserve this member name throughout production Sources. This is deliberately
// stricter than receiver-type analysis: new wrappers may not hide a raw call.
enum BoundaryError: Error, CustomStringConvertible {
    case failure(String)
    var description: String {
        switch self { case .failure(let message): return message }
    }
}

func forbiddenLocations(_ source: String, file: String) throws -> [String] {
    let syntax = Parser.parse(source: source)
    guard !syntax.hasError else {
        throw BoundaryError.failure("\(file): Swift parse failed; ownership check is inconclusive")
    }
    let converter = SourceLocationConverter(fileName: file, tree: syntax)
    return syntax.tokens(viewMode: .all).compactMap { token in
        guard case .identifier = token.tokenKind,
              token.text.trimmingCharacters(in: CharacterSet(charactersIn: "`")) == "asyncWrite" else {
            return nil
        }
        let location = converter.location(for: token.positionAfterSkippingLeadingTrivia)
        return "\(file):\(location.line):\(location.column)"
    }
}

func selfTest() throws {
    let cases: [(String, String, Bool)] = [
        ("direct call", "func f() async throws { try await realm.asyncWrite {} }", true),
        ("method reference", "let callback = realm.asyncWrite", true),
        ("escaped identifier", "let callback = realm.`asyncWrite`", true),
        ("split member", "let callback = realm\n .asyncWrite", true),
        ("inactive condition", "#if NEVER_DEFINED\nlet callback = realm.asyncWrite\n#endif", true),
        ("conditional alternatives", "#if DEBUG\nlet a = 1\n#else\nlet callback = realm.asyncWrite\n#endif", true),
        ("implicit member", "let callback = .asyncWrite", true),
        ("interpolation", #"let text = "\(realm.asyncWrite)""#, true),
        ("declaration alias", "func asyncWrite() {}", true),
        ("owned writer", "func f() async throws { try await realm.asyncWritePreservingOwnership {} }", false),
        ("comments", "// realm.asyncWrite\n/* outer /* realm.asyncWrite */ nested */\nlet a = 1", false),
        ("string contents", #"let text = "realm.asyncWrite""#, false),
        ("raw string", ##"let text = #"realm.asyncWrite"#"##, false),
        ("multiline string", "let text = \"\"\"\nrealm.asyncWrite\n\"\"\"", false)
    ]
    for (name, source, expected) in cases {
        let found = try !forbiddenLocations(source, file: name).isEmpty
        guard found == expected else {
            throw BoundaryError.failure("Guard self-test failed: \(name)")
        }
    }
    var rejectedMalformedSource = false
    do { _ = try forbiddenLocations("func broken(", file: "malformed fixture") }
    catch { rejectedMalformedSource = true }
    guard rejectedMalformedSource else {
        throw BoundaryError.failure("Guard must reject malformed Swift rather than pass it")
    }
    print("Owned-write guard self-tests: \(cases.count + 1) passed")
}

func checkRepository(_ root: URL) throws {
    let sources = root.appendingPathComponent("Sources", isDirectory: true)
    let paths = try FileManager.default.subpathsOfDirectory(atPath: sources.path)
        .filter { $0.hasSuffix(".swift") }.sorted()
    guard !paths.isEmpty else {
        throw BoundaryError.failure("No Swift sources found at \(sources.path); refusing an empty pass")
    }
    var failures = [String]()
    for path in paths {
        let file = sources.appendingPathComponent(path)
        let text = try String(contentsOf: file, encoding: .utf8)
        failures += try forbiddenLocations(text, file: "Sources/\(path)")
    }
    guard failures.isEmpty else {
        throw BoundaryError.failure(
            failures.map { "\($0): raw asyncWrite is forbidden; use asyncWritePreservingOwnership" }
                .joined(separator: "\n")
        )
    }
    print("Owned-write source boundary: \(paths.count) Swift files passed")
    print("Source check only: not native Realm, CloudKit or composed-app qualification")
}

do {
    try selfTest()
    let arguments = CommandLine.arguments.dropFirst()
    if arguments.count == 1, arguments.first == "--self-test" {
        // Used by the launcher's standalone checker qualification.
    } else if arguments.count == 1, let root = arguments.first {
        try checkRepository(URL(fileURLWithPath: root, isDirectory: true))
    } else {
        throw BoundaryError.failure("Usage: check-owned-writes <repository-root> | --self-test")
    }
} catch {
    FileHandle.standardError.write(Data("\(error)\n".utf8))
    exit(1)
}
