import Foundation

/// Exact binding history carried by the existing account-scoped recovery
/// envelope. This is transport intent, not a second outbox. A return to an
/// account may combine several interrupted handoffs, but never adopts an
/// unrelated generation merely because it is not current. An empty retiring
/// set is verification-only: it can admit destination work, never adopt old work.
public struct BigSyncReplicaJournalHandoff: Sendable, Equatable {
    public let installationIdentifier: String
    public let retiringBindingGenerationIdentifiers: [String]
    public let destinationBindingGenerationIdentifier: String

    public init(
        installationIdentifier: String,
        retiringBindingGenerationIdentifiers: [String],
        destinationBindingGenerationIdentifier: String
    ) throws {
        let retiring = Set(retiringBindingGenerationIdentifiers)
        guard !installationIdentifier.isEmpty,
              !destinationBindingGenerationIdentifier.isEmpty,
              retiring.allSatisfy({ !$0.isEmpty }),
              !retiring.contains(destinationBindingGenerationIdentifier) else {
            throw BigSyncReplicaJournalHandoffError.invalidIdentity
        }
        self.installationIdentifier = installationIdentifier
        self.retiringBindingGenerationIdentifiers = retiring.sorted()
        self.destinationBindingGenerationIdentifier =
            destinationBindingGenerationIdentifier
    }

    /// The coordinator must establish one zone and a connected binding path
    /// (or a previous unfinished envelope for the destination account). The
    /// incoming handoff selects the destination; recorded predecessors remain
    /// recoverable until this envelope completes.
    func retainingUnfinished(
        _ previous: Self
    ) throws -> Self {
        guard installationIdentifier == previous.installationIdentifier else {
            throw BigSyncReplicaJournalHandoffError.invalidIdentity
        }
        var retiring = Set(retiringBindingGenerationIdentifiers)
        retiring.formUnion(previous.retiringBindingGenerationIdentifiers)
        retiring.insert(previous.destinationBindingGenerationIdentifier)
        retiring.remove(destinationBindingGenerationIdentifier)
        return try Self(
            installationIdentifier: installationIdentifier,
            retiringBindingGenerationIdentifiers: Array(retiring),
            destinationBindingGenerationIdentifier:
                destinationBindingGenerationIdentifier
        )
    }

    enum Action: Equatable {
        case preserveDestination
        case rebindRetiring
    }

    func action(
        for bindingGenerationIdentifier: String?,
        verifyOnly: Bool
    ) throws -> Action {
        if bindingGenerationIdentifier == destinationBindingGenerationIdentifier {
            return .preserveDestination
        }
        guard let bindingGenerationIdentifier,
              retiringBindingGenerationIdentifiers.contains(
                bindingGenerationIdentifier
              ) else {
            throw BigSyncReplicaJournalHandoffError.unexpectedBinding
        }
        guard !verifyOnly else {
            throw BigSyncReplicaJournalHandoffError.retiringMutationRemaining
        }
        return .rebindRetiring
    }

    var propertyList: [String: Any] {
        [
            "installationIdentifier": installationIdentifier,
            "retiringBindingGenerationIdentifiers":
                retiringBindingGenerationIdentifiers,
            "destinationBindingGenerationIdentifier":
                destinationBindingGenerationIdentifier,
        ]
    }

    init(propertyList: [String: Any]) throws {
        guard let installation = propertyList["installationIdentifier"] as? String,
              let retiring = propertyList["retiringBindingGenerationIdentifiers"]
                as? [String],
              let destination = propertyList["destinationBindingGenerationIdentifier"]
                as? String else {
            throw BigSyncReplicaJournalHandoffError.invalidIdentity
        }
        try self.init(
            installationIdentifier: installation,
            retiringBindingGenerationIdentifiers: retiring,
            destinationBindingGenerationIdentifier: destination
        )
    }
}

public enum BigSyncReplicaJournalHandoffError: Error, Sendable, Equatable {
    case invalidIdentity
    case unexpectedBinding
    case retiringMutationRemaining
    case unsupportedAdapter
    case invalidRecord(String)
}
