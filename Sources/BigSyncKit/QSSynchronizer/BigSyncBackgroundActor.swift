import CloudKit
import Realm
import RealmSwift
import Combine
import Logging

private enum BigSyncDeadlineOutcome: Sendable {
    case completed(CloudKitSynchronizer.SynchronizationResult?)
    case timedOut
}

private actor BigSyncDeadlineRace {
    private var outcome: BigSyncDeadlineOutcome?
    private var continuation:
        CheckedContinuation<BigSyncDeadlineOutcome, Never>?

    func resolve(_ outcome: BigSyncDeadlineOutcome) {
        guard self.outcome == nil else { return }
        self.outcome = outcome
        continuation?.resume(returning: outcome)
        continuation = nil
    }

    func value() async -> BigSyncDeadlineOutcome {
        if let outcome { return outcome }
        return await withCheckedContinuation { continuation in
            if let outcome {
                continuation.resume(returning: outcome)
            } else {
                self.continuation = continuation
            }
        }
    }
}

public struct BigSyncBackgroundWorkerConfiguration {
    public typealias SynchronizationCompletionHandler =
        @BigSyncBackgroundActor @Sendable (
            CloudKitSynchronizer.SynchronizationResult
        ) async -> Void
    public typealias SynchronizationWillConsumeServerChangesHandler =
        CloudKitSynchronizer
            .SynchronizationWillConsumeServerChangesHandler
    public typealias DomainPrepublicationHandler =
        CloudKitSynchronizer.DomainPrepublicationHandler
    public typealias DomainPublicationScopeIdentifierProvider =
        CloudKitSynchronizer.DomainPublicationScopeIdentifierProvider
    public typealias DurablePublicationEvidenceHandler =
        @BigSyncBackgroundActor @Sendable (
            BigSyncDurablePublicationEvidence?
        ) async throws -> Void
    public typealias AccountScopeInvalidationHandler =
        @BigSyncBackgroundActor @Sendable (
            BigSyncAccountScopeInvalidationReason
        ) async throws -> Void
    public typealias InitialReplicaBindingAdmissionHandler =
        @BigSyncBackgroundActor @Sendable (
            BigSyncInitialReplicaBindingContext
        ) async throws -> Void

    let synchronizerName: String
    let containerName: String
    let configurations: [Realm.Configuration]
    let excludedClassNames: [String]
    let accountScopePropertyByClassName: [String: String]
    let priorityClassNames: [String]
    let suiteName: String?
    let recordZoneID: CKRecordZone.ID?
    /// Optional stable identity for durable local state while the active
    /// CloudKit transport zone is replaced or migrated.
    let durableStateRecordZoneID: CKRecordZone.ID?
    let logger: Logging.Logger
    let localState: BigSyncLocalStateConfiguration?
    let performsAccountAvailabilityPreflight: Bool
    let accountIdentifierProvider: CloudKitSynchronizer.AccountIdentifierProvider?
    let progressHandler: CloudKitSynchronizer.ProgressHandler?
    let synchronizationCompletionHandler: SynchronizationCompletionHandler?
    let synchronizationWillConsumeServerChangesHandler:
        SynchronizationWillConsumeServerChangesHandler?
    let domainPrepublicationHandler: DomainPrepublicationHandler?
    let domainPublicationScopeIdentifierProvider:
        DomainPublicationScopeIdentifierProvider?
    let durablePublicationEvidenceHandler:
        DurablePublicationEvidenceHandler?
    let accountScopeInvalidationHandler: AccountScopeInvalidationHandler?
    let initialReplicaBindingAdmissionHandler:
        InitialReplicaBindingAdmissionHandler?
    let accountReplacementPolicy: BigSyncCloudAccountReplacementPolicy
    var changeFeedOverride: (any CloudKitChangeFeed)?
    var recordStoreOverride: (any CloudKitRecordStore)?
    /// Production synchronizers never delete CloudKit zones. Only an isolated
    /// per-run test client may opt into deleting its own disposable zone.
    private(set) var allowsDisposableZoneDeletion: Bool

    public init(
        synchronizerName: String,
        containerName: String,
        configurations: [Realm.Configuration],
        mutationPolicy: BigSyncMutationPolicy,
        priorityObjectTypes: [RealmSwift.Object.Type] = [],
        suiteName: String? = nil,
        recordZoneID: CKRecordZone.ID? = nil,
        durableStateRecordZoneID: CKRecordZone.ID? = nil,
        localState: BigSyncLocalStateConfiguration? = nil,
        performsAccountAvailabilityPreflight: Bool = true,
        accountIdentifierProvider: CloudKitSynchronizer.AccountIdentifierProvider? = nil,
        progressHandler: CloudKitSynchronizer.ProgressHandler? = nil,
        synchronizationWillConsumeServerChangesHandler:
            SynchronizationWillConsumeServerChangesHandler? = nil,
        domainPrepublicationHandler: DomainPrepublicationHandler? = nil,
        domainPublicationScopeIdentifierProvider:
            DomainPublicationScopeIdentifierProvider? = nil,
        durablePublicationEvidenceHandler:
            DurablePublicationEvidenceHandler? = nil,
        synchronizationCompletionHandler: SynchronizationCompletionHandler? = nil,
        accountScopeInvalidationHandler: AccountScopeInvalidationHandler? = nil,
        initialReplicaBindingAdmissionHandler:
            InitialReplicaBindingAdmissionHandler? = nil,
        accountReplacementPolicy: BigSyncCloudAccountReplacementPolicy =
            .serverReconciliation,
        logger: Logging.Logger
    ) {
        let zoneID = recordZoneID ?? CloudKitSynchronizer.defaultCustomZoneID
        let durableStateZoneID = durableStateRecordZoneID ?? zoneID
        let sharedStateBaseURL: URL
        if let localState {
            sharedStateBaseURL = localState.trackingRealmDirectoryURL
                .deletingLastPathComponent()
                .appendingPathComponent(
                    "BigSyncKitBackupDetection",
                    isDirectory: true
                )
        } else if let suiteName {
            guard let groupContainerURL = FileManager.default
                .containerURL(
                    forSecurityApplicationGroupIdentifier: suiteName
                ) else {
                preconditionFailure(
                    "BigSyncKit cannot resolve the configured App Group \(suiteName)"
                )
            }
            sharedStateBaseURL = groupContainerURL
                .appendingPathComponent("Library", isDirectory: true)
                .appendingPathComponent(
                    "Application Support",
                    isDirectory: true
                )
                .appendingPathComponent("BigSyncKit", isDirectory: true)
        } else {
            sharedStateBaseURL = FileManager.default.urls(
                for: .applicationSupportDirectory,
                in: .userDomainMask
            )[0].appendingPathComponent("BigSyncKit", isDirectory: true)
        }
        let clientIdentity = BigSyncClientIdentity(
            synchronizerName: synchronizerName,
            containerName: containerName,
            recordZoneID: durableStateZoneID,
            databaseScope: .private,
            sharedStateBaseURL: sharedStateBaseURL
        )
        let installationIdentifier: String
        do {
            installationIdentifier = try clientIdentity.prepareInstallation()
        } catch {
            preconditionFailure(
                "BigSyncKit installation identity failed before worker configuration: \(error)"
            )
        }
        let mutationJournalIdentityProvider:
            (@Sendable () -> BigSyncMutationJournalIdentity?)?
        if accountReplacementPolicy.usesDatasetReplicaBinding {
            let replicaBindingStore: any KeyValueStore =
                localState?.keyValueStore
                ?? FileKeyValueStore(
                    fileURL: clientIdentity.synchronizationStateFileURL
                )
            let replicaBindingKey = clientIdentity.durableStateNamespace
                + ".ReplicaBinding.v1"
            do {
                if let durableStore = replicaBindingStore
                    as? any DurableKeyValueStore {
                    try durableStore.prepareForUse()
                } else {
                    try replicaBindingStore.bigSyncValidateDurability()
                }
                _ = try BigSyncReplicaBindingStateStore.prepare(
                    store: replicaBindingStore,
                    key: replicaBindingKey,
                    installationIdentifier: installationIdentifier
                )
            } catch {
                preconditionFailure(
                    "BigSyncKit replica binding failed before worker configuration: \(error)"
                )
            }
            let mutationIdentityReader =
                BigSyncMutationJournalIdentityReader(
                    clientIdentity: clientIdentity,
                    store: replicaBindingStore,
                    key: replicaBindingKey
                )
            mutationJournalIdentityProvider = {
                mutationIdentityReader.current()
            }
        } else {
            mutationJournalIdentityProvider = nil
        }
        if let mutationJournalIdentityProvider {
            mutationPolicy.install(
                configurations: configurations,
                mutationJournalIdentityProvider:
                    mutationJournalIdentityProvider
            )
        } else {
            mutationPolicy.install(
                configurations: configurations,
                installationIdentifierProvider: {
                    clientIdentity.currentInstallationIdentifier()
                }
            )
        }
        self.synchronizerName = synchronizerName
        self.containerName = containerName
        self.configurations = configurations
        self.excludedClassNames = mutationPolicy.excludedClassNames
        self.accountScopePropertyByClassName =
            mutationPolicy.accountScopePropertyByClassName
        self.priorityClassNames = priorityObjectTypes.map { $0.className() }
        self.suiteName = suiteName
        self.recordZoneID = recordZoneID
        self.durableStateRecordZoneID = durableStateRecordZoneID
        self.localState = localState
        self.performsAccountAvailabilityPreflight =
            performsAccountAvailabilityPreflight
        self.accountIdentifierProvider = accountIdentifierProvider
        self.progressHandler = progressHandler
        self.synchronizationWillConsumeServerChangesHandler =
            synchronizationWillConsumeServerChangesHandler
        self.domainPrepublicationHandler = domainPrepublicationHandler
        self.domainPublicationScopeIdentifierProvider =
            domainPublicationScopeIdentifierProvider
        self.durablePublicationEvidenceHandler =
            durablePublicationEvidenceHandler
        self.synchronizationCompletionHandler = synchronizationCompletionHandler
        self.accountScopeInvalidationHandler = accountScopeInvalidationHandler
        self.initialReplicaBindingAdmissionHandler =
            initialReplicaBindingAdmissionHandler
        self.accountReplacementPolicy = accountReplacementPolicy
        self.changeFeedOverride = nil
        self.recordStoreOverride = nil
        self.allowsDisposableZoneDeletion = false
        self.logger = logger
    }

#if DEBUG
    /// Installs an observable transport wrapper for the isolated E2E client.
    /// Production configurations always use the default CloudKit transport.
    @_spi(CloudKitE2E)
    public mutating func installCloudKitE2ETransport(
        changeFeed: any CloudKitChangeFeed,
        recordStore: any CloudKitRecordStore
    ) {
        changeFeedOverride = changeFeed
        recordStoreOverride = recordStore
    }

    /// Grants the destructive zone-deletion capability only to Manabi's
    /// isolated, per-run CloudKit E2E client. This is SPI rather than a public
    /// boolean so an ordinary debug synchronizer cannot opt an arbitrary app
    /// zone into deletion.
    @_spi(CloudKitE2E)
    @discardableResult
    public mutating func authorizeDisposableZoneDeletionForE2E(runID: UUID) -> Bool {
        let canonicalRunID = runID.uuidString.lowercased()
        let expectedZoneName = "ManabiPlatform.e2e.v2.\(canonicalRunID)"
        guard let recordZoneID,
              recordZoneID.ownerName == CKCurrentUserDefaultName,
              recordZoneID.zoneName == expectedZoneName,
              synchronizerName == "\(expectedZoneName).cleanup",
              let localState else {
            return false
        }

        let trackingParent = localState.trackingRealmDirectoryURL
            .standardizedFileURL
            .deletingLastPathComponent()
        let assetParent = localState.assetDirectoryURL
            .standardizedFileURL
            .deletingLastPathComponent()
        let phase = trackingParent.lastPathComponent
        let runRoot = trackingParent.deletingLastPathComponent()
        guard trackingParent == assetParent,
              phase == "cleanup",
              runRoot.lastPathComponent == canonicalRunID,
              runRoot.deletingLastPathComponent().lastPathComponent == "CloudKitE2E" else {
            return false
        }
        allowsDisposableZoneDeletion = true
        return true
    }
#endif

}

@globalActor
public actor BigSyncBackgroundActor {
    public static let shared = BigSyncBackgroundActor()
    private static let initialSynchronizationDelayNanoseconds: UInt64 = 10_000_000_000

    private weak var synchronizerDelegate: RealmSwiftAdapterDelegate?
    private let accountAvailabilityGate: CloudKitAccountAvailabilityGate
    @BigSyncBackgroundActor
    private var initialSynchronizationTask: Task<Void, Never>?
    @BigSyncBackgroundActor
    private var accountAvailabilityRetryTask: Task<Void, Never>?
    @BigSyncBackgroundActor
    private var publicationRestorationTask: Task<Void, Never>?
    @BigSyncBackgroundActor
    private var performsAccountAvailabilityPreflight = true
    @BigSyncBackgroundActor
    private var synchronizationCompletionHandler:
        BigSyncBackgroundWorkerConfiguration.SynchronizationCompletionHandler?

    @BigSyncBackgroundActor
    public private(set) var realmSynchronizer: CloudKitSynchronizer?
    @BigSyncBackgroundActor
    public private(set) var logger: Logging.Logger?

    public init() {
        accountAvailabilityGate = CloudKitAccountAvailabilityGate()
    }

    init(accountAvailabilityGate: CloudKitAccountAvailabilityGate) {
        self.accountAvailabilityGate = accountAvailabilityGate
    }

    @BigSyncBackgroundActor
    public func configure(_ configuration: BigSyncBackgroundWorkerConfiguration) {
        precondition(
            realmSynchronizer == nil,
            "BigSyncKit worker configuration is one-shot; await shutdown before introducing runtime replacement"
        )
        logger = configuration.logger

        let accountStatusProvider:
            CloudKitSynchronizer.AccountStatusProvider? =
                configuration.performsAccountAvailabilityPreflight
                    ? nil
                    : { @Sendable in .available }
        let synchronizer = CloudKitSynchronizer.privateSynchronizer(
            synchronizerName: configuration.synchronizerName,
            containerName: configuration.containerName,
            configurations: configuration.configurations,
            excludedClassNames: configuration.excludedClassNames,
            accountScopePropertyByClassName:
                configuration.accountScopePropertyByClassName,
            priorityClassNames: configuration.priorityClassNames,
            committedInboundIdentityDeliveryEnabled:
                configuration.domainPrepublicationHandler != nil,
            suiteName: configuration.suiteName,
            recordZoneID: configuration.recordZoneID,
            durableStateRecordZoneID:
                configuration.durableStateRecordZoneID,
            localState: configuration.localState,
            accountIdentifierProvider: configuration.accountIdentifierProvider,
            accountStatusProvider: accountStatusProvider,
            progressHandler: configuration.progressHandler,
            changeFeed: configuration.changeFeedOverride,
            recordStore: configuration.recordStoreOverride,
            allowsDisposableZoneDeletion: configuration.allowsDisposableZoneDeletion,
            initialReplicaBindingAdmissionHandler:
                configuration.initialReplicaBindingAdmissionHandler,
            accountReplacementPolicy: configuration.accountReplacementPolicy,
            compatibilityVersion: Int(configuration.configurations.map { $0.schemaVersion } .reduce(0, +)),
            logger: configuration.logger
        )

        realmSynchronizer = synchronizer
        synchronizer.synchronizationWillConsumeServerChangesHandler =
            configuration.synchronizationWillConsumeServerChangesHandler
        synchronizer.domainPrepublicationHandler =
            configuration.domainPrepublicationHandler
        synchronizer.domainPublicationScopeIdentifierProvider =
            configuration.domainPublicationScopeIdentifierProvider
        synchronizer.accountScopeInvalidationHandler =
            configuration.accountScopeInvalidationHandler
        performsAccountAvailabilityPreflight =
            configuration.performsAccountAvailabilityPreflight
        synchronizationCompletionHandler =
            configuration.synchronizationCompletionHandler

        if let restorationHandler =
            configuration.durablePublicationEvidenceHandler {
            publicationRestorationTask = Task(
                priority: .utility
            ) { @BigSyncBackgroundActor in
                do {
                    try await restorationHandler(
                        try await synchronizer
                            .restoredDurablePublicationEvidence()
                    )
                } catch {
                    configuration.logger.error(
                        "QSCloudKitSynchronizer >> Could not restore terminal publication evidence: \(error)"
                    )
                }
            }
        } else {
            publicationRestorationTask = nil
        }

        (synchronizer.modelAdapters.first as? RealmSwiftAdapter)?.mergePolicy = .custom

        let compatibilityVersion = synchronizer.compatibilityVersion
        configuration.logger.info("QSCloudKitSynchronizer >> Local compatibility version: \(compatibilityVersion)")

        initialSynchronizationTask = Task(priority: .utility) { @BigSyncBackgroundActor [weak self] in
            guard let self else { return }
            do {
                try await Task.sleep(nanoseconds: Self.initialSynchronizationDelayNanoseconds)
            } catch {
                return
            }
            _ = await self.synchronizeCloudKit(expectedSynchronizer: synchronizer)
        }
    }

    @BigSyncBackgroundActor
    public func hasInboundSemanticQuarantine(
        entityType: String,
        accountScopeIdentifier: String,
        semanticScopeIdentifier: String? = nil
    ) throws -> Bool {
        guard let adapter = realmSynchronizer?.modelAdapters.first
                as? RealmSwiftAdapter else {
            return false
        }
        return try adapter.hasInboundSemanticQuarantine(
            entityType: entityType,
            accountScopeIdentifier: accountScopeIdentifier,
            semanticScopeIdentifier: semanticScopeIdentifier
        )
    }

    @BigSyncBackgroundActor
    public func serverRecordEvidence(
        recordName: String,
        expectedEntityType: String
    ) throws -> BigSyncServerRecordEvidence? {
        guard let adapter = realmSynchronizer?.modelAdapters.first
                as? RealmSwiftAdapter else {
            return nil
        }
        return try adapter.serverRecordEvidence(
            recordName: recordName,
            expectedEntityType: expectedEntityType
        )
    }

    /// Current-account membership catalog at the latest consumed server
    /// boundary. Target-Realm presence is deliberately not consulted.
    @BigSyncBackgroundActor
    public func serverRecordEvidence(
        entityTypes: Set<String>
    ) throws -> [BigSyncServerRecordEvidence] {
        guard let adapter = realmSynchronizer?.modelAdapters.first
                as? RealmSwiftAdapter else {
            throw RealmSwiftAdapterError.setupUnavailable
        }
        return try adapter.serverRecordEvidence(entityTypes: entityTypes)
    }

    @BigSyncBackgroundActor
    public func currentServerBoundaryIdentifier() throws -> String? {
        guard let synchronizer = realmSynchronizer,
              let adapter = synchronizer.modelAdapters.first
                as? RealmSwiftAdapter else {
            return nil
        }
        return try adapter.currentServerBoundaryIdentifier(
            containerIdentifier: synchronizer.containerIdentifier,
            databaseScope: synchronizer.database.databaseScope
        )
    }

    /// Read-only inventory used by account-fenced model cutovers. The
    /// inventory never clears or acknowledges the sole mutation journal.
    @BigSyncBackgroundActor
    public func pendingMutationInventory(
        entityTypes: Set<String>
    ) throws -> [BigSyncPendingMutationInventoryItem] {
        guard let adapter = realmSynchronizer?.modelAdapters.first
                as? RealmSwiftAdapter else {
            throw RealmSwiftAdapterError.setupUnavailable
        }
        return try adapter.pendingMutationInventory(
            entityTypes: entityTypes
        )
    }

    @BigSyncBackgroundActor
    @discardableResult
    public func synchronizeCloudKit()
        async -> CloudKitSynchronizer.SynchronizationResult? {
        // An explicit request supersedes the delayed startup request. Leaving
        // both alive performs a second full drain ten seconds after every
        // configuration, or queues it behind a long initial reupload.
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = nil
        accountAvailabilityRetryTask?.cancel()
        accountAvailabilityRetryTask = nil

        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Synchronization requested before background synchronizer configuration completed")
            return nil
        }

        return await synchronizeCloudKit(expectedSynchronizer: realmSynchronizer)
    }

    /// Returns at the deadline even when an underlying CloudKit await does not
    /// cooperate with Swift task cancellation. The losing request task is
    /// canceled and fenced; an already-running shared synchronization may still
    /// finish for other waiters, but it cannot hold a lifecycle completion
    /// handler hostage.
    @BigSyncBackgroundActor
    @discardableResult
    public func synchronizeCloudKit(
        deadlineNanoseconds: UInt64
    ) async -> CloudKitSynchronizer.SynchronizationResult? {
        let race = BigSyncDeadlineRace()
        let synchronizationTask = Task { @BigSyncBackgroundActor [weak self] in
            let result = await self?.synchronizeCloudKit()
            await race.resolve(.completed(result))
        }
        let deadlineTask = Task.detached { [race, deadlineNanoseconds] in
            do {
                try await Task.sleep(nanoseconds: deadlineNanoseconds)
            } catch {
                return
            }
            await race.resolve(.timedOut)
        }

        let outcome = await withTaskCancellationHandler {
            await race.value()
        } onCancel: {
            synchronizationTask.cancel()
            Task { await race.resolve(.timedOut) }
        }
        synchronizationTask.cancel()
        deadlineTask.cancel()
        switch outcome {
        case .completed(let result):
            return result
        case .timedOut:
            logger?.warning(
                "QSCloudKitSynchronizer >> Lifecycle synchronization deadline elapsed"
            )
            return nil
        }
    }

    @BigSyncBackgroundActor
    private func synchronizeCloudKit(
        expectedSynchronizer: CloudKitSynchronizer
    ) async -> CloudKitSynchronizer.SynchronizationResult? {
        guard realmSynchronizer === expectedSynchronizer else {
            return nil
        }
        await publicationRestorationTask?.value
        publicationRestorationTask = nil
        let containerIdentifier = expectedSynchronizer.containerIdentifier

        // Explicit lifecycle requests share the same cheap durable gate used
        // by ordinary journal wakeups. A real CKAccountChanged notification
        // enters the synchronizer directly and can still refresh the pending
        // destination account before stopping again.
        guard !expectedSynchronizer
            .reportPendingCloudAccountPortIfNeeded() else {
            return nil
        }

        if performsAccountAvailabilityPreflight {
            switch await accountAvailabilityGate.availability(
                for: containerIdentifier
            ) {
            case .available:
                accountAvailabilityRetryTask?.cancel()
                accountAvailabilityRetryTask = nil
                expectedSynchronizer.accountValidationRequired = false
                expectedSynchronizer.cancelledDueToUnauthentication = false
            case .unavailable(let status):
                logger?.info(
                    "QSCloudKitSynchronizer >> Synchronization deferred because iCloud account status is \(status.rawValue)"
                )
                // Apple requires accountTemporarilyUnavailable to remain
                // quiescent until CKAccountChanged. The synchronizer observes
                // that notification and revalidates status before doing any
                // database work. `couldNotDetermine` is different: it may be
                // a transient status-query/network failure with no account
                // transition notification, so retain its bounded poll.
                if status == .couldNotDetermine {
                    scheduleAccountAvailabilityRetry(
                        expectedSynchronizer: expectedSynchronizer
                    )
                }
                return nil
            case .failed:
                logger?.warning("QSCloudKitSynchronizer >> Synchronization deferred because iCloud account status failed")
                scheduleAccountAvailabilityRetry(
                    expectedSynchronizer: expectedSynchronizer
                )
                return nil
            }
        } else {
            // Callers may already have proved private-database access with a
            // successful CloudKit operation. Avoid repeating the callback-only
            // account-status preflight in that case; synchronize() still
            // fetches and fences the exact account identifier before any work.
            expectedSynchronizer.cancelledDueToUnauthentication = false
            expectedSynchronizer.accountValidationRequired = false
        }

        guard !Task.isCancelled,
              realmSynchronizer === expectedSynchronizer else { return nil }
        do {
            let result = try await expectedSynchronizer.synchronize()
            guard !Task.isCancelled,
                  realmSynchronizer === expectedSynchronizer else {
                return nil
            }
            await synchronizationCompletionHandler?(result)
            return result
        } catch is CancellationError {
            return nil
        } catch {
            logger?.error(
                "QSCloudKitSynchronizer >> Synchronization failed: \(error)"
            )
            return nil
        }
    }

    @BigSyncBackgroundActor
    private func scheduleAccountAvailabilityRetry(
        expectedSynchronizer: CloudKitSynchronizer
    ) {
        guard realmSynchronizer === expectedSynchronizer else { return }
        accountAvailabilityRetryTask?.cancel()
        accountAvailabilityRetryTask = Task(priority: .utility) {
            @BigSyncBackgroundActor [weak self, weak expectedSynchronizer] in
            guard let self, let expectedSynchronizer else { return }
            do {
                try await Task.sleep(nanoseconds: 30_000_000_000)
            } catch {
                return
            }
            guard realmSynchronizer === expectedSynchronizer else { return }
            accountAvailabilityRetryTask = nil
            _ = await synchronizeCloudKit(
                expectedSynchronizer: expectedSynchronizer
            )
        }
    }

    @BigSyncBackgroundActor
    public func cancelSynchronization() async {
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = nil
        accountAvailabilityRetryTask?.cancel()
        accountAvailabilityRetryTask = nil
        guard let realmSynchronizer else {
            logger?.warning("QSCloudKitSynchronizer >> Cancellation requested before background synchronizer configuration completed")
            return
        }

        await realmSynchronizer.cancelSynchronizationAndWait()
    }

    /// Restores the current account's redacted, durable CloudKit health state.
    /// A synchronizer that has not yet been configured, or a snapshot belonging
    /// to another iCloud account, intentionally yields `nil`.
    @BigSyncBackgroundActor
    public func cloudKitSyncHealthSnapshot()
        async -> CloudKitSyncHealthSnapshot? {
        guard let realmSynchronizer else { return nil }
        return try? await realmSynchronizer.syncHealthSnapshot()
    }

    /// Returns the durable account authority used by account-scoped domain
    /// writers. The lease remains usable while offline until an account,
    /// restore, or installation boundary durably invalidates its generation.
    @BigSyncBackgroundActor
    public func accountScopeLease() throws -> BigSyncAccountScopeLease? {
        guard let realmSynchronizer else { return nil }
        return try realmSynchronizer.accountScopeLease()
    }

    @BigSyncBackgroundActor
    public func pendingCloudAccountPortRequirement() throws
        -> BigSyncCloudAccountPortRequirement? {
        try realmSynchronizer?.pendingCloudAccountPortRequirement()
    }

    @BigSyncBackgroundActor
    public func activateCloudAccountPort(
        _ expected: BigSyncCloudAccountPortRequirement
    ) async throws {
        guard let realmSynchronizer else {
            throw BigSyncCloudAccountPortError.corruptRequirement
        }
        try await realmSynchronizer.activateCloudAccountPort(expected)
    }

    /// Revalidates a lease captured before suspension against the currently
    /// configured synchronizer.
    @BigSyncBackgroundActor
    public func validateAccountScopeLease(
        _ expected: BigSyncAccountScopeLease
    ) throws {
        guard let realmSynchronizer else {
            throw BigSyncAccountScopeLeaseError.unavailable
        }
        try realmSynchronizer.validateAccountScopeLease(expected)
    }

    /// Revalidates an application pre-inbound callback after suspension using
    /// the currently configured synchronizer rather than a separately
    /// reconstructed production identity. This also works for isolated E2E
    /// synchronizers with their own durable local state.
    @BigSyncBackgroundActor
    public func validateSynchronizationBoundaryContext(
        _ context: CloudKitSynchronizer.SynchronizationBoundaryContext
    ) throws {
        guard let realmSynchronizer else {
            throw CancellationError()
        }
        try realmSynchronizer.validateBoundaryContext(context)
    }

    /// Revalidates an application terminal-prepublication callback after
    /// suspension against the exact current synchronization run.
    @BigSyncBackgroundActor
    public func validatePrepublicationBoundaryContext(
        _ context: CloudKitSynchronizer.PrepublicationBoundaryContext
    ) throws {
        guard let realmSynchronizer else {
            throw CancellationError()
        }
        try realmSynchronizer.validateBoundaryContext(context)
    }

    /// Revalidates the active run, durable binding, and exact CloudKit account
    /// across the account-provider suspension.
    @BigSyncBackgroundActor
    public func revalidateSynchronizationBoundaryContext(
        _ context: CloudKitSynchronizer.SynchronizationBoundaryContext
    ) async throws {
        guard let realmSynchronizer else {
            throw CancellationError()
        }
        try await realmSynchronizer.revalidateBoundaryContext(context)
    }

    /// Exact-account revalidation for terminal-prepublication application
    /// mutations.
    @BigSyncBackgroundActor
    public func revalidatePrepublicationBoundaryContext(
        _ context: CloudKitSynchronizer.PrepublicationBoundaryContext
    ) async throws {
        guard let realmSynchronizer else {
            throw CancellationError()
        }
        try await realmSynchronizer.revalidateBoundaryContext(context)
    }

    /// Exact-account revalidation for local writes performed during initial
    /// replica admission.
    @BigSyncBackgroundActor
    public func revalidateInitialReplicaBindingContext(
        _ context: BigSyncInitialReplicaBindingContext
    ) async throws {
        guard let realmSynchronizer else {
            throw CancellationError()
        }
        try await realmSynchronizer
            .revalidateInitialReplicaBindingContext(context)
    }

#if DEBUG
    @BigSyncBackgroundActor
    var _test_hasScheduledInitialSynchronization: Bool {
        initialSynchronizationTask != nil
    }

    @BigSyncBackgroundActor
    var _test_hasScheduledAccountAvailabilityRetry: Bool {
        accountAvailabilityRetryTask != nil
    }

    @BigSyncBackgroundActor
    func _test_installSynchronizer(
        _ synchronizer: CloudKitSynchronizer,
        performsAccountAvailabilityPreflight: Bool = true,
        synchronizationCompletionHandler:
            BigSyncBackgroundWorkerConfiguration
                .SynchronizationCompletionHandler? = nil
    ) {
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = nil
        realmSynchronizer = synchronizer
        self.performsAccountAvailabilityPreflight =
            performsAccountAvailabilityPreflight
        self.synchronizationCompletionHandler =
            synchronizationCompletionHandler
    }

    @BigSyncBackgroundActor
    func _test_scheduleDormantInitialSynchronization() {
        initialSynchronizationTask?.cancel()
        initialSynchronizationTask = Task {
            try? await Task.sleep(nanoseconds: 60_000_000_000)
        }
    }
#endif
}
