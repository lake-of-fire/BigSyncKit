    private func pendingMutationSnapshots(
        for recordNames: some Sequence<String>,
        in realm: Realm
    ) -> [BigSyncPendingMutationSnapshot] {
        if !realm.isInWriteTransaction { realm.refresh() }
        let snapshot = realm.freeze()
        return recordNames.compactMap { recordName in
            guard let mutation = snapshot.object(
                ofType: BigSyncPendingMutation.self,
                forPrimaryKey: recordName
            ) else { return nil }
            return pendingMutationSnapshot(mutation, in: snapshot)
        }
    }

    private func pendingMutationSnapshot(
        _ mutation: BigSyncPendingMutation,
        in realm: Realm
    ) -> BigSyncPendingMutationSnapshot {
        BigSyncPendingMutationSnapshot(
            recordName: mutation.recordName,
            entityType: mutation.entityType,
            objectIdentifier: mutation.objectIdentifier,
            accountScopeIdentifier: mutation.accountScopeIdentifier,
            replicaBindingGenerationIdentifier:
                mutation.replicaBindingGenerationIdentifier,
            generation: mutation.generation,
            changedAt: mutation.changedAt,
            isDeletion: pendingMutationTargetsDeletedObject(mutation, in: realm)
        )
    }
    @BigSyncBackgroundActor
    @discardableResult
    private func forwardPendingMutations(
        in targetReaderRealm: Realm,
        notifyDelegate: Bool = true,
        progress: (@BigSyncBackgroundActor @Sendable (String) -> Void)? = nil
    ) async throws -> Int {
        // Freeze the journal boundary so paging does not change which generations
        // this drain promises to forward, while avoiding one O(N) snapshot array.
        progress?("adapter-import-journal-snapshot-started")
        if !targetReaderRealm.isInWriteTransaction { targetReaderRealm.refresh() }
        let snapshot = targetReaderRealm.freeze()
        let mutations = snapshot.objects(BigSyncPendingMutation.self)
            .sorted(byKeyPath: "recordName")
        let mutationCount = mutations.count
        progress?("adapter-import-journal-snapshot-completed")
        let pageSize = 1_000
        var forwardedCount = 0
        var offset = 0
        while offset < mutationCount {
            try Task.checkCancellation()
            guard !cancelSync else { throw CancellationError() }
            let end = min(offset + pageSize, mutationCount)
            var pending = [BigSyncPendingMutationSnapshot]()
            pending.reserveCapacity(end - offset)
            for index in offset..<end {
                pending.append(
                    pendingMutationSnapshot(
                        mutations[index],
                        in: snapshot
                    )
                )
            }
            progress?("adapter-import-journal-page-tracking-started")
            forwardedCount += try await forwardPendingMutations(
                pending,
                in: targetReaderRealm,
                notifyDelegate: false,
                updateStatus: false
            )
                // A frozen/page snapshot can become stale while this task is
                // waiting for the tracking transaction. Re-resolve each
                // identity only after that transaction is acquired, keeping
                // committed target read and tracking publication in one
                // non-suspending boundary. Owning the tracking write does not
                // authorize reading another owner's provisional target write.
                // Journal fields and deletion disposition share this snapshot.
#if DEBUG
                traceJournalForwarding("tracking-target-refresh-started operation=\(traceOperation)")
#endif
                if !targetReaderRealm.isInWriteTransaction { targetReaderRealm.refresh() }
#if DEBUG
                traceJournalForwarding("tracking-target-refresh-completed operation=\(traceOperation)")
#endif
                let snapshot = targetReaderRealm.freeze()
                let currentMutations: [BigSyncPendingMutationSnapshot] =
                    chunk.compactMap { mutation in
                        guard let current = snapshot.object(
                            ofType: BigSyncPendingMutation.self,
                            forPrimaryKey: mutation.recordName
                        ) else { return nil }
                        return pendingMutationSnapshot(
                            current,
                            in: snapshot
                        )
                    }
#if DEBUG
                traceJournalForwarding(
                    "tracking-live-snapshot operation=\(traceOperation) count=\(currentMutations.count)"
                )
#endif
                for mutation in currentMutations {
                    try Task.checkCancellation()
                    guard !cancelSync else { throw CancellationError() }
                    // The account can change while this task is suspended
                    // waiting for the persistence transaction. Recheck the
                    // committed journal snapshot at the final publication boundary
                    // so old-account work is never copied into the new run's
                    // tracking Realm.
                    guard pendingMutationIsEligibleForActiveTransport(
                        mutation
                    ) else {
                        continue
                    }
