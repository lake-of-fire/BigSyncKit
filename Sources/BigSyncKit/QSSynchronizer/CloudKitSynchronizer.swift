    internal func finishSynchronizationDrain(
        with result: Result<SynchronizationResult, Error>,
        failureCategory: BigSyncSynchronizationFailure.Category = .failed,
        failureAttemptIdentifier: UUID? = nil,
        failureRunIdentifier: UUID? = nil
    ) {
        synchronizationDrainIsActive = false
        synchronizationDrainMode = nil
        synchronizationRequestedWhileRunning = false
        let waiters = synchronizationWaiters
        let failureHandlers = synchronizationFailureHandlers
        synchronizationWaiters.removeAll(keepingCapacity: false)
        synchronizationFailureHandlers.removeAll(keepingCapacity: false)
        // Snapshot every observer's evidence before the first callout. A
        // synchronous observer can reenter cancellation/settlement or start a
        // successor; later waiters must not inherit its origin or task state.
        let attemptIdentifier = failureAttemptIdentifier ?? synchronizationAttemptID
        let runIdentifier = failureRunIdentifier ?? activeRunContext?.runID
        let progressStage = lastSynchronizationProgressStage
        lastSynchronizationProgressStage = nil
        var failures = [UUID: BigSyncSynchronizationFailure]()
        if case .failure(let error) = result {
            for requestIdentifier in waiters.keys where failureHandlers[requestIdentifier] != nil {
                failures[requestIdentifier] = BigSyncSynchronizationFailure(
                    category: failureCategory, requestIdentifier: requestIdentifier,
                    attemptIdentifier: attemptIdentifier, runIdentifier: runIdentifier,
                    lastProgressStage: progressStage, error: error
                )
            }
        }
        for (requestIdentifier, waiter) in waiters {
            if let failure = failures[requestIdentifier] {
                failureHandlers[requestIdentifier]?(failure)
            }
            waiter.resume(with: result)
        }
    }

