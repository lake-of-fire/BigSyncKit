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
        for (requestIdentifier, waiter) in waiters {
            if case .failure(let error) = result {
                failureHandlers[requestIdentifier]?(failureDiagnostic(
                    error: error, category: failureCategory,
                    requestIdentifier: requestIdentifier,
                    attemptIdentifier: failureAttemptIdentifier,
                    runIdentifier: failureRunIdentifier
                ))
            }
            waiter.resume(with: result)
        }
        lastSynchronizationProgressStage = nil
    }

