/// Bridges callback-only CloudKit APIs with request-local cancellation and an
/// optional budget captured before synchronous registration. Registration itself
/// cannot be interrupted; late callbacks cannot renew the captured deadline.
internal func awaitCancellableCloudKitCallback<Value: Sendable>(
    timeoutNanoseconds: UInt64? = nil,
    now: @escaping @Sendable () -> UInt64 = { DispatchTime.now().uptimeNanoseconds },
    _ start: (@escaping (Result<Value, Error>) -> Void) -> Void
) async throws -> Value {
    try Task.checkCancellation()
    let deadline = timeoutNanoseconds.map { duration in
        let sum = now().addingReportingOverflow(duration)
        return sum.overflow ? UInt64.max : sum.partialValue
    }
    let deadlineError: @Sendable () -> CKError = {
        CKError(
            .networkFailure,
            userInfo: [
                NSLocalizedDescriptionKey:
                    "CloudKit callback exceeded its deadline"
            ]
        )
    }
    if let deadline, now() >= deadline { throw deadlineError() }
    let (stream, continuation) = AsyncThrowingStream<Value, Error>.makeStream()
    defer { continuation.finish() }
    try Task.checkCancellation()
    start { result in
        // Delivery, not timer scheduling, decides whether a callback is on time.
        if let deadline, now() >= deadline {
            continuation.finish(throwing: deadlineError())
            return
        }
        switch result {
        case .success(let value):
            continuation.yield(value)
            continuation.finish()
        case .failure(let error):
            continuation.finish(throwing: error)
        }
    }
    let timeoutTask = deadline.map { deadline in
        Task.detached {
            while !Task.isCancelled {
                let instant = now()
                guard instant < deadline else {
                    continuation.finish(throwing: deadlineError())
                    return
                }
                do { try await Task.sleep(nanoseconds: deadline - instant) }
                catch { return } // Timer cancellation is not deadline expiry.
            }
        }
    }
    defer { timeoutTask?.cancel() }
    do {
        try Task.checkCancellation()
        var iterator = stream.makeAsyncIterator()
        guard let value = try await iterator.next() else {
            throw CancellationError()
        }
        // Preserve an on-time accepted callback despite later scheduling, but
        // never deliver its buffered result to a caller that was cancelled.
        try Task.checkCancellation()
        return value
    } catch {
        try Task.checkCancellation()
        throw error
    }
}

