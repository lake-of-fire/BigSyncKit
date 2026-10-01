/// Bridges callback-only CloudKit APIs without pinning the caller to a checked
/// continuation after its task is cancelled. The callback may still arrive,
/// but `AsyncThrowingStream` safely discards it after termination.
internal func awaitCancellableCloudKitCallback<Value>(
    timeoutNanoseconds: UInt64? = nil,
    _ start: (@escaping (Result<Value, Error>) -> Void) -> Void
) async throws -> Value {
    let (stream, continuation) = AsyncThrowingStream<Value, Error>.makeStream()
    start { result in
        switch result {
        case .success(let value):
            continuation.yield(value)
            continuation.finish()
        case .failure(let error):
            continuation.finish(throwing: error)
        }
    }
    let timeoutTask = timeoutNanoseconds.map { timeoutNanoseconds in
        Task.detached {
            do {
                try await Task.sleep(nanoseconds: timeoutNanoseconds)
            } catch {
                return
            }
            continuation.finish(
                throwing: CKError(
                    .networkFailure,
                    userInfo: [
                        NSLocalizedDescriptionKey:
                            "CloudKit callback exceeded its deadline"
                    ]
                )
            )
        }
    }
    defer {
        timeoutTask?.cancel()
        continuation.finish()
    }
    var iterator = stream.makeAsyncIterator()
    guard let value = try await iterator.next() else {
        throw CancellationError()
    }
    return value
}

