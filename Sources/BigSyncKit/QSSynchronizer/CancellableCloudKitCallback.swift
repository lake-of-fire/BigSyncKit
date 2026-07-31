import Foundation

/// Bridges callback-only CloudKit APIs without pinning the caller to a checked
/// continuation after its task is cancelled. The callback may still arrive,
/// but `AsyncThrowingStream` safely discards it after termination.
internal func awaitCancellableCloudKitCallback<Value>(
    _ start: (@escaping (Result<Value, Error>) -> Void) -> Void
) async throws -> Value {
    let stream = AsyncThrowingStream<Value, Error> { continuation in
        start { result in
            switch result {
            case .success(let value):
                continuation.yield(value)
                continuation.finish()
            case .failure(let error):
                continuation.finish(throwing: error)
            }
        }
    }
    var iterator = stream.makeAsyncIterator()
    guard let value = try await iterator.next() else {
        throw CancellationError()
    }
    return value
}
