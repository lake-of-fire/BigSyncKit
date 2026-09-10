import Foundation
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

private let bigSyncInterruptedOperationAttemptLimit = 8

/// Retries only operations that failed because a syscall was interrupted
/// before completion. Foundation commonly wraps `EINTR` in a Cocoa error, so
/// inspect the underlying-error chain rather than only the outer domain.
/// Other failures remain fail-closed and are returned immediately.
internal func bigSyncRetryingInterruptedOperation<T>(
    _ operation: () throws -> T
) throws -> T {
    for attempt in 1 ... bigSyncInterruptedOperationAttemptLimit {
        do {
            return try operation()
        } catch {
            guard attempt < bigSyncInterruptedOperationAttemptLimit,
                  bigSyncErrorWasInterrupted(error) else {
                throw error
            }
        }
    }
    preconditionFailure("Interrupted-operation retry loop exhausted unexpectedly")
}

private func bigSyncErrorWasInterrupted(_ error: Error) -> Bool {
    var candidate: NSError? = error as NSError
    for _ in 0 ..< 8 {
        guard let current = candidate else { return false }
        if current.domain == NSPOSIXErrorDomain,
           current.code == Int(EINTR) {
            return true
        }
        candidate = current.userInfo[NSUnderlyingErrorKey] as? NSError
    }
    return false
}

@discardableResult
internal func bigSyncFlock(
    _ descriptor: Int32,
    _ operation: Int32
) -> Int32 {
    while true {
        let result = flock(descriptor, operation)
        if result == 0 || errno != EINTR {
            return result
        }
    }
}

/// Creates every missing path component and durably publishes each directory
/// entry in its parent before returning. A leaf-directory fsync alone cannot
/// prove that a newly-created namespace survives power loss: the entry that
/// names that directory belongs to its parent.
internal func bigSyncCreateDirectoryDurably(
    at directoryURL: URL,
    fileManager: FileManager = .default
) throws {
    let targetURL = directoryURL.standardizedFileURL
    var missingDirectories = [URL]()
    var cursor = targetURL
    var isDirectory: ObjCBool = false

    while !fileManager.fileExists(
        atPath: cursor.path,
        isDirectory: &isDirectory
    ) {
        missingDirectories.append(cursor)
        let parent = cursor.deletingLastPathComponent().standardizedFileURL
        guard parent.path != cursor.path else {
            throw POSIXError(.ENOENT)
        }
        cursor = parent
    }
    guard isDirectory.boolValue else {
        throw CocoaError(.fileWriteFileExists)
    }

    for directory in missingDirectories.reversed() {
        do {
            try fileManager.createDirectory(
                at: directory,
                withIntermediateDirectories: false
            )
        } catch {
            var racedDirectory: ObjCBool = false
            guard fileManager.fileExists(
                atPath: directory.path,
                isDirectory: &racedDirectory
            ), racedDirectory.boolValue else {
                throw error
            }
        }
        try bigSyncSynchronizeDirectory(
            at: directory.deletingLastPathComponent()
        )
    }
}

internal func bigSyncSynchronizeDirectory(at directoryURL: URL) throws {
    let descriptor = open(directoryURL.path, O_RDONLY | O_DIRECTORY)
    guard descriptor >= 0 else {
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    defer { close(descriptor) }
    guard fsync(descriptor) == 0 else {
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
}


/// One independently opened lease. Never unlink/replace lock files, and never
/// share a descriptor between admissions: flock locks belong to open-file
/// descriptions, so reusing one descriptor would let a peer bypass itself.
internal final class BigSyncFileLease: @unchecked Sendable {
    private var descriptor: Int32
    private let url: URL

    init(at url: URL) throws {
        self.url = url.standardizedFileURL
        try bigSyncCreateDirectoryDurably(at: url.deletingLastPathComponent())
        descriptor = open(url.path, O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW,
                          S_IRUSR | S_IWUSR)
        guard descriptor >= 0 else { throw Self.posixError() }
        do {
            try bigSyncExcludeFromBackup(url)
            try validateIdentity()
        } catch {
            close(descriptor)
            descriptor = -1
            throw error
        }
    }

    deinit { if descriptor >= 0 { close(descriptor) } }

    /// Nonblocking so a peer cannot park a cooperative actor executor while
    /// an admitted upload needs that same executor in order to finish.
    func tryLock(exclusive: Bool) throws -> Bool {
        guard bigSyncFlock(descriptor, (exclusive ? LOCK_EX : LOCK_SH) | LOCK_NB) == 0 else {
            if errno == EWOULDBLOCK || errno == EAGAIN { return false }
            throw Self.posixError()
        }
        try validateIdentity()
        return true
    }

    func validateIdentity() throws {
        var held = stat()
        var named = stat()
        guard fstat(descriptor, &held) == 0, lstat(url.path, &named) == 0 else {
            throw Self.posixError()
        }
        guard held.st_dev == named.st_dev, held.st_ino == named.st_ino,
              (named.st_mode & S_IFMT) == S_IFREG else { throw POSIXError(.ESTALE) }
    }

    private static func posixError() -> POSIXError {
        POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
}

internal func bigSyncExcludeFromBackup(_ url: URL) throws {
#if canImport(Darwin)
    var mutableURL = url
    var values = URLResourceValues()
    values.isExcludedFromBackup = true
    try mutableURL.setResourceValues(values)
#endif
}

/// Publish bytes without a missing-file window. The directory entry and all
/// newly-created ancestor entries are synchronized before success is returned.
internal func bigSyncWriteDataDurably(_ data: Data, to url: URL) throws {
    try bigSyncCreateDirectoryDurably(at: url.deletingLastPathComponent())
    let temporary = url.deletingLastPathComponent()
        .appendingPathComponent(".\(url.lastPathComponent).\(UUID().uuidString).tmp")
    defer { try? FileManager.default.removeItem(at: temporary) }
    try bigSyncRetryingInterruptedOperation { try data.write(to: temporary, options: .withoutOverwriting) }
    try bigSyncExcludeFromBackup(temporary)
    let handle = try FileHandle(forWritingTo: temporary)
    defer { try? handle.close() }
    try bigSyncRetryingInterruptedOperation { try handle.synchronize() }
    guard rename(temporary.path, url.path) == 0 else {
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    try bigSyncSynchronizeDirectory(at: url.deletingLastPathComponent())
    guard try Data(contentsOf: url) == data else { throw CocoaError(.fileReadCorruptFile) }
}

/// Read a bounded regular file from the opened descriptor, not a pathname
/// preflight. Nonblocking open rejects a FIFO without parking the admission
/// lock; no-follow keeps a missing symlink target from looking like first use.
/// The caller still owns locking and decoding/semantic validation.
internal func bigSyncReadDataBoundedly(from url: URL, maximumBytes: Int) throws -> Data {
    guard url.isFileURL, maximumBytes >= 0, maximumBytes < Int.max else {
        throw CocoaError(.fileReadCorruptFile)
    }
    let descriptor = open(url.path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK)
    guard descriptor >= 0 else {
        // Preserve the existing caller's first-use versus missing-state policy.
        if errno == ENOENT { throw CocoaError(.fileReadNoSuchFile) }
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    let handle = FileHandle(fileDescriptor: descriptor, closeOnDealloc: true)
    defer { try? handle.close() }
    var metadata = stat()
    guard fstat(descriptor, &metadata) == 0 else {
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    guard (metadata.st_mode & S_IFMT) == S_IFREG,
          metadata.st_size >= 0, metadata.st_size <= maximumBytes else {
        throw CocoaError(.fileReadCorruptFile)
    }
    let expectedBytes = Int(metadata.st_size)
    var data = Data()
    data.reserveCapacity(expectedBytes)
    while true {
        // Short reads are legal. Read at most one extra byte to detect growth;
        // never let a growing file extend this bounded admission-lock scope.
        let remaining = expectedBytes - data.count
        guard let chunk = try handle.read(upToCount: min(65_536, remaining + 1)),
              !chunk.isEmpty else { break }
        guard chunk.count <= remaining else { throw CocoaError(.fileReadCorruptFile) }
        data.append(chunk)
    }
    guard data.count == expectedBytes else { throw CocoaError(.fileReadCorruptFile) }
    return data
}
