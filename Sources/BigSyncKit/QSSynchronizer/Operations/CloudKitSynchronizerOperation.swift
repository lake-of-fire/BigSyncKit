//
//  QSCloudKitSynchronizerOperation.swift
//  Pods
//
//  Created by Manuel Entrena on 18/05/2018.
//

import Foundation
import Logging

class CloudKitSynchronizerOperation: Operation {
    override var isAsynchronous: Bool { return true }
    override var isExecuting: Bool { return state == .executing }
    override var isFinished: Bool { return state == .finished }
    @objc var errorHandler: ((CloudKitSynchronizerOperation, Error) -> ())?
    
    internal var logger: Logging.Logger?

    private let stateLock = NSRecursiveLock()
    private var _state = State.ready
    private var state: State {
        stateLock.withLock { _state }
    }

    @discardableResult
    private func transition(to newState: State) -> Bool {
        stateLock.withLock {
            let oldState = _state
            guard oldState != newState else { return false }
            willChangeValue(forKey: oldState.keyPath)
            willChangeValue(forKey: newState.keyPath)
            _state = newState
            didChangeValue(forKey: newState.keyPath)
            didChangeValue(forKey: oldState.keyPath)
            return true
        }
    }
    
    enum State: String {
        case ready = "Ready"
        case executing = "Executing"
        case finished = "Finished"
        fileprivate var keyPath: String { return "is" + self.rawValue }
    }
    
    override func start() {
        if self.isCancelled {
            finish(error: nil)
        } else {
            logStart()
            main()
        }
    }
    
    override func main() {
        transition(to: self.isCancelled ? .finished : .executing)
    }
    
    func finish(error: Error?) {
        guard transition(to: .finished) else { return }
        if let error {
//            logger?.info("QSCloudKitSynchronizer >> Operation failed: \(error)")
            errorHandler?(self, error)
        } else {
//            logger?.info("QSCloudKitSynchronizer >> Operation succeeded: \(type(of: self))")
        }
    }

    override func cancel() {
        super.cancel()
        finish(error: nil)
    }
    
    internal func logStart() {
//        logger?.info("QSCloudKitSynchronizer >> Starting operation: \(type(of: self))")
    }
}
