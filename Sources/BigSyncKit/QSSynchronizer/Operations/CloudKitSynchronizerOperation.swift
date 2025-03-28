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

    var state = State.ready {
        willSet {
            willChangeValue(forKey: state.keyPath)
            willChangeValue(forKey: newValue.keyPath)
        }
        didSet {
            didChangeValue(forKey: state.keyPath)
            didChangeValue(forKey: oldValue.keyPath)
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
            state = .finished
        } else {
            state = .ready
            main()
        }
    }
    
    override func main() {
        state = self.isCancelled ? .finished : .executing
    }
    
    func finish(error: Error?) {
        if let error = error {
            errorHandler?(self, error)
        } else {
            logger?.info("QSCloudKitSynchronizer >> Operation succeeded: \(type(of: self))")
        }
        state = .finished
    }
    
    internal func logStart() {
         logger?.info("QSCloudKitSynchronizer >> Starting operation: \(type(of: self))")
   }
}
