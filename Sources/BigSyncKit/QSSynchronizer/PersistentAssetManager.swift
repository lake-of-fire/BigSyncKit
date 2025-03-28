//
//  PersistentAssetManager.swift
//  Pods-CoreDataExample
//
//  Created by Manuel Entrena on 25/04/2019.
//

import Foundation

class PersistentAssetManager {
    let identifier: String
    init(identifier: String) {
        self.identifier = identifier
    }
    
    private lazy var assetDirectory: URL = {
        let appSupportURL = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        let directoryURL = appSupportURL
            .appendingPathComponent("CloudKitAssets")
            .appendingPathComponent(identifier)
        
        if !FileManager.default.fileExists(atPath: directoryURL.path) {
            try? FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true, attributes: nil)
        }
        
        return directoryURL
    }()
    
    func store(data: Data) -> URL {
        
        let fileName = ProcessInfo.processInfo.globallyUniqueString
        let url = assetDirectory.appendingPathComponent(fileName)
        try? data.write(to: url, options: .atomicWrite)
        return url
    }
    
    func clearAssetFiles() {
        guard let fileURLs = try? FileManager.default.contentsOfDirectory(at: assetDirectory, includingPropertiesForKeys: nil, options: []) else {
            return
        }
        
        for fileURL in fileURLs {
            try? FileManager.default.removeItem(at: fileURL)
        }
    }
}

func delete(fileAt url: URL) {
    try? FileManager.default.removeItem(at: url)
}
