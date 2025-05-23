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
    
    func store(data: Data, forRecordID recordID: String) -> URL {
        let unique = ProcessInfo.processInfo.globallyUniqueString
        let fileName = "\(recordID)_\(unique)"
        let url = assetDirectory.appendingPathComponent(fileName)
        try? data.write(to: url, options: .atomicWrite)
//        debugPrint("# wrote:", url.lastPathComponent)
        return url
    }
    
    func clearAssetFiles(excludingSyncedEntityIDs ids: Set<String>) {
        guard let fileURLs = try? FileManager.default.contentsOfDirectory(at: assetDirectory, includingPropertiesForKeys: nil, options: []) else {
            return
        }
        
        for fileURL in fileURLs {
            let fileName = fileURL.lastPathComponent
            // Find the last underscore in the file name
            if let underscoreIndex = fileName.lastIndex(of: "_") {
                // Extract the substring from the beginning to the underscore
                let recordID = String(fileName[..<underscoreIndex])
                if !ids.contains(recordID) {
                    try? FileManager.default.removeItem(at: fileURL)
//                    debugPrint("# deleted:", fileURL)
                }
            } else {
                print("Invalid file detected by PersistentAssetManager - deleting:", fileURL)
                try? FileManager.default.removeItem(at: fileURL)
            }
        }
    }
}
