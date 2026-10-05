    public var serverChangeToken: RecordZoneChangeCursor? {
        get async {
            return await { @BigSyncBackgroundActor in
                guard let persistenceRealm = realmProvider?.persistenceRealm else { return nil }
                // Another independently owned tracking write may be suspended.
                // Its provisional cursor is not permission to skip a server page.
                // Refresh may open such a write through notification reentry;
                // freeze unconditionally and let only detached bytes escape.
                if !persistenceRealm.isFrozen && !persistenceRealm.isInWriteTransaction {
                    persistenceRealm.refresh()
                }
                let snapshot = persistenceRealm.freeze()
                let serverToken = snapshot.objects(ServerToken.self).first
                return serverToken?.token.map(RecordZoneChangeCursor.init(serializedData:))
            }()
        }
    }
