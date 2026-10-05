    public var serverChangeToken: RecordZoneChangeCursor? {
        get async {
            return await { @BigSyncBackgroundActor in
                guard let persistenceRealm = realmProvider?.persistenceRealm else { return nil }
                let serverToken = persistenceRealm.objects(ServerToken.self).first
                return serverToken?.token.map(RecordZoneChangeCursor.init(serializedData:))
            }()
        }
    }
