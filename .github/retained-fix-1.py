from pathlib import Path
p=Path('Sources/BigSyncKit/RealmSwift/BigSyncRecordEvidence.swift')
s=p.read_text();old='''        let copy = type.init(value: losingObject)
        copy.setValue(uuid, forKey: key.name)
        (copy as? SoftDeletable)?.isDeleted = false
        realm.add(copy)''';new='''        var values = [String: Any]()
        for property in losingObject.objectSchema.properties where property.type != .linkingObjects {
            values[property.name] = losingObject[property.name] ?? NSNull()
        }
        values[key.name] = uuid
        values["isDeleted"] = false
        let copy = realm.create(type, value: values)''';assert s.count(old)==1;s=s.replace(old,new);p.write_text(s)
p=Path('Sources/BigSyncKit/RealmSwift/RealmSwiftAdapter.swift');s=p.read_text();old='''    @RealmBackgroundActor
    private func applyComparisonFields(''';assert s.count(old)==1;s=s.replace(old,'''    // Caller owns this Realm's executor and final write transaction. Inbound
    // import and explicit conflict resolution use different existing actors;
    // no managed object crosses that boundary through this synchronous helper.
    private func applyComparisonFields(''');old='''    ) throws -> Bool {
        var merged = local
        for key in incoming''';assert s.count(old)==1;s=s.replace(old,'''    ) throws -> Bool {
        precondition(realm.isInWriteTransaction)
        var merged = local
        for key in incoming''');s=s.replace('&& row.containerIdentifier == activeContainerIdentifier','&& row.containerIdentifier == self.activeContainerIdentifier').replace('&& row.databaseScopeRawValue == activeDatabaseScopeRawValue','&& row.databaseScopeRawValue == self.activeDatabaseScopeRawValue').replace('&& row.zoneOwnerName == recordZoneID.ownerName && row.zoneName == recordZoneID.zoneName','&& row.zoneOwnerName == self.recordZoneID.ownerName && row.zoneName == self.recordZoneID.zoneName');p.write_text(s)
