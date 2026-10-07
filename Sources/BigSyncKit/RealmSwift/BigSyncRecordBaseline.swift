            if property.isMap {
                let entries = try mapEntries(value, type: property.type)
                digest = frame(entries.sorted { $0.0 < $1.0 }.flatMap {
                    [Data($0.0.utf8), $0.1]
                })
            } else if property.isArray || property.isSet {
