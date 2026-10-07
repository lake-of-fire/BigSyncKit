import Foundation
import XCTest
@testable import BigSyncKit

final class MapIdentityValueTests: XCTestCase, @unchecked Sendable {
    private let pairs: [(String, String)] = [
        ("\u{304C}", "\u{304B}\u{3099}"), ("\u{00E9}", "e\u{0301}"),
        ("\u{AC00}", "\u{1100}\u{1161}"), ("a\u{0323}\u{0301}", "a\u{0301}\u{0323}"),
    ]

    func testIntegerMapKeyRenameRetainsByteIdentity() async {
        for (a, b) in pairs {
            XCTAssertEqual(a, b)
            XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual([(key:a,value:7)], [(key:b,value:7)], by: ==))
        }
    }
    func testBothRenameDirectionsRejectForAllExistingScalarComparisons() async {
        func check<Value: Equatable>(_ value: Value) {
            for (a,b) in pairs {
                XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual([(key:a,value:value)], [(key:b,value:value)], by: ==))
                XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual([(key:b,value:value)], [(key:a,value:value)], by: ==))
            }
        }
        check(true); check(Float(1.25)); check(Double(1.25)); check(Data([0,255]))
        check(Date(timeIntervalSinceReferenceDate:1234)); check(UUID(uuidString:"AABBCCDD-0000-4000-8000-000000000001")!)
    }
    func testEqualMapPermutationIsNoOp() async {
        XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual([(key:"a",value:1),(key:"b",value:2)], [(key:"b",value:2),(key:"a",value:1)], by: ==))
    }
    func testChangedValueAtExactKeyRejects() async {
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual([(key:"a",value:1)], [(key:"a",value:2)], by: ==))
    }
    func testMissingAndExtraMapMembersReject() async {
        let one=[(key:"a",value:1)], two=[(key:"a",value:1),(key:"b",value:2)]
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(one,two,by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(two,one,by: ==))
    }
    func testEmptyMapMatchesOnlyEmptyMap() async {
        let empty=[(key:String,value:Int)]()
        XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual(empty,empty,by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(empty,[(key:"",value:0)],by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual([(key:"",value:0)],empty,by: ==))
    }
    func testDuplicateExactKeysRejectInsteadOfLastWriteWins() async {
        let one=[(key:"a",value:1)], twice=[(key:"a",value:1),(key:"a",value:1)]
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(one,twice,by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(twice,one,by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(twice,twice,by: ==))
    }
    func testDistinctEquivalentKeysDoNotBecomeDuplicateExactKeys() async {
        for (a,b) in pairs {
            let lhs=[(key:a,value:1),(key:b,value:2)], rhs=[(key:b,value:2),(key:a,value:1)]
            XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual(lhs,rhs,by: ==))
            XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(lhs,[(key:a,value:2)],by: ==))
        }
    }
    func testOptionalNilValueIsDifferentFromMissingKey() async {
        let nilValue=[(key:"a",value:Optional<Int>.none)], empty=[(key:String,value:Int?)]()
        XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual(nilValue,nilValue,by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(nilValue,empty,by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(empty,nilValue,by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(nilValue,[(key:"a",value:Optional(1))],by: ==))
    }
    func testEmptyAndEmbeddedNULKeysStayDistinct() async {
        let lhs=[(key:"",value:0),(key:"a\0b",value:1),(key:"a",value:2)]
        XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual(lhs,lhs.reversed(),by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(lhs,[(key:"",value:0),(key:"a",value:1)],by: ==))
    }
    func testStringMapKeepsExactValuePolicy() async {
        for (a,b) in pairs {
            XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual([(key:"key",value:a)],[(key:"key",value:b)]))
            XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual([(key:a,value:b)],[(key:a,value:b)]))
        }
    }
    func testValuePolicyIsExplicitNotAnImplicitNormalization() async {
        let lhs=[(key:"a",value:11)], rhs=[(key:"a",value:21)]
        XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual(lhs,rhs,by: { $0 % 10 == $1 % 10 }))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual(lhs,rhs,by: ==))
    }
    func testMapInputsAreConsumedOnlyOnce() async {
        final class Cursor<Value> {
            var values:[(key:String,value:Value)]; var index=0
            init(_ values:[(key:String,value:Value)]) { self.values=values }
        }
        func once<Value>(_ values:[(key:String,value:Value)]) -> AnySequence<(key:String,value:Value)> {
            let cursor=Cursor(values)
            return AnySequence<(key:String,value:Value)> { AnyIterator<(key:String,value:Value)> {
                guard cursor.index < cursor.values.count else { return nil }
                defer { cursor.index += 1 }; return cursor.values[cursor.index]
            } }
        }
        XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual(once([(key:"a",value:1)]),once([(key:"a",value:1)]),by: ==))
    }
    func testFloatingPointValueComparisonRetainsNaNAndSignedZeroBehavior() async {
        XCTAssertTrue(BigSyncStringIdentity.mappedValuesEqual([(key:"a",value:-0.0)],[(key:"a",value:0.0)],by: ==))
        XCTAssertFalse(BigSyncStringIdentity.mappedValuesEqual([(key:"a",value:Double.nan)],[(key:"a",value:Double.nan)],by: ==))
    }
    func testOpaqueMapEncodingAmbiguityGateIsNotWeakened() async {
        for (a,b) in pairs {
            XCTAssertTrue(BigSyncStringIdentity.mapKeysAreUnambiguous([a]))
            XCTAssertTrue(BigSyncStringIdentity.mapKeysAreUnambiguous([b]))
            XCTAssertFalse(BigSyncStringIdentity.mapKeysAreUnambiguous([a,b]))
        }
        XCTAssertTrue(BigSyncStringIdentity.mapKeysAreUnambiguous([String]()))
    }
    func testSortBreaksOnlyCanonicalEquivalenceTies() async {
        for (a,b) in pairs {
            XCTAssertFalse(a < b); XCTAssertFalse(b < a)
            XCTAssertNotEqual(BigSyncStringIdentity.less(a,b),BigSyncStringIdentity.less(b,a))
            XCTAssertEqual(BigSyncStringIdentity.less(a,b),a.utf8.lexicographicallyPrecedes(b.utf8))
        }
    }
    func testSortRetainsHistoricalOrderForUnambiguousMaps() async {
        let keys=["","\0","a","z","\u{00E9}","\u{212B}","\u{304C}","\u{AC00}","\u{1F600}"]
        for a in keys { for b in keys where a != b { XCTAssertEqual(BigSyncStringIdentity.less(a,b),a < b) } }
        XCTAssertEqual(keys.sorted(by:BigSyncStringIdentity.less).map{Data($0.utf8)},keys.sorted().map{Data($0.utf8)})
    }
    func testSortIsStrictAndTransitiveOverRawIdentities() async {
        let keys=["","a","z"]+pairs.flatMap{[$0.0,$0.1]}
        for a in keys {
            XCTAssertFalse(BigSyncStringIdentity.less(a,a))
            for b in keys where BigSyncStringIdentity.less(a,b) {
                XCTAssertFalse(BigSyncStringIdentity.less(b,a))
                for c in keys where BigSyncStringIdentity.less(b,c) { XCTAssertTrue(BigSyncStringIdentity.less(a,c)) }
            }
        }
    }
    func testMapFingerprintKeyOrderIsIndependentOfEnumerationOrder() async {
        for (a,b) in pairs {
            let entries=[(a,Data([1])),(b,Data([2])),("z",Data([3]))]
            func ordered(_ input:[(String,Data)]) -> [Data] {
                input.sorted{BigSyncStringIdentity.less($0.0,$1.0)}.flatMap{[Data($0.0.utf8),$0.1]}
            }
            for permutation in [[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]] {
                XCTAssertEqual(ordered(entries),ordered(permutation.map{entries[$0]}))
            }
        }
    }
    func testExistingScalarAndOrderedStringIdentityStayExact() async {
        for (a,b) in pairs {
            XCTAssertFalse(BigSyncStringIdentity.equal(a,b))
            XCTAssertFalse(BigSyncStringIdentity.orderedValuesEqual([a,b],[b,a]))
            XCTAssertTrue(BigSyncStringIdentity.orderedValuesEqual([a,b],[a,b]))
        }
    }
    func testExistingUnorderedStringIdentityKeepsDistinctMembers() async {
        for (a,b) in pairs {
            XCTAssertTrue(BigSyncStringIdentity.unorderedValuesEqual([a,b,a],[b,a]))
            XCTAssertFalse(BigSyncStringIdentity.unorderedValuesEqual([a,b],[a]))
        }
    }
}
