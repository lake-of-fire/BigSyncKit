# BigSyncKit
**Available for hire:** *If you need help adding iCloud sync to your RealmSwift project, or you're using SyncKit/IceCream already and need to fix the issues you'll face with those libraries, I'm available for custom integrating this library into your existing projects. Please contact me via alex dot ehlke at gmail.*

CURRENT STATUS: Working reliably in production, but docs and code need cleanup. Test suite hasn't been updated.

Synchronize RealmSwift databases with CloudKit.

This project began with a fork of [mentrena's SyncKit](https://mentrena.github.io/SyncKit) and combines pieces of [caiyue1993's IceCream](https://github.com/caiyue1993/IceCream) to improve some gaps and scalability. This project builds on that foundation with further scalability-focused improvements.

- Scales to much larger Realm databases. The goal is to scale to millions of records and beyond.
- Supports latest RealmSwift versions with modern `@Persisted` annotation.
- Supports UUID primary keys.
- Supports List (Array) and MutableSet (Set) properties for primitives and custom object types.
- Moves most operations from the main thread into background threads (see BackgroundWorker).
- Adds schema version metadata for use in custom conflict resolution and migrating older incoming data.

The downsides that were traded off to achieve this:
- Removes CKReference / CKRecord.Reference usage. Record names are stored directly as a plain string field. This is because CloudKit limits references with deletion cascades to 750 references (leaving little upside to using references), and because references require precise sequencing in uploading/downloading records which leads to unreasonable memory requirements at scale (at least without a more performant rearchitecture). This trade-off is mitigated by moving data integrity checks or flexibility for eventually-consistent data into clients. It can be worth it at scale.
- Gives up ability to use fine-grained changed properties notifications to resolve merge conflicts. This was not a default behavior in SyncKit but could be enabled via the `client` merge policy or a PR to add this capability first-class.
- Custom conflict resolution functions must also account for list objects.
- Removes CoreData support in order to simplify maintenance needed for this fork, and because it can better focus on the primary use case of this fork.
- Requires using isDeleted soft-deletes for reliable deletion sync. (Future to-do: Cleans up automatically, deleting corresponding CKRecords and Realm objects.)

TODO:
- [ ] Require isDeleted field, and use as IceCream does for CK deletion operations.

Future roadmap aspirations:
- [ ] Realtime sync with RxDB via WKWebView postMessage
- [ ] Realtime sync with Supabase
- [ ] Realtime sync with websocket
- [ ] OpenAPI-Generator custom templates for BigSyncKit boilerplate and configuration from specs

-------------
**Old readme copied below:**

![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/mentrena/synckit/Test/master)
[![Carthage compatible](https://img.shields.io/badge/Carthage-compatible-4BC51D.svg?style=flat)](https://github.com/Carthage/Carthage)
[![Version](https://img.shields.io/cocoapods/v/SyncKit.svg?style=flat)](http://cocoapods.org/pods/SyncKit)
[![License](https://img.shields.io/cocoapods/l/SyncKit.svg?style=flat)](http://cocoapods.org/pods/SyncKit)
[![Platform](https://img.shields.io/cocoapods/p/SyncKit.svg?style=flat)](http://cocoapods.org/pods/SyncKit)

SyncKit automates the process of synchronizing Core Data or Realm models using CloudKit.

SyncKit uses introspection to work with any model. It sits next to your Core Data or Realm stack, making it easy to add synchronization to existing apps.

For installation instructions and more information check the [Docs](https://mentrena.github.io/SyncKit)

## Author

Manuel Entrena, manuel@mentrena.com

## License

SyncKit is available under the MIT license. See the LICENSE file for more info.
