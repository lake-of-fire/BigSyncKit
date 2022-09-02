# BigSyncKit

Synchronize RealmSwift databases with CloudKit.

Fork of [mentrena's SyncKit](https://mentrena.github.io/SyncKit) for these reasons:

- Scales to larger Realm databases
- Supports UUID primary keys

The downsides that were traded off to achieve this:
- Gives up ability to use fine-grained changed properties notifications to resolve merge conflicts. This was not a default behavior in SyncKit but could be enabled via the `client` merge policy.
- Removes CoreData support in order to simplify maintenance needed for this fork, and because it can better focus on the primary use case of this fork.
- Requires using isDeleted soft-deletes for reliable deletion sync. Cleans up automatically, deleting corresponding CKRecords and Realm objects.

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
