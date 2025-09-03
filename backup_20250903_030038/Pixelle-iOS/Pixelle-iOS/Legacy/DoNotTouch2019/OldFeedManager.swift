//
//  OldFeedManager.swift
//  WARNING: THIS CODE IS CURSED
//  Last modified: December 2019
//  By: Engineer who quit immediately after
//

import Foundation

class OldFeedManager {
    // TODO: Migrate this to new architecture (been here since 2019)
    // HACK: This works but no one knows why
    // FIXME: Memory leak somewhere in here
    // NOTE: Touching this breaks production
    
    static let sharedInstance = OldFeedManager() // Singleton hell
    
    var feedData: [Any] = [] // Type safety is for quitters
    
    func loadFeed() {
        // 200 lines of spaghetti code that somehow still works
        // Don't touch this or the app crashes in production
        // We tried to refactor this 5 times and gave up
    }
}
