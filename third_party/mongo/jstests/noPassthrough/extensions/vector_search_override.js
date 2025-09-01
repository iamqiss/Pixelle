/**
 * Tests that the extension $vectorSearch stage has overriden the server implementation
 * when loaded.
 *
 * @tags: [featureFlagExtensionsAPI]
 */
import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {isLinux} from "jstests/libs/os_helpers.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";

if (!isLinux()) {
    jsTest.log.info("Skipping test since extensions are only available on Linux platforms.");
    quit();
}

const pathToExtensionVectorSearch = MongoRunner.getExtensionPath("libvector_search_extension.so");
const options = {
    loadExtensions: pathToExtensionVectorSearch,
};

function runVectorSearchOverrideTest(conn, shardingTest = null) {
    const db = conn.getDB("test");
    const coll = db[jsTestName()];
    coll.drop();
    const testData = [
        {_id: 0, vector: [1, 2, 3, 4], text: "poppi cans"},
        {_id: 1, vector: [0, 2, 4, 6], text: "homegrown tomatoes"},
        {_id: 2, vector: [3, 6, 9, 16], text: "crispy rice puffs"},
    ];
    assert.commandWorked(coll.insertMany(testData));
    if (shardingTest) {
        shardingTest.shardColl(coll, {_id: 1});
    }

    // Test one $vectorSearch stage passes documents through unchanged.
    {
        const pipeline = [{$vectorSearch: {}}];
        const result = coll.aggregate(pipeline).toArray();

        assertArrayEq({actual: result, expected: testData});
    }

    // Test $vectorSearch stage at different positions in complex pipeline.
    {
        const pipeline = [
            {$vectorSearch: {}},
            {$match: {_id: {$in: [0, 1]}}},
            {$vectorSearch: {}},
            {$project: {y: "$text", _id: 0}},
            {$vectorSearch: {a: 0}},
            {$sort: {y: -1}},
        ];
        const result = coll.aggregate(pipeline).toArray();

        assertArrayEq({actual: result, expected: [{y: "homegrown tomatoes"}, {y: "poppi cans"}]});
    }
}

// Test $vectorSearch override on a standalone mongod.
const mongodConn = MongoRunner.runMongod(options);
runVectorSearchOverrideTest(mongodConn);
MongoRunner.stopMongod(mongodConn);

// Test $vectorSearch override in a sharded cluster.
const shardingTest = new ShardingTest({
    shards: 2,
    rs: {nodes: 2},
    mongos: 1,
    config: 1,
    mongosOptions: options,
    configOptions: options,
    rsOptions: options,
});
runVectorSearchOverrideTest(shardingTest.s, shardingTest);
shardingTest.stop();
