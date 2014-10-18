//
//
// Index insertion tests - currently supported via bulk write commands

var coll = db.getCollection( "batch_write_insert" );
assert(coll.getDB().getMongo().useWriteCommands(), "test is not running with write commands");

//
// Successful index creation
coll.drop();
request = { insert: "system.indexes",
            documents: [{ ns: coll.toString(), key: { x: 1 }, name: "x_1" }]};
result = coll.runCommand(request);
assert(result.ok, tojson(result));
assert.eq(1, result.n);
assert.eq(coll.getIndexes().length, 2);

//
// Duplicate index insertion gives n = 0
coll.drop();
coll.ensureIndex({x : 1}, {unique : true});
request = { insert: "system.indexes",
            documents : [{ ns: coll.toString(),
                           key: { x: 1 }, name: "x_1", unique: true}]};
result = coll.runCommand(request);
assert(result.ok, tojson(result));
assert.eq(0, result.n);
assert(!('writeErrors' in result));
assert.eq(coll.getIndexes().length, 2);

//
// Invalid index insertion with mismatched collection db
coll.drop();
request = { insert: "system.indexes",
            documents: [{ ns: "invalid." + coll.getName(),
                          key: { x: 1 }, name: "x_1", unique: true }]};
result = coll.runCommand(request);
assert(!result.ok, tojson(result));
assert.eq(coll.getIndexes().length, 0);

//
// Empty index insertion
coll.drop();
request = { insert: "system.indexes", documents : [{}] };
result = coll.runCommand(request);
assert(!result.ok, tojson(result));
assert.eq(coll.getIndexes().length, 0);

//
// Invalid index desc
coll.drop();
request = { insert: "system.indexes", documents: [{ ns: coll.toString() }] };
result = coll.runCommand(request);
assert(result.ok, tojson(result));
assert.eq(0, result.n);
assert.eq(0, result.writeErrors[0].index);
assert.eq(coll.getIndexes().length, 1);

//
// Invalid index desc
coll.drop();
request = { insert: "system.indexes",
            documents: [{ ns: coll.toString(), key: { x: 1 }}] };
result = coll.runCommand(request);
assert(result.ok, tojson(result));
assert.eq(0, result.n);
assert.eq(0, result.writeErrors[0].index);
assert.eq(coll.getIndexes().length, 1);

//
// Invalid index desc
coll.drop();
request = { insert: "system.indexes",
            documents: [{ ns: coll.toString(), name: "x_1" }]};
result = coll.runCommand(request);
assert(result.ok, tojson(result));
assert.eq(0, result.n);
assert.eq(0, result.writeErrors[0].index);
assert.eq(coll.getIndexes().length, 1);

//
// Cannot insert more than one index at a time through the batch writes
coll.drop();
request = { insert: "system.indexes",
            documents: [{ ns: coll.toString(), key: { x: 1 }, name: "x_1" },
                        { ns: coll.toString(), key: { y: 1 }, name: "y_1" }]};
result = coll.runCommand(request);
assert(!result.ok, tojson(result));
assert.eq(coll.getIndexes().length, 0);

//
// Background index creation
// Note: due to SERVER-13304 this test is at the end of this file, and we don't drop
// the collection afterwards.
coll.drop();
coll.insert({ x : 1 });
request = { insert: "system.indexes",
            documents: [{ ns: coll.toString(),
                          key: { x: 1 },
                          name: "x_1",
                          background : true }]};
result = coll.runCommand(request);
assert(result.ok, tojson(result));
assert.eq(1, result.n);
assert.eq(coll.getIndexes().length, 2);

