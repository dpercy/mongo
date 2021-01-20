(function() {
"use strict";

const featureEnabled =
    assert.commandWorked(db.adminCommand({getParameter: 1, featureFlagWindowFunctions: 1}))
        .featureFlagWindowFunctions.value;
if (!featureEnabled) {
    jsTestLog("Skipping test because the window function feature flag is disabled");
    return;
}

const coll = db[jsTestName()];
coll.drop();

assert.commandWorked(coll.insert({}));

// Test that the stage spec must be an object.
assert.commandFailedWithCode(
    coll.runCommand(
        {aggregate: coll.getName(), pipeline: [{$setWindowFields: "invalid"}], cursor: {}}),
    ErrorCodes.FailedToParse);

// Test that the stage parameters are the correct type.
assert.commandFailedWithCode(coll.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$setWindowFields: {sortBy: "invalid"}}],
    cursor: {}
}),
                             ErrorCodes.TypeMismatch);
assert.commandFailedWithCode(coll.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$setWindowFields: {output: "invalid"}}],
    cursor: {}
}),
                             ErrorCodes.TypeMismatch);

// Test that parsing fails for an invalid partitionBy expression.
assert.commandFailedWithCode(coll.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$setWindowFields: {partitionBy: {$notAnOperator: 1}, output: {}}}],
    cursor: {}
}),
                             ErrorCodes.InvalidPipelineOperator);

// Since partitionBy can be any expression, it can be a variable.
assert.commandWorked(coll.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$setWindowFields: {partitionBy: "$$NOW", output: {}}}],
    cursor: {}
}));
assert.commandWorked(coll.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$setWindowFields: {partitionBy: "$$myobj.a", output: {}}}],
    let: {myobj: {a: 456}},
    cursor: {}
}));

// Test that parsing fails for unrecognized parameters.
assert.commandFailedWithCode(
    coll.runCommand(
        {aggregate: coll.getName(), pipeline: [{$setWindowFields: {what_is_this: 1}}], cursor: {}}),
    40415);

// Test for a successful parse, ignoring the response documents.
assert.commandWorked(coll.runCommand({
    aggregate: coll.getName(),
    pipeline:
        [{$setWindowFields: {partitionBy: "$state", sortBy: {city: 1}, output: {a: {$sum: 1}}}}],
    cursor: {}
}));

})();
