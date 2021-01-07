// Just some examples for now.

function example(spec) {
    const explain = db.c.explain().aggregate([
        {$setWindowFields: spec}
    ]);
    const parsed = explain.stages.map(x => x.$_setWindowFields_assumeSorted).filter(x => x)[0];
    printjson({ spec, parsed });
}

// The most basic case: $sum everything.
example({output: {v: {$sum: { input: "$a", }}}});

// That's equivalent to bounds of [unbounded, unbounded].
example({output: {v: {$sum: { input: "$a", documents: ['unbounded', 'unbounded'], }}}});

// Bounds can be bounded, or bounded on one side.
example({output: {v: {$sum: { input: "$a", documents: [-2, +4], }}}});
example({output: {v: {$sum: { input: "$a", documents: [-3, 'unbounded'], }}}});
example({output: {v: {$sum: { input: "$a", documents: ['unbounded', +5], }}}});


// Range-based bounds:
example({output: {v: {$sum: { input: "$a", range: ['unbounded', 'unbounded'], }}}});
example({output: {v: {$sum: { input: "$a", range: [-2, +4], }}}});
example({output: {v: {$sum: { input: "$a", range: [-3, 'unbounded'], }}}});
example({output: {v: {$sum: { input: "$a", range: ['unbounded', +5], }}}});
example({output: {v: {$sum: { input: "$a", range: [NumberDecimal('1.01'), NumberLong(5)]}}}});

// Time-based bounds:
example({output: {v: {$sum: { input: "$a", range: [-3, 'unbounded'], unit: 'hour' }}}});



// $rank doesn't take an input.
// But it requires a sortBy.
example({ sortBy: {x: 1, y: -1}, output: {v: {$rank: {}}}, });


// $derivative also requires a sortBy, which must be a single field.
// It's interpreted as the time axis.
// - If the time axis is numeric, it computes (delta input)/(delta time).
//   The output uses the same units as the input and sortBy field.
// - If the time axis is datetime, you must specify which time unit
//   you'd like the output expressed in.
example({ sortBy: {ts: 1}, output: {v: {$derivative: {input: "$a"}}}, });
example({ sortBy: {ts: 1}, output: {v: {$derivative: {input: "$a", unit: 'second'}}}, });
