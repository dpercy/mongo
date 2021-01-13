/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_set_window_fields_gen.h"
#include "mongo/db/query/query_feature_flags_gen.h"

#define REGISTER_WINDOW_FUNCTION(name, parser)                                              \
    MONGO_INITIALIZER_GENERAL(addToWindowFunctionMap_##name,                                \
                              (),                                                           \
                              ())(InitializerContext*) {                                    \
        if (!::mongo::feature_flags::gFeatureFlagWindowFunctions.isEnabledAndIgnoreFCV()) { \
            return Status::OK();                                                            \
        }                                                                                   \
        namespace wf = ::mongo::window_function;                                            \
        ::mongo::window_function::Expression::registerParser(#name, parser);                \
        return Status::OK();                                                                \
    }

namespace mongo {
namespace window_function {

/**
 * A window-function expression describes how to compute a single output value in a
 * $setWindowFields stage. For example, in
 * 
 *     {$setWindowFields: {
 *         output: {
 *             totalCost: {$sum: {input: "$price"}},
 *             numItems: {$count: {}},
 *         }
 *     }}
 * 
 * the two window-function expressions are {$sum: {input: "$price"}} and {$count: {}}.
 * 
 * Because this class is part of a syntax tree, it does not hold any execution state:
 * instead it lets you create new instances of a window-function state.
 * 
 * Its other responsibilities include:
 * - parsing and serialization
 * - TODO dependency analysis?
 * - TODO rewrites?
 */
class Expression : public RefCountable {
public:
    /**
     * Parses a single window-function expression. The BSONElement's key is the function name,
     * and the value is the spec: for example, the whole BSONElement might be '$sum: {input: "$x"}'.
     * 
     * 'sortBy' is from the sortBy argument of $setWindowFields. Some window functions require
     * a sort spec, or require a one-field sort spec; they use this argument to enforce those
     * requirements.
     */
    static boost::intrusive_ptr<Expression> parse(
        BSONElement elem, boost::optional<BSONObj> sortBy, ExpressionContext* expCtx);

    /**
     * A Parser has the same signature as parse(). The BSONElement is the whole expression, such
     * as '$sum: {$input: "$x"}', because some parsers need to switch on the function name.
     */
    using Parser = std::function<decltype(parse)>;
    static void registerParser(std::string functionName, Parser parser);

    virtual Value serialize(boost::optional<ExplainOptions::Verbosity> explain) const = 0;

    // TODO: another virtual method should create the execution state.

private:
    static StringMap<Parser> parserMap;
};

/**
 * Window bounds describe a set of documents around the current document.
 *
 * Document-based bounds select documents based on their position in the input:
 * 
 *     documents: [-2, +4]
 *     documents: [-2, 0]
 * 
 * Range-based bounds select documents based on the value of the sortBy field.
 *
 *     range: [-0.3, +2.4]
 *     range: [-0.3, +2.4], unit: 'seconds'
 * 
 * In either case, the lower and upper bound can each be 'unbounded', or 'current'.
 * 
 *     documents: ['unbounded', +4]
 *     range: ['unbounded', 'current']
 */
struct WindowBounds {

    struct Unbounded {};
    struct Current {};
    template<class T>
    using Bound = stdx::variant<Unbounded, Current, T>;

    struct DocumentBased {
        Bound<int> lower;
        Bound<int> upper;
    };
    struct RangeBased {
        // Range-based bounds can be any numeric type: int, double, Decimal, etc.
        Bound<Value> lower;
        Bound<Value> upper;

        boost::optional<TimeUnit> unit;
    };

    stdx::variant<DocumentBased, RangeBased> bounds;

    /**
     * Parses bounds from the arguments object of a window-function expression.
     * For example, in:
     *
     *     {$setWindowFields: {
     *         output: {
     *             v: {$sum: {input: "$x", range: [-1, +1], unit: 'seconds'}},
     *         }
     *     }}
     * 
     * 'args' would be {input: "$x", range: [-1, +1], unit: 'seconds'}.
     * 
     * If the BSON doesn't specify bounds, we default to:
     *
     *     documents: ['unbounded', 'unbounded']
     */
    static WindowBounds parse(BSONObj args, ExpressionContext* expCtx);

    void serialize(MutableDocument& args) const;
};

template<class AccumulatorState>
class ExpressionFromAccumulator : public Expression {
public:
    static boost::intrusive_ptr<Expression> parse(
        BSONElement elem,
        boost::optional<BSONObj> sortBy, 
        ExpressionContext* expCtx) {
        // 'elem' is something like '$sum: {input: E, ...}'
        std::string accumulatorName = elem.fieldName();
        boost::intrusive_ptr<::mongo::Expression> input = ::mongo::Expression::parseOperand(
            expCtx,
            elem.Obj()["input"],
            expCtx->variablesParseState);
        auto bounds = WindowBounds::parse(elem.Obj(), expCtx);
        return make_intrusive<ExpressionFromAccumulator<AccumulatorState>>(
            std::move(accumulatorName),
            std::move(input),
            std::move(bounds));
    }
    Value serialize(boost::optional<ExplainOptions::Verbosity> explain) const final {
        MutableDocument args;

        args["input"] = input->serialize(static_cast<bool>(explain));
        bounds.serialize(args);

        return Value{Document{
            {accumulatorName, args.freezeToValue()},
        }};
    }

    ExpressionFromAccumulator(
        std::string accumulatorName,
        boost::intrusive_ptr<::mongo::Expression> input,
        WindowBounds bounds)
    : accumulatorName(std::move(accumulatorName)),
      input(std::move(input)),
      bounds(std::move(bounds)) {}

private:
    std::string accumulatorName;
    boost::intrusive_ptr<::mongo::Expression> input;
    WindowBounds bounds;
};

}
}
