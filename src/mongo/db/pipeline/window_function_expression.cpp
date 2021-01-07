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

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_add_fields.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "mongo/db/pipeline/document_source_set_window_fields.h"
#include "mongo/db/pipeline/document_source_set_window_fields_gen.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/db/query/query_feature_flags_gen.h"

#include "mongo/db/pipeline/window_function_expression.h"

using std::unordered_map;
using boost::intrusive_ptr;
using boost::optional;

namespace mongo {

StringMap<WindowFunctionExpression::Parser> WindowFunctionExpression::parserMap;


WindowBounds WindowBounds::parse(BSONObj args) {
    auto documents = args["documents"];
    auto range = args["range"];
    auto unit = args["unit"];

    uassert(0,
            "Window bounds can specify either 'documents' or 'range', not both.",
            !(documents && range));
    if (!range) {
        uassert(0,
                "Window bounds can only specify 'unit' with range-based bounds.",
                !unit);
    }

    if (documents) {
        uassert(0,
                "Window bounds must be a 2-element array.",
                documents.type() == BSONType::Array
                && documents.Obj().nFields() == 2);
        // TODO parse either: "unbounded", "current", or ExpressionConstant int.
        int lower = documents.Obj()[0].safeNumberLong();
        int upper = documents.Obj()[1].safeNumberLong();
        return WindowBounds{DocumentBased{lower, upper}};
    } else if (range) {
        uasserted(0, "TODO handle range-based bounds");
    } else {
        return WindowBounds{DocumentBased{Unbounded{}, Unbounded{}}};
    }
}
namespace {
template<class T>
Value serializeBound(const WindowBounds::Bound<T>& b) {
    return stdx::visit(
        visit_helper::Overloaded{
            [&](const WindowBounds::Unbounded&) { return Value("unbounded"_sd); },
            [&](const WindowBounds::Current&) { return Value("current"_sd); },
            [&](const T& n) { return Value(n); },
        },
        b);
}
}
void WindowBounds::serialize(MutableDocument& args) const {
    stdx::visit(
        visit_helper::Overloaded{
            [&](const DocumentBased& docBounds) {
                args["documents"] = Value{std::vector<Value>{
                    serializeBound(docBounds.lower),
                    serializeBound(docBounds.upper)}};
            },
            [&](const RangeBased& rangeBounds) {
                invariant(0 && "TODO range-based bounds");
            },
        },
        bounds);
}


intrusive_ptr<WindowFunctionExpression> WindowFunctionExpression::parse(
    BSONElement elem, optional<BSONObj> sortBy, ExpressionContext* expCtx) {
    auto parser = parserMap.find(elem.fieldName());
    uassert(0,
            str::stream() << "No such window function: " << elem.fieldName(),
            parser != parserMap.end());
    uassert(0,
            str::stream()
                << "Window function "
                << elem.fieldName()
                << " requires an object.",
            elem.type() == BSONType::Object);
    return parser->second(elem, sortBy, expCtx);
}

void WindowFunctionExpression::registerParser(std::string functionName, Parser parser) {
    invariant(parserMap.find(functionName) == parserMap.end());
    parserMap.emplace(std::move(functionName), std::move(parser));
}


class WFEAccumulator : public WindowFunctionExpression {
public:
    static intrusive_ptr<WindowFunctionExpression> parse(BSONElement elem, optional<BSONObj> sortBy, ExpressionContext* expCtx) {
        // 'elem' is something like '$sum: {input: E, ...}'
        std::string accumulatorName = elem.fieldName();
        intrusive_ptr<Expression> input = Expression::parseOperand(
            expCtx,
            elem.Obj()["input"],
            expCtx->variablesParseState);
        auto bounds = WindowBounds::parse(elem.Obj());
        return make_intrusive<WFEAccumulator>(
            std::move(accumulatorName),
            std::move(input),
            std::move(bounds));
    }
    Value serialize(optional<ExplainOptions::Verbosity> explain) const final {
        MutableDocument args;

        args["input"] = input->serialize(static_cast<bool>(explain));
        bounds.serialize(args);

        return Value{Document{
            {accumulatorName, args.freezeToValue()},
        }};
    }

    WFEAccumulator(
        std::string accumulatorName,
        intrusive_ptr<Expression> input,
        WindowBounds bounds)
    : accumulatorName(std::move(accumulatorName)),
      input(std::move(input)),
      bounds(std::move(bounds)) {}

private:
    std::string accumulatorName;
    intrusive_ptr<Expression> input;
    WindowBounds bounds;
};
MONGO_INITIALIZER(wfe_parserMap_accumulator)(InitializerContext*) {
    WindowFunctionExpression::registerParser("$sum", WFEAccumulator::parse);
    WindowFunctionExpression::registerParser("$max", WFEAccumulator::parse);

    return Status::OK();
}



}
