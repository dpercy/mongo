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

#include "mongo/db/pipeline/accumulation_statement.h"
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
namespace wf = mongo::window_function;
using wf::WindowBounds;

namespace mongo {

StringMap<wf::Expression::Parser> wf::Expression::parserMap;


namespace {
template<class T>
WindowBounds::Bound<T> parseBound(
        ExpressionContext* expCtx,
        BSONElement elem,
        std::function<T(Value)> handleExpression) {
    if (elem.type() == BSONType::String) {
        auto s = elem.str();
        if (s == "unbounded") {
            return WindowBounds::Unbounded{};
        } else if (s == "current") {
            return WindowBounds::Current{};
        } else {
            uasserted(ErrorCodes::FailedToParse,
                      "Window bounds must be 'unbounded', 'current', or a number.");
        }
    } else {
        // Expect a constant number expression.
        auto expr = ::mongo::Expression::parseOperand(expCtx, elem, expCtx->variablesParseState);
        expr = expr->optimize();
        auto constant = dynamic_cast<ExpressionConstant*>(expr.get());
        uassert(ErrorCodes::FailedToParse,
                "Window bounds expression must be a constant.",
                constant);
        return handleExpression(constant->getValue());
    }
}
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

WindowBounds WindowBounds::parse(BSONObj args, ExpressionContext* expCtx) {
    auto documents = args["documents"];
    auto range = args["range"];
    auto unit = args["unit"];

    uassert(ErrorCodes::FailedToParse,
            "Window bounds can specify either 'documents' or 'range', not both.",
            !(documents && range));
    if (!range) {
        uassert(ErrorCodes::FailedToParse,
                "Window bounds can only specify 'unit' with range-based bounds.",
                !unit);
    }

    if (!range && !documents) {
        return WindowBounds{DocumentBased{Unbounded{}, Unbounded{}}};
    }

    auto unpack = [](BSONElement e) -> std::pair<BSONElement, BSONElement> {
        uassert(ErrorCodes::FailedToParse,
                "Window bounds must be a 2-element array.",
                e.type() == BSONType::Array && e.Obj().nFields() == 2);
        return {e.Obj()[0], e.Obj()[1]};
    };
    if (documents) {
        auto [lower, upper] = unpack(documents);

        auto parseInt = [](Value v) -> int {
            uassert(ErrorCodes::FailedToParse,
                    "Numeric document-based bounds must be an integer",
                    v.integral());
            return v.coerceToInt();
        };
        return WindowBounds{DocumentBased{
            parseBound<int>(expCtx, lower, parseInt),
            parseBound<int>(expCtx, upper, parseInt),
        }};
    } else {
        auto [lower, upper] = unpack(range);

        optional<TimeUnit> parsedUnit;
        if (unit) {
            uassert(ErrorCodes::FailedToParse,
                    "'unit' must be a string",
                    unit.type() == BSONType::String);
            parsedUnit = parseTimeUnit(unit.str());
        }

        auto identity = [](Value v) -> Value { return v; };
        return WindowBounds{RangeBased{
            parseBound<Value>(expCtx, lower, identity),
            parseBound<Value>(expCtx, upper, identity),
            parsedUnit,
        }};
    }
}
void WindowBounds::serialize(MutableDocument& args) const {
    stdx::visit(
        visit_helper::Overloaded{
            [&](const DocumentBased& docBounds) {
                args["documents"] = Value{std::vector<Value>{
                    serializeBound(docBounds.lower),
                    serializeBound(docBounds.upper),
                }};
            },
            [&](const RangeBased& rangeBounds) {
                args["range"] = Value{std::vector<Value>{
                    serializeBound(rangeBounds.lower),
                    serializeBound(rangeBounds.upper),
                }};
                if (rangeBounds.unit) {
                    args["unit"] = Value{serializeTimeUnit(*rangeBounds.unit)};
                }
            },
        },
        bounds);
}


intrusive_ptr<wf::Expression> wf::Expression::parse(
    BSONElement elem, optional<BSONObj> sortBy, ExpressionContext* expCtx) {
    auto parser = parserMap.find(elem.fieldName());
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "No such window function: " << elem.fieldName(),
            parser != parserMap.end());
    uassert(ErrorCodes::FailedToParse,
            str::stream()
                << "Window function "
                << elem.fieldName()
                << " requires an object.",
            elem.type() == BSONType::Object);
    return parser->second(elem, sortBy, expCtx);
}

void wf::Expression::registerParser(std::string functionName, Parser parser) {
    invariant(parserMap.find(functionName) == parserMap.end());
    parserMap.emplace(std::move(functionName), std::move(parser));
}


}
