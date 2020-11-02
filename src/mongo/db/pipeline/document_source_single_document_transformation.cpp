/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#include "mongo/db/pipeline/document_source_single_document_transformation.h"

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_skip.h"
#include "mongo/db/pipeline/expression.h"

namespace mongo {

using boost::intrusive_ptr;

DocumentSourceSingleDocumentTransformation::DocumentSourceSingleDocumentTransformation(
    const intrusive_ptr<ExpressionContext>& pExpCtx,
    std::unique_ptr<TransformerInterface> parsedTransform,
    const StringData name,
    bool isIndependentOfAnyCollection)
    : DocumentSource(name, pExpCtx),
      _parsedTransform(std::move(parsedTransform)),
      _name(name.toString()),
      _isIndependentOfAnyCollection(isIndependentOfAnyCollection) {}

const char* DocumentSourceSingleDocumentTransformation::getSourceName() const {
    return _name.c_str();
}

DocumentSource::GetNextResult DocumentSourceSingleDocumentTransformation::doGetNext() {
    // Get the next input document.
    auto input = pSource->getNext();
    if (!input.isAdvanced()) {
        return input;
    }

    // Apply and return the document with added fields.
    return _parsedTransform->applyTransformation(input.releaseDocument());
}

intrusive_ptr<DocumentSource> DocumentSourceSingleDocumentTransformation::optimize() {
    _parsedTransform->optimize();
    return this;
}

void DocumentSourceSingleDocumentTransformation::doDispose() {
    if (_parsedTransform) {
        // Cache the stage options document in case this stage is serialized after disposing.
        _cachedStageOptions = _parsedTransform->serializeTransformation(pExpCtx->explain);
        _parsedTransform.reset();
    }
}

Value DocumentSourceSingleDocumentTransformation::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(Document{{getSourceName(),
                           _parsedTransform ? _parsedTransform->serializeTransformation(explain)
                                            : _cachedStageOptions}});
}

Pipeline::SourceContainer::iterator DocumentSourceSingleDocumentTransformation::doOptimizeAt(
    Pipeline::SourceContainer::iterator itr, Pipeline::SourceContainer* container) {
    invariant(*itr == this);

    if (std::next(itr) == container->end()) {
        return container->end();
    }

    auto nextSkip = dynamic_cast<DocumentSourceSkip*>((*std::next(itr)).get());

    if (nextSkip) {
        std::swap(*itr, *std::next(itr));
        return itr == container->begin() ? itr : std::prev(itr);
    }
    return std::next(itr);
}

DepsTracker::State DocumentSourceSingleDocumentTransformation::getDependencies(
    DepsTracker* deps) const {
    // Each parsed transformation is responsible for adding its own dependencies, and returning
    // the correct dependency return type for that transformation.
    return _parsedTransform->addDependencies(deps);
}

DocumentSource::GetModPathsReturn DocumentSourceSingleDocumentTransformation::getModifiedPaths()
    const {
    return _parsedTransform->getModifiedPaths();
}

DocumentSource::Sorts DocumentSourceSingleDocumentTransformation::getOutputSorts(
    Pipeline::SourceContainer::iterator begin, Pipeline::SourceContainer::iterator it) const {
    // Handle a few fast paths first:
    // 1. If we are the first pipeline stage then there is no previous stage to analyze.
    if (it == begin)
        return {};

    // 2. If this stage loses all sorting information, don't bother analyzing previous stages.
    auto mod = getModifiedPaths();
    if (mod.type !== DocumentSource::GetModPathsReturn::Type::kFiniteSet
        && mod.type !== cumentSource::GetModPathsReturn::Type::kAllExcept) {
        return {};
    }

    // Now handle the interesting case: we have a previous stage to analyze, and we know how to
    // preserve some sorting information from it.

    auto prev = std::prev(it);
    auto prevSorts = (*prev)->getOutputSorts(begin, prev);

    // Two different things can happen to a field when it passes through a stage like $set:
    // 1. We can lose all information about it.
    // 2. We can learn that its value is still available, under one or more new names.

    std::set<FieldPath> interestingPaths;
    for (auto sort : prevSorts.sorts) {
        for (auto part : sort) {
            if (auto fieldPath = part.fieldPath) {
                interestingPaths.insert(fieldPath);
            }
        }
    }

    // For each field path in interestingPaths, what happened to it?
    std::map<FieldPath, std::vector<FieldPath>> oldToNew;
    for (auto oldName : interestingPaths) {
        // whatHappenedTo() knows how to handle dotted prefixes: if the user renamed a -> b
        // and we ask whatHappenedTo() about a.b it will say b.b.
        std::vector<FieldPath> newNames = mod.whatHappenedTo(path);
        oldToNew[oldName]
    }


    auto mod = getModifiedPaths();
    switch (mod.type) {
        case DocumentSource::GetModPathsReturn::Type::kFiniteSet: {
            // If we modify only a finite set of paths, then each path in 'mod.paths' represents
            // a path that we've lost information about. Its value is not available under any new name.
            for (auto lostName : mod.paths) {
                oldToNew[lostName] = {};
            }
            // Each entry [new, old] in 'mod.paths' means two things:
            // 1. 'old' is available under a new name, 'new'.
            // 2. 'new' is lost, unless it was simultaneously renamed.
            for (auto [newName, oldName] : mod.renames) {
                oldToNew[oldName].push_back(newName);
            }
            for (auto [newName, oldName] : mod.renames) {
                if (oldToNew.find(newName) != oldToNew.end()) {
                    // 'new' was renamed, so it's not lost.
                } else {
                    // 'new' is overwritten and not renamed, so it's lost.
                    oldToNew[newName] = {};
                }
            }
            // Any paths not mentioned in the 'mod.paths' or 'mod.renames' are preserved.
            keepOther = true;
        } break;
        case DocumentSource::GetModPathsReturn::Type::kAllExcept: {
            // If we modify all but a finite set of paths, then each path in 'mod.paths' represents
            // a preserved path.
            for (auto keptName : mod.paths) {
                oldToNew[keptName] = {keptName};
            }
            // Each entry [new, old] in 'mod.paths' means two things:
            // 1. 'old' is available under a new name, 'new'.
            // 2. 'new' is lost, unless it was simultaneously renamed.
            // However, the second part happens implicitly, because every field not renamed or
            // preserved is lost.
            for (auto [newName, oldName] : mod.renames) {
                oldToNew[oldName].push_back(newName);
            }
            // Any paths not mentioned in the 'mod.paths' or 'mod.renames' are lost.
            keepOther = false;
        } break;
        // In all other cases, we don't have enough information to determine which sort orders are
        // preserved.
        default: return {};
    }

    auto prev = std::prev(it);
    auto prevSorts = (*prev)->getOutputSorts(begin, prev);
    return prevSorts.rename(oldToNew);
}

}  // namespace mongo
