/**
 *    Copyright 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/repl/replica_set_config.h"

#include <algorithm>

#include "mongo/bson/util/bson_check.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/jsobj.h"
#include "mongo/stdx/functional.h"

namespace mongo {
namespace repl {

#ifndef _MSC_VER
    const size_t ReplicaSetConfig::kMaxMembers;
    const size_t ReplicaSetConfig::kMaxVotingMembers;
#endif

    const Seconds ReplicaSetConfig::kDefaultHeartbeatTimeoutPeriod(10);

namespace {

    const std::string kIdFieldName = "_id";
    const std::string kVersionFieldName = "version";
    const std::string kMembersFieldName = "members";
    const std::string kSettingsFieldName = "settings";

    const std::string kLegalConfigTopFieldNames[] = {
        kIdFieldName,
        kVersionFieldName,
        kMembersFieldName,
        kSettingsFieldName
    };

    const std::string kHeartbeatTimeoutFieldName = "heartbeatTimeoutSecs";
    const std::string kChainingAllowedFieldName = "chainingAllowed";
    const std::string kGetLastErrorDefaultsFieldName = "getLastErrorDefaults";
    const std::string kGetLastErrorModesFieldName = "getLastErrorModes";

}  // namespace

    ReplicaSetConfig::ReplicaSetConfig() : _heartbeatTimeoutPeriod(0) {}

    Status ReplicaSetConfig::initialize(const BSONObj& cfg) {
        _members.clear();
        Status status = bsonCheckOnlyHasFields(
                "replica set configuration", cfg, kLegalConfigTopFieldNames);
        if (!status.isOK())
            return status;

        //
        // Parse replSetName
        //
        status = bsonExtractStringField(cfg, kIdFieldName, &_replSetName);
        if (!status.isOK())
            return status;

        //
        // Parse version
        //
        status = bsonExtractIntegerField(cfg, kVersionFieldName, &_version);
        if (!status.isOK())
            return status;

        //
        // Parse members
        //
        BSONElement membersElement;
        status = bsonExtractTypedField(cfg, kMembersFieldName, Array, &membersElement);
        if (!status.isOK())
            return status;

        for (BSONObj::iterator membersIterator(membersElement.Obj()); membersIterator.more();) {
            BSONElement memberElement = membersIterator.next();
            if (memberElement.type() != Object) {
                return Status(ErrorCodes::TypeMismatch, str::stream() <<
                              "Expected type of " << kMembersFieldName << "." <<
                              memberElement.fieldName() << " to be Object, but found " <<
                              typeName(memberElement.type()));
            }
            _members.resize(_members.size() + 1);
            status = _members.back().initialize(memberElement.Obj(), &_tagConfig);
            if (!status.isOK())
                return status;
        }

        //
        // Parse settings
        //
        BSONElement settingsElement;
        status = bsonExtractTypedField(cfg, kSettingsFieldName, Object, &settingsElement);
        BSONObj settings;
        if (status.isOK()) {
            settings = settingsElement.Obj();
        }
        else if (status != ErrorCodes::NoSuchKey) {
            return status;
        }
        status = _parseSettingsSubdocument(settings);
        if (!status.isOK())
            return status;

        _calculateMajorityNumber();
        return Status::OK();
    }

    Status ReplicaSetConfig::_parseSettingsSubdocument(const BSONObj& settings) {
        //
        // Parse heartbeatTimeoutSecs
        //
        BSONElement hbTimeoutSecsElement = settings[kHeartbeatTimeoutFieldName];
        if (hbTimeoutSecsElement.eoo()) {
            _heartbeatTimeoutPeriod = Seconds(kDefaultHeartbeatTimeoutPeriod);
        }
        else if (hbTimeoutSecsElement.isNumber()) {
            _heartbeatTimeoutPeriod = Seconds(hbTimeoutSecsElement.numberInt());
        }
        else {
            return Status(ErrorCodes::TypeMismatch, str::stream() << "Expected type of " <<
                          kSettingsFieldName << "." << kHeartbeatTimeoutFieldName <<
                          " to be a number, but found a value of type " <<
                          typeName(hbTimeoutSecsElement.type()));
        }

        //
        // Parse chainingAllowed
        //
        Status status = bsonExtractBooleanFieldWithDefault(settings,
                                                           kChainingAllowedFieldName,
                                                           true,
                                                           &_chainingAllowed);
        if (!status.isOK())
            return status;

        //
        // Parse getLastErrorDefaults
        //
        BSONElement gleDefaultsElement;
        status = bsonExtractTypedField(settings,
                                       kGetLastErrorDefaultsFieldName,
                                       Object,
                                       &gleDefaultsElement);
        if (status.isOK()) {
            status = _defaultWriteConcern.parse(gleDefaultsElement.Obj());
            if (!status.isOK())
                return status;
        }
        else if (status == ErrorCodes::NoSuchKey) {
            // Default write concern is w: 1.
            _defaultWriteConcern.reset();
            _defaultWriteConcern.wNumNodes = 1;
        }
        else {
            return status;
        }

        //
        // Parse getLastErrorModes
        //
        BSONElement gleModesElement;
        status = bsonExtractTypedField(settings,
                                       kGetLastErrorModesFieldName,
                                       Object,
                                       &gleModesElement);
        BSONObj gleModes;
        if (status.isOK()) {
            gleModes = gleModesElement.Obj();
        }
        else if (status != ErrorCodes::NoSuchKey) {
            return status;
        }

        for (BSONObj::iterator gleModeIter(gleModes); gleModeIter.more();) {
            const BSONElement modeElement = gleModeIter.next();
            if (_customWriteConcernModes.find(modeElement.fieldNameStringData()) !=
                _customWriteConcernModes.end()) {

                return Status(ErrorCodes::DuplicateKey, str::stream() << kSettingsFieldName <<
                              '.' << kGetLastErrorModesFieldName <<
                              " contains multiple fields named " << modeElement.fieldName());
            }
            if (modeElement.type() != Object) {
                return Status(ErrorCodes::TypeMismatch, str::stream() << "Expected " <<
                              kSettingsFieldName << '.' << kGetLastErrorModesFieldName << '.' <<
                              modeElement.fieldName() << " to be an Object, not " <<
                              typeName(modeElement.type()));
            }
            ReplicaSetTagPattern pattern = _tagConfig.makePattern();
            for (BSONObj::iterator constraintIter(modeElement.Obj()); constraintIter.more();) {
                const BSONElement constraintElement = constraintIter.next();
                if (!constraintElement.isNumber()) {
                    return Status(ErrorCodes::TypeMismatch, str::stream() << "Expected " <<
                                  kSettingsFieldName << '.' << kGetLastErrorModesFieldName << '.' <<
                                  modeElement.fieldName() << '.' << constraintElement.fieldName() <<
                                  " to be a number, not " << typeName(constraintElement.type()));
                }
                const int minCount = constraintElement.numberInt();
                if (minCount <= 0) {
                    return Status(ErrorCodes::BadValue, str::stream() << "Value of " <<
                                  kSettingsFieldName << '.' << kGetLastErrorModesFieldName << '.' <<
                                  modeElement.fieldName() << '.' << constraintElement.fieldName() <<
                                  " must be positive, but found " << minCount);
                }
                status = _tagConfig.addTagCountConstraintToPattern(
                        &pattern,
                        constraintElement.fieldNameStringData(),
                        minCount);
                if (!status.isOK()) {
                    return status;
                }
            }
            _customWriteConcernModes[modeElement.fieldNameStringData()] = pattern;
        }
        return Status::OK();
    }

    Status ReplicaSetConfig::validate() const {
        if (_version <= 0 || _version > std::numeric_limits<int>::max()) {
            return Status(ErrorCodes::BadValue, str::stream() << kVersionFieldName <<
                          " field value of " << _version << " is out of range");
        }
        if (_replSetName.empty()) {
            return Status(ErrorCodes::BadValue, str::stream() <<
                          "Replica set configuration must have non-empty " << kIdFieldName <<
                          " field");
        }
        if (_heartbeatTimeoutPeriod < Seconds(0)) {
            return Status(ErrorCodes::BadValue, str::stream() << kSettingsFieldName << '.' <<
                          kHeartbeatTimeoutFieldName << " field value must be non-negative, "
                          "but found " << _heartbeatTimeoutPeriod.total_seconds());
        }
        if (_members.size() > kMaxMembers || _members.empty()) {
            return Status(ErrorCodes::BadValue, str::stream() <<
                          "Replica set configuration contains " << _members.size() <<
                          " members, but must have at least 1 and no more than  " << kMaxMembers);
        }

        size_t localhostCount = 0;
        size_t voterCount = 0;
        size_t arbiterCount = 0;
        size_t electableCount = 0;
        for (size_t i = 0; i < _members.size(); ++i) {
            const MemberConfig& memberI = _members[i];
            Status status = memberI.validate();
            if (!status.isOK())
                return status;
            if (memberI.getHostAndPort().isLocalHost()) {
                ++localhostCount;
            }
            if (memberI.isVoter()) {
                ++voterCount;
            }
            // Nodes may be arbiters or electable, or neither, but never both.
            if (memberI.isArbiter()) {
                ++arbiterCount;
            }
            else if (memberI.getPriority() > 0) {
                ++electableCount;
            }
            for (size_t j = 0; j < _members.size(); ++j) {
                if (i == j)
                    continue;
                const MemberConfig& memberJ = _members[j];
                if (memberI.getId() == memberJ.getId()) {
                    return Status(
                            ErrorCodes::BadValue, str::stream() <<
                            "Found two member configurations with same " <<
                            MemberConfig::kIdFieldName << " field, " <<
                            kMembersFieldName << "." << i << "." << MemberConfig::kIdFieldName <<
                            " == " <<
                            kMembersFieldName << "." << j << "." << MemberConfig::kIdFieldName <<
                            " == " << memberI.getId());
                }
                if (memberI.getHostAndPort() == memberJ.getHostAndPort()) {
                    return Status(
                            ErrorCodes::BadValue, str::stream() <<
                            "Found two member configurations with same " << 
                            MemberConfig::kHostFieldName << " field, " <<
                            kMembersFieldName << "." << i << "." << MemberConfig::kHostFieldName <<
                            " == " <<
                            kMembersFieldName << "." << j << "." << MemberConfig::kHostFieldName <<
                            " == " << memberI.getHostAndPort().toString());
                }
            }
        }

        if (localhostCount != 0 && localhostCount != _members.size()) {
            return Status(ErrorCodes::BadValue, str::stream() <<
                          "Either all host names in a replica set configuration must be localhost "
                          "references, or none must be; found " << localhostCount << " out of " <<
                          _members.size());
        }

        if (voterCount > kMaxVotingMembers || voterCount == 0) {
            return Status(ErrorCodes::BadValue, str::stream() <<
                          "Replica set configuration contains " << voterCount <<
                          " voting members, but must be between 0 and " << kMaxVotingMembers);
        }

        if (electableCount == 0) {
            return Status(ErrorCodes::BadValue, "Replica set configuration must contain at least "
                          "one non-arbiter member with priority > 0");
        }

        // TODO(schwerin): Validate satisfiability of write modes? Omitting for backwards
        // compatibility.
        if (!_defaultWriteConcern.wMode.empty() && "majority" != _defaultWriteConcern.wMode) {
            if (!findCustomWriteMode(_defaultWriteConcern.wMode).isOK()) {
                return Status(ErrorCodes::BadValue, str::stream() <<
                              "Default write concern requires undefined write mode " <<
                              _defaultWriteConcern.wMode);
            }
        }

        return Status::OK();
    }

    ReplicaSetTag ReplicaSetConfig::findTag(const StringData& key, const StringData& value) const {
        return _tagConfig.findTag(key, value);
    }

    StatusWith<ReplicaSetTagPattern> ReplicaSetConfig::findCustomWriteMode(
            const StringData& patternName) const {

        const StringMap<ReplicaSetTagPattern>::const_iterator iter = _customWriteConcernModes.find(
                patternName);
        if (iter == _customWriteConcernModes.end()) {
            return StatusWith<ReplicaSetTagPattern>(
                    ErrorCodes::NoSuchKey,
                    str::stream() <<
                    "No write concern mode named \"" << escape(patternName.toString()) <<
                    " found in replica set configuration");
        }
        return StatusWith<ReplicaSetTagPattern>(iter->second);
    }

    void ReplicaSetConfig::_calculateMajorityNumber() {
        const size_t total = getNumMembers();
        const size_t strictMajority = total/2+1;
        const size_t nonArbiters = total - std::count_if(
                _members.begin(),
                _members.end(),
                stdx::bind(&MemberConfig::isArbiter, stdx::placeholders::_1));

        // majority should be all "normal" members if we have something like 4
        // arbiters & 3 normal members
        //
        // TODO(SERVER-14403): Should majority exclude hidden nodes? non-voting nodes? unelectable
        // nodes?
        _majorityNumber = (strictMajority > nonArbiters) ? nonArbiters : strictMajority;
    }

}  // namespace repl
}  // namespace mongo
