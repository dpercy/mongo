// rollbacktests.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */



#include "mongo/bson/bsonobj.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/dbtests/dbtests.h"
#include "mongo/unittest/unittest.h"

using std::list;
using std::string;

namespace RollbackTests {

    namespace {
        bool doesCollectionExist( Client::Context* ctx, const string& ns ) {
            const DatabaseCatalogEntry* dbEntry = ctx->db()->getDatabaseCatalogEntry();
            list<string> names;
            dbEntry->getCollectionNamespaces( &names );
            return std::find( names.begin(), names.end(), ns ) != names.end();
        }
    }

    class RollbackCreateCollection {
    public:
        void run() {
            string ns = "unittests.rollback_create_collection";
            OperationContextImpl txn;
            NamespaceString nss( ns );

            Lock::DBLock dbXLock( txn.lockState(), nss.db(), MODE_X );
            Client::Context ctx( &txn, ns );
            {
                WriteUnitOfWork uow( &txn );
                ASSERT( !doesCollectionExist( &ctx, ns ) );
                ASSERT_OK( userCreateNS( &txn, ctx.db(), ns, BSONObj(), false ) );
                ASSERT( doesCollectionExist( &ctx, ns ) );
                // no commit
            }
            ASSERT( !doesCollectionExist( &ctx, ns ) );
        }
    };

    class RollbackDropCollection {
    public:
        void run() {
            string ns = "unittests.rollback_drop_collection";
            OperationContextImpl txn;
            Client::WriteContext ctx( &txn, ns );

            {
                WriteUnitOfWork uow( &txn );
                ASSERT( !doesCollectionExist( &ctx, ns ) );
                ASSERT_OK( userCreateNS( &txn, ctx.db(), ns, BSONObj(), false ) );
                uow.commit();
            }
            ASSERT( doesCollectionExist( &ctx, ns ) );

            {
                WriteUnitOfWork uow( &txn );
                ASSERT( doesCollectionExist( &ctx, ns ) );
                ASSERT_OK( ctx.db()->dropCollection( &txn, ns ) );
                ASSERT( !doesCollectionExist( &ctx, ns ) );
                // no commit
            }
            ASSERT( doesCollectionExist( &ctx, ns ) );
        }
    };

    class RollbackRenameCollection {
    public:
        void run() {
            string nsSrc  = "unittests.rollback_rename_collection_src";
            string nsDest = "unittests.rollback_rename_collection_dest";
            OperationContextImpl txn;
            Lock::GlobalWrite globalWriteLock(txn->lockState());

            {
                WriteUnitOfWork uow( &txn );
                ASSERT( !doesCollectionExist( &ctx, ns ) );
                ASSERT_OK( userCreateNS( &txn, ctx.db(), ns, BSONObj(), false ) );
                uow.commit();
            }
            ASSERT( doesCollectionExist( &ctx, ns ) );

            {
                WriteUnitOfWork uow( &txn );
                ASSERT( doesCollectionExist( &ctx, ns ) );
                ASSERT_OK( ctx.db()->dropCollection( &txn, ns ) );
                ASSERT( !doesCollectionExist( &ctx, ns ) );
                // no commit
            }
            ASSERT( doesCollectionExist( &ctx, ns ) );
        }
    };

    class All : public Suite {
    public:
        All() : Suite( "rollback" ) {
        }

        void setupTests() {
            add< RollbackCreateCollection >();
            add< RollbackRenameCollection >();
            add< RollbackDropCollection >();
        }
    } myall;

} // namespace RollbackTests

