/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "TossTestUtils.h"
#include "common/meta/ServerBasedSchemaManager.h"

#define FLOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

DECLARE_int32(heartbeat_interval_secs);
class TossEnvironment;
class TestSpace;

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;

struct TossEnvironment {
    std::shared_ptr<folly::IOThreadPoolExecutor>        executor_;
    std::unique_ptr<meta::MetaClient>                   mClient_;
    std::unique_ptr<StorageClient>                      sClient_;
    std::unique_ptr<storage::InternalStorageClient>     iClient_;
    std::unique_ptr<meta::SchemaManager>                schemaMan_;

public:
    static TossEnvironment* getInstance() {
        static TossEnvironment inst;
        return &inst;
    }

    TossEnvironment() {
        executor_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
    }

    std::unique_ptr<meta::MetaClient>
    setupMetaClient(const std::string& metaName, uint32_t metaPort) {
        std::vector<HostAddr> metas;
        metas.emplace_back(HostAddr(metaName, metaPort));
        meta::MetaClientOptions options;
        auto client = std::make_unique<meta::MetaClient>(executor_, metas, options);
        if (!client->waitForMetadReady()) {
            LOG(FATAL) << "!client->waitForMetadReady()";
        }
        return client;
    }

    /*
     * setup space and edge type, then describe the cluster
     * */
    void init(const std::string& spaceName,
              size_t part,
              int replica,
              std::vector<meta::cpp2::ColumnDef>& colDefs) {
        if (spaceId_ != 0) {
            return;
        }
        spaceId_ = setupSpace(spaceName, part, replica);
        edgeType_ = setupEdgeSchema("test_edge", colDefs);

        int sleepSecs = FLAGS_heartbeat_interval_secs + 2;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        auto stVIdLen = mClient_->getSpaceVidLen(spaceId_);
        LOG_IF(FATAL, !stVIdLen.ok());
        vIdLen_ = stVIdLen.value();

        bool leaderLoaded = false;
        while (!leaderLoaded) {
            auto statusOrLeaderMap = mClient_->getLeaderInfo();
            if (!statusOrLeaderMap.ok()) {
                LOG(FATAL) << "mClient_->loadLeader() failed!!!!!!";
            }

            auto leaderMap = statusOrLeaderMap.value().leaderMap_;
            for (auto& leader : leaderMap) {
                LOG(INFO) << "spaceId=" << leader.first.first
                            << ", part=" << leader.first.second
                            << ", host=" << leader.second;
                if (leader.first.first == spaceId_) {
                    leaderLoaded = true;
                }
            }
        }
    }
};

struct TestSpace {
    TossEnvironment* env_;
    int32_t spaceId_{};
    int32_t edgeType_{};
    int32_t vIdLen_{0};
    std::string edgeName_;
    bool useToss_;

    meta::MetaClient*                   mClient_;
    StorageClient*                      sClient_;
    InternalStorageClient*              iClient_;

public:
    TestSpace(TossEnvironment* env,
              const std::string& spaceName,
              int nPart,
              int nReplica,
              meta::cpp2::PropertyType vidType,
              const std::string& edgeName,
              std::vector<meta::cpp2::PropertyType> colTypes,
              bool useToss = true) : env_(env), edgeName_(edgeName), useToss_(useToss) {
        mClient_ = env->mClient_.get();
        sClient_ = env->sClient_.get();
        iClient_ = env->iClient_.get();

        spaceId_ = createSpace(spaceName, nPart, nReplica, vidType);
        auto colDefs = TossTestUtils::makeColDefs(colTypes);
        edgeType_ = setupEdgeSchema(colDefs);

        waitLeaderCollection();
    }

    int createSpace(const std::string& spaceName,
                    int nPart,
                    int nReplica,
                    meta::cpp2::PropertyType vidType) {
        LOG(INFO) << "TossEnvironment::createSpace()";
        auto fDropSpace = env_->mClient_->dropSpace(spaceName, true);
        fDropSpace.wait();
        LOG(INFO) << "drop space " << spaceName;

        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name(spaceName);
        spaceDesc.set_partition_num(nPart);
        spaceDesc.set_replica_factor(nReplica);
        meta::cpp2::ColumnTypeDef colType;

        colType.set_type(vidType);
        spaceDesc.set_vid_type(colType);
        auto lv = useToss_ ? meta::cpp2::IsolationLevel::TOSS : meta::cpp2::IsolationLevel::DEFAULT;
        spaceDesc.set_isolation_level(lv);

        auto fCreateSpace = env_->mClient_->createSpace(spaceDesc, true);
        fCreateSpace.wait();
        if (!fCreateSpace.valid()) {
            LOG(FATAL) << "!fCreateSpace.valid()";
        }
        if (!fCreateSpace.value().ok()) {
            LOG(FATAL) << "!fCreateSpace.value().ok(): "
                       << fCreateSpace.value().status().toString();
        }

        return fCreateSpace.value().value();
    }

    EdgeType setupEdgeSchema(std::vector<meta::cpp2::ColumnDef> columns) {
        meta::cpp2::Schema schema;
        schema.set_columns(std::move(columns));

        auto fCreateEdgeSchema = mClient_->createEdgeSchema(spaceId_, edgeName_, schema, true);
        fCreateEdgeSchema.wait();

        if (!fCreateEdgeSchema.valid() || !fCreateEdgeSchema.value().ok()) {
            LOG(FATAL) << "createEdgeSchema failed";
        }
        return fCreateEdgeSchema.value().value();
    }

    void waitLeaderCollection() {
        int sleepSecs = FLAGS_heartbeat_interval_secs + 2;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        return newEdge;
    }

    cpp2::NewEdge reverseEdge(const cpp2::NewEdge& e0) {
        cpp2::NewEdge e(e0);
        std::swap(*(*e.key_ref()).src_ref(), *(*e.key_ref()).dst_ref());
        (*e.key_ref()).set_edge_type(-e.get_key().get_edge_type());
        return e;
    cpp2::ErrorCode syncAddMultiEdges(std::vector<cpp2::NewEdge>& edges, bool useToss) {
        bool retLeaderChange = false;
        int32_t retry = 0;
        int32_t retryMax = 10;
        do {
            if (retry > 0 && retry != retryMax) {
                LOG(INFO) << "\n leader changed, retry=" << retry;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                retLeaderChange = false;
            }
            auto f = addEdgesAsync(edges, useToss);
            f.wait();
            if (!f.valid()) {
                auto retcode = nebula::cpp2::ErrorCode::E_UNKNOWN;
                LOG(INFO) << apache::thrift::util::enumNameSafe(retcode);
                return retcode;
            }
            if (!f.value().succeeded()) {
                LOG(INFO) << "addEdgeAsync() !f.value().succeeded()";
                LOG(INFO) << "f.value().failedParts().size()=" << f.value().failedParts().size();
                for (auto& part : f.value().failedParts()) {
                    LOG(INFO) << "partId=" << part.first
                              << ", ec=" << apache::thrift::util::enumNameSafe(part.second);
                    if (part.second == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
                        retLeaderChange = true;
                    }
                }
            }

            std::vector<cpp2::ExecResponse>& execResps = f.value().responses();
            for (auto& execResp : execResps) {
                // ResponseCommon
                auto& respComn = execResp.get_result();
                auto& failedParts = respComn.get_failed_parts();
                for (auto& part : failedParts) {
                    if (part.code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
                        retLeaderChange = true;
                        LOG(INFO) << "addEdgeAsync() !f.value().succeeded(), retry";
                    }
                }
            }


            if (++retry == retryMax) {
                break;
            }
        } while (retLeaderChange);
        LOG(INFO) << "addEdgeAsync() succeeded";
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    nebula::cpp2::ErrorCode syncAddEdge(const cpp2::NewEdge& edge, bool useToss = true) {
        std::vector<cpp2::NewEdge> edges{edge};
        return syncAddMultiEdges(edges, useToss);
    }

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
    addEdgesAsync(const std::vector<cpp2::NewEdge>& edges, bool useToss = true) {
        auto propNames = makeColNames(edges.back().get_props().size());
        return sClient_->addEdges(spaceId_, edges, propNames, true, nullptr, useToss);
    }

    static std::vector<std::string> makeColNames(size_t n) {
        std::vector<std::string> colNames;
        for (auto i = 0U; i < n; ++i) {
            colNames.emplace_back(folly::sformat("c{}", i+1));
    }

    std::string ConsistUtil::edgeKey(const cpp2::EdgeKey& e) {
        auto edgeKey = TossTestUtils::makeEdgeKeyS(e);
        auto partId = getPartId(edgeKey.src.getStr());
        return TransactionUtils::edgeKey(vIdLen_, partId, edgeKey);
    }

    static std::vector<std::string> extractNeiProps(cpp2::GetNeighborsResponse& resp) {
        std::vector<std::string> ret;
        auto& ds = *resp.vertices_ref();
        LOG(INFO) << "ds.rows.size()=" << ds.rows.size();
        for (auto& row : ds.rows) {
            if (row.values.size() < 4) {
                continue;
            }
            LOG(INFO) << "row.values.size()=" << row.values.size();
            ret.emplace_back(row.values[3].toString());
        }
        return ret;
    }
    }

    std::string edgeKeyPrime(const cpp2::EdgeKey& e) {
        auto edgeKey = TossTestUtils::makeEdgeKeyS(e);
        auto partId = getPartId(edgeKey.src.getStr());
        return TransactionUtils::edgeKeyPrime(vIdLen_, partId, edgeKey);
    }

    std::string strLockKey(const cpp2::EdgeKey& e) {
        CHECK_GT(e.edge_type, 0);
        auto key = strInEdgeKey(e);
        return NebulaKeyUtils::toLockKey(key);
    }

    cpp2::EdgeKey reverseEdgeKey(const cpp2::EdgeKey& key) {
        cpp2::EdgeKey ret(key);
        std::swap(ret.src, ret.dst);
        ret.edge_type = 0 - ret.edge_type;
        return ret;
    }

    std::set<std::string> extractStrVals(const std::vector<std::string>& svec) {
        auto len = 36;
        std::set<std::string> strVals;
        for (auto& e : svec) {
            strVals.insert(e.substr(e.size() - len));
        }
        // std::sort(strVals.begin(), strVals.end());
        return strVals;
    }
    std::vector<std::string> diffProps(std::vector<std::string> actual,
                                       std::vector<std::string> expect) {
        std::sort(actual.begin(), actual.end());
        std::sort(expect.begin(), expect.end());
        std::vector<std::string> diff;

        std::set_difference(actual.begin(), actual.end(), expect.begin(), expect.end(),
                            std::inserter(diff, diff.begin()));
        return diff;
    }

    cpp2::NewEdge dupEdge(const cpp2::NewEdge& e) {
        cpp2::NewEdge dupEdge{e};
        int n = e.get_props()[0].getInt() / 1024 + 1;
        dupEdge.set_props(TossTestUtils::genSingleVal(n));
        return dupEdge;
    }

    /**
     * @brief gen num edges base from src
     *        dst shoule begin from [src + 1, src + num + 1)
     * @param extraSameKey
     *        if set, gen another edge, has the same src & dst will always equal to src + 1
     */
    std::vector<cpp2::NewEdge> generateMultiEdges(int num, int64_t src, bool extraSameKey = false) {
        LOG_IF(FATAL, num <= 0) << "num must > 0";
        num += static_cast<int>(extraSameKey);
        auto vals = TossTestUtils::genValues(num);
        std::vector<cpp2::NewEdge> edges;
        for (int i = 0; i < num; ++i) {
            auto dst = extraSameKey ? 1 : src + i + 1;
            edges.emplace_back(generateEdge(src, 0, vals[i], dst));
            auto keyPair = makeRawKey(edges.back().get_key());
            LOG(INFO) << "gen key=" << folly::hexlify(keyPair.first)
                      << ", val=" << edges.back().get_props().back().toString();
        }
        return edges;
    }

    void insertMutliLocks(const std::vector<cpp2::NewEdge>& edges) {
        UNUSED(edges);
        // auto lockKey = ;
    }

    int32_t getPartId(const std::string& src) {
        // auto stPart = mClient_->partId(spaceId_, edgeKey.src.getStr());
        auto stPart = mClient_->partId(spaceId_, src);
        LOG_IF(FATAL, !stPart.ok()) << "mClient_->partId failed";
        return stPart.value();
    }

    /**
     * @brief gen rawkey and partId from EdgeKey
     * @return std::pair<std::string, int32_t> rawkey vs partId
     */
    std::pair<std::string, int32_t> makeRawKey(const cpp2::EdgeKey& e) {
        auto edgeKey = TossTestUtils::toVidKey(e);
        auto partId = getPartId(edgeKey.get_src().getStr());

        auto rawKey = TransactionUtils::edgeKey(vIdLen_, partId, edgeKey);
        return std::make_pair(rawKey, partId);
    }

    std::string encodeProps(const cpp2::NewEdge& e) {
        auto edgeType = e.get_key().get_edge_type();
        auto pSchema = schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType)).get();
        LOG_IF(FATAL, !pSchema) << "Space " << spaceId_ << ", Edge " << edgeType << " invalid";
        auto propNames = makeColNames(e.get_props().size());
        return encodeRowVal(pSchema, propNames, e.get_props());
    }

    std::string insertEdge(const cpp2::NewEdge& e) {
        std::string rawKey;
        int32_t partId = 0;
        std::tie(rawKey, partId) = makeRawKey(e.get_key());
        auto encodedProps = encodeProps(e);
        putValue(rawKey, encodedProps, partId);
        return rawKey;
    }

    cpp2::EdgeKey reverseEdgeKey(const cpp2::EdgeKey& input) {
        cpp2::EdgeKey ret(input);
        std::swap(*ret.src_ref(), *ret.dst_ref());
        ret.set_edge_type(-ret.get_edge_type());
        return ret;
    }

    std::string insertReverseEdge(const cpp2::NewEdge& _e) {
        cpp2::NewEdge e(_e);
        e.set_key(reverseEdgeKey(_e.get_key()));
        return insertEdge(e);
    }

        return putValue(key, val, part);
    }


    std::string insertValidLock(const cpp2::NewEdge& e) {
        CHECK_GT(e.key.edge_type, 0);
        insertOutEdge(e);
        return insertLock(e);
        auto key = strOutEdgeKey(e.key);

        auto part = NebulaKeyUtils::getPart(key);
        return putValue(key, val, part);
    }

    std::string insertBiEdge(const cpp2::NewEdge& e) {
        insertInEdge(e);
        return insertOutEdge(e);
    }

    std::string putValue(std::string key, std::string val, int32_t partId) {
        LOG(INFO) << "put value, partId=" << partId << ", key=" << folly::hexlify(key);
        kvstore::BatchHolder bat;
        bat.put(std::string(key), std::move(val));
        auto batch = encodeBatchValue(bat.getBatch());

        auto txnId = 0;
        auto sf = iClient_->forwardTransaction(txnId, spaceId_, partId, std::move(batch));
        sf.wait();

        if (sf.value() != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(FATAL) << "forward txn return=" << apache::thrift::util::enumNameSafe(sf.value());
        }
        return key;
    }

    bool keyExist(folly::StringPiece key) {
        auto sf = iClient_->getValue(vIdLen_, spaceId_, key);
        sf.wait();
        if (!sf.hasValue()) {
            LOG(FATAL) << "iClient_->getValue has no value";
            return false;
        }
        return nebula::ok(sf.value());
    }

    bool outEdgeExist(const cpp2::NewEdge& e) {
        LOG(INFO) << "check outEdgeExist: " << folly::hexlify(strOutEdgeKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strOutEdgeKey(e.key));
    }

    bool inEdgeExist(const cpp2::NewEdge& e) {
        LOG(INFO) << "check inEdgeExist: " << folly::hexlify(strInEdgeKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strInEdgeKey(e.key));
    }

    bool lockExist(const cpp2::NewEdge& e) {
        LOG(INFO) << "check lockExist: " << folly::hexlify(strLockKey(e.key));
        CHECK_GT(e.key.edge_type, 0);
        return keyExist(strLockKey(e.key));
    }

    // simple copy of Storage::BaseProcessor::encodeRowVal
    std::string encodeRowVal(const meta::NebulaSchemaProvider* schema,
                             const std::vector<std::string>& propNames,
                             const std::vector<Value>& props) {
        RowWriterV2 rowWrite(schema);
        WriteResult wRet;
        if (!propNames.empty()) {
            for (size_t i = 0; i < propNames.size(); i++) {
                wRet = rowWrite.setValue(propNames[i], props[i]);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(FATAL) << "Add field faild";
                }
            }
        } else {
            for (size_t i = 0; i < props.size(); i++) {
                wRet = rowWrite.setValue(i, props[i]);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(FATAL) << "Add field faild";
                }
            }
        }
        wRet = rowWrite.finish();
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(FATAL) << "Add field faild";
        }

        return std::move(rowWrite).moveEncodedStr();
    }
};

}  // namespace storage
}  // namespace nebula
