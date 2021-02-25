/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "TossEnvironment.h"

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;

template <typename Response>
class StorageResponseReader {
public:
    explicit StorageResponseReader(StorageRpcResponse<Response>& resp) : resp_(&resp) {}

    bool isLeaderChange() {
        auto& c = resp_->failedParts();
        return std::any_of(c.begin(), c.end(), [](auto it){
            return it.second == cpp2::ErrorCode::E_LEADER_CHANGED;
        });
    }

    std::vector<Response>& data() {
        return resp_->responses();
    }

    cpp2::ErrorCode firstErrCode() {
        for (auto& p : resp_->failedParts()) {
            if (p.second != cpp2::ErrorCode::SUCCEEDED) {
                return p.second;
            }
        }
        return cpp2::ErrorCode::SUCCEEDED;
    }

private:
    StorageRpcResponse<Response>* resp_;
};

class GetNeighborsExecutor {
    std::vector<std::string> data_;

public:
    explicit GetNeighborsExecutor(cpp2::NewEdge edge)
        : GetNeighborsExecutor(std::vector<cpp2::NewEdge>{edge}) {
    }

    GetNeighborsExecutor(std::vector<cpp2::NewEdge> edges,
                         int64_t limit = std::numeric_limits<int64_t>::max())
        : edges_(std::move(edges)), limit_(limit) {
        env_ = TossEnvironment::getInstance();
        auto uniEdges = uniqueEdges(edges_);
        data_ = run(uniEdges);
    }

    std::vector<std::string> data() {
        return data_;
    }

    std::vector<cpp2::NewEdge> uniqueEdges(const std::vector<cpp2::NewEdge>& __edges) {
        std::vector<cpp2::NewEdge> edges(__edges);
        std::sort(edges.begin(), edges.end(), [](const auto& a, const auto& b) {
            if (a.key.src == b.key.src) {
                return a.key.dst < b.key.dst;
            }
            return a.key.src < b.key.src;
        });
        auto last = std::unique(edges.begin(), edges.end(), [](const auto& a, const auto& b) {
            return a.key.src == b.key.src && a.key.dst == b.key.dst;
        });
        edges.erase(last, edges.end());
        return edges;
    }

    /**
     * @brief Get the Nei Props object,
     *        will unique same src & dst input edges.
     */
    std::vector<std::string> run(const std::vector<cpp2::NewEdge>& edges) {
        bool retLeaderChange = false;
        LOG(INFO) << "edges.size()=" << edges.size() << ", edges.size()=" << edges.size();
        do {
            auto f = rpc(edges);
            f.wait();
            if (!f.valid()) {
                LOG(ERROR) << "!f.valid()";
                break;
            }
            if (f.value().succeeded()) {
                return readStorageResp(f.value());
            } else {
                LOG(ERROR) << "!f.value().succeeded()";
            }
            auto parts = f.value().failedParts();
            for (auto& part : parts) {
                if (part.second == cpp2::ErrorCode::E_LEADER_CHANGED) {
                    retLeaderChange = true;
                    break;
                }
            }
        } while (retLeaderChange);

        LOG(ERROR) << "getOutNeighborsProps failed";
        std::vector<std::string> ret;
        return ret;
    }

    folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>> rpc(
        const std::vector<cpp2::NewEdge>& edges) {
        // para3
        std::vector<Row> vertices;
        std::set<Value> vids;
        for (auto& e : edges) {
            // vids.insert(e.key.src);
            vids.insert(e.key.dst);
        }
        for (auto& vid : vids) {
            Row row;
            row.emplace_back(vid);
            vertices.emplace_back(row);
        }
        // para 4
        std::vector<EdgeType> edgeTypes;
        // para 5
        cpp2::EdgeDirection edgeDirection = cpp2::EdgeDirection::BOTH;
        // para 6
        std::vector<cpp2::StatProp>* statProps = nullptr;
        // para 7
        std::vector<cpp2::VertexProp>* vertexProps = nullptr;
        // para 8
        // const std::vector<cpp2::EdgeProp> edgeProps(1);
        std::vector<cpp2::EdgeProp> edgeProps(1);
        edgeProps.back().type = 0 - edges[0].key.edge_type;
        // para 9
        const std::vector<cpp2::Expr>* expressions = nullptr;
        // para 10
        bool dedup = false;
        // para 11
        bool random = false;
        // para 12
        const std::vector<cpp2::OrderBy> orderBy = std::vector<cpp2::OrderBy>();

        auto colNames = TossTestUtils::makeColNames(edges.back().props.size());

        return env_->sClient_->getNeighbors(env_->iSpaceId_,
                                            colNames,
                                            vertices,
                                            edgeTypes,
                                            edgeDirection,
                                            statProps,
                                            vertexProps,
                                            &edgeProps,
                                            expressions,
                                            dedup,
                                            random,
                                            orderBy,
                                            limit_);
    }

    std::vector<std::string> readStorageResp(StorageRpcResponse<cpp2::GetNeighborsResponse>& rpc) {
        std::vector<std::string> ret;
        LOG(INFO) << "rpc.responses().size()=" << rpc.responses().size();
        for (auto& resp : rpc.responses()) {
            auto sub = readGetNeighborsResp(resp);
            ret.insert(ret.end(), sub.begin(), sub.end());
        }
        return ret;
    }

    std::vector<std::string> readGetNeighborsResp(cpp2::GetNeighborsResponse& resp) {
        std::vector<std::string> ret;
        auto& ds = resp.vertices;
        LOG(INFO) << "ds.rows.size()=" << ds.rows.size();
        for (auto& row : ds.rows) {
            LOG(INFO) << "row.values.size()=" << row.values.size();
            for (auto& val : row.values) {
                LOG(INFO) << "row.val = " << val.toString();
            }
            ret.emplace_back(row.values[3].toString());
        }
        return ret;
    }

private:
    std::vector<cpp2::NewEdge> edges_;
    int64_t limit_{0};
    TossEnvironment* env_;
};

struct AddEdgeExecutor {
    TossEnvironment* env_;
    std::vector<cpp2::NewEdge> edges_;
    cpp2::ErrorCode code_;
    GraphSpaceID spaceId_;

public:
    explicit AddEdgeExecutor(cpp2::NewEdge edge, GraphSpaceID spaceId = 0) : spaceId_(spaceId) {
        env_ = TossEnvironment::getInstance();
        CHECK_GT(edge.key.edge_type, 0);
        LOG(INFO) << "src.type() = " << static_cast<int>(edge.key.src.type());
        edges_.push_back(edge);
        std::swap(edges_.back().key.src, edges_.back().key.dst);
        edges_.back().key.edge_type = 0 - edges_.back().key.edge_type;
        LOG(INFO) << "src.type() = " << static_cast<int>(edges_.back().key.src.type());
        if (spaceId == 0) {
            spaceId_ = env_->iSpaceId_;
        }
        rpc();
    }

    void rpc() {
        LOG(INFO) << "AddEdgeExecutor::rpc(), spaceId=" << spaceId_;
        auto propNames = TossTestUtils::makeColNames(edges_.back().props.size());
        bool overwritable = true;
        folly::EventBase* evb = nullptr;
        bool useToss = true;
        int retry = 10;
        do {
            LOG(INFO) << "AddEdgeExecutor::rpc(), do retry = " << retry;
            auto f = env_->sClient_->addEdges(
                spaceId_, edges_, propNames, overwritable, evb, useToss);
            f.wait();
            CHECK(f.hasValue());
            StorageResponseReader<cpp2::ExecResponse> reader(f.value());
            if (reader.isLeaderChange()) {
                continue;
            }
            code_ = reader.firstErrCode();
            break;
        } while (retry-- > 0);
    }

    bool ok() {
        return code_ == cpp2::ErrorCode::SUCCEEDED;
    }

    cpp2::ErrorCode code() {
        return code_;
    }
    // cpp2::ErrorCode syncAddMultiEdges(std::vector<cpp2::NewEdge>& edges, bool useToss) {
    //     bool retLeaderChange = false;
    //     int32_t retry = 0;
    //     int32_t retryMax = 10;
    //     do {
    //         if (retry > 0 && retry != retryMax) {
    //             LOG(INFO) << "\n leader changed, retry=" << retry;
    //             std::this_thread::sleep_for(std::chrono::milliseconds(500));
    //             retLeaderChange = false;
    //         }
    //         auto f = addEdgesAsync(edges, useToss);
    //         f.wait();
    //         if (!f.valid()) {
    //             LOG(INFO) << cpp2::_ErrorCode_VALUES_TO_NAMES.at(cpp2::ErrorCode::E_UNKNOWN);
    //             return cpp2::ErrorCode::E_UNKNOWN;
    //         }
    //         if (!f.value().succeeded()) {
    //             LOG(INFO) << "addEdgeAsync() !f.value().succeeded()";
    //             LOG(INFO) << "f.value().failedParts().size()=" << f.value().failedParts().size();
    //             for (auto& part : f.value().failedParts()) {
    //                 LOG(INFO) << "partId=" << part.first
    //                           << ", ec=" << cpp2::_ErrorCode_VALUES_TO_NAMES.at(part.second);
    //                 if (part.second == cpp2::ErrorCode::E_LEADER_CHANGED) {
    //                     retLeaderChange = true;
    //                 }
    //             }
    //         }

    //         std::vector<cpp2::ExecResponse>& execResps = f.value().responses();
    //         for (auto& execResp : execResps) {
    //             // ResponseCommon
    //             auto& respComn = execResp.result;
    //             auto& failedParts = respComn.get_failed_parts();
    //             for (auto& part : failedParts) {
    //                 if (part.code == cpp2::ErrorCode::E_LEADER_CHANGED) {
    //                     retLeaderChange = true;
    //                     LOG(INFO) << "addEdgeAsync() !f.value().succeeded(), retry";
    //                 }
    //             }
    //         }

    //         if (++retry == retryMax) {
    //             break;
    //         }
    //     } while (retLeaderChange);
    //     LOG(INFO) << "addEdgeAsync() succeeded";
    //     return cpp2::ErrorCode::SUCCEEDED;
    // }

    // cpp2::ErrorCode syncAddEdge(const cpp2::NewEdge& edge, bool useToss = true) {
    //     std::vector<cpp2::NewEdge> edges{edge};
    //     return syncAddMultiEdges(edges, useToss);
    // }

    // folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
    // addEdgesAsync(const std::vector<cpp2::NewEdge>& edges, bool useToss = true) {
    //     auto propNames = TossTestUtils::makeColNames(edges.back().props.size());
    //     return sClient_->addEdges(spaceId_, edges, propNames, true, nullptr, useToss);
    // }
};

struct GetPropsExecutor {
    TossEnvironment* env_;
    cpp2::NewEdge edge_;
    std::vector<Value> result_;

public:
    explicit GetPropsExecutor(cpp2::NewEdge edge) : edge_(std::move(edge)) {
        env_ = TossEnvironment::getInstance();
        CHECK_GT(edge_.key.edge_type, 0);
        std::swap(edge_.key.src, edge_.key.dst);
        edge_.key.edge_type = 0 - edge_.key.edge_type;
        rpc();
    }

    explicit GetPropsExecutor(std::vector<cpp2::NewEdge> edges) : GetPropsExecutor(edges[0]) {}

    std::vector<Value> data() {
        return result_;
    }

    void rpc() {
        nebula::Row row;
        CHECK_EQ(Value::Type::INT, edge_.key.src.type());
        row.values.emplace_back(edge_.key.src);
        row.values.emplace_back(edge_.key.edge_type);
        row.values.emplace_back(edge_.key.ranking);
        CHECK_EQ(Value::Type::INT, edge_.key.dst.type());
        row.values.emplace_back(edge_.key.dst);

        nebula::DataSet ds;
        ds.rows.emplace_back(std::move(row));

        std::vector<cpp2::EdgeProp> props(1);
        props.back().type = edge_.key.edge_type;

        int retry = 10;

        CHECK_EQ(Value::Type::INT, ds.rows[0].values[0].type());
        CHECK_EQ(Value::Type::INT, ds.rows[0].values[3].type());
        do {
            LOG(INFO) << "enter do{} while";
            auto frpc = env_->sClient_
                            ->getProps(env_->iSpaceId_,
                                       ds, /*DataSet*/
                                       nullptr,       /*vector<cpp2::VertexProp>*/
                                       &props,        /*vector<cpp2::EdgeProp>*/
                                       nullptr        /*expressions*/)
                            .via(env_->executor_.get());
            frpc.wait();
            CHECK(frpc.hasValue());
            StorageResponseReader<cpp2::GetPropResponse> reader(frpc.value());
            if (reader.isLeaderChange()) {
                continue;
            }

            auto resps = reader.data();
            CHECK_EQ(resps.size(), 1);
            cpp2::GetPropResponse& propResp = resps.front();
            cpp2::ResponseCommon result = propResp.get_result();
            nebula::DataSet& dataSet = propResp.props;
            std::vector<Row>& rows = dataSet.rows;
            if (rows.empty()) {
                LOG(FATAL) << "getProps() dataSet.rows.empty())";
            }
            LOG(INFO) << "rows.size() = " << rows.size();
            for (auto& r : rows) {
                LOG(INFO) << "values.size() = " << r.values.size();
                for (auto& val : r.values) {
                    LOG(INFO) << "val: " << val.toString();
                }
            }
            result_ = rows[0].values;
            if (result_.empty()) {
                LOG(FATAL) << "getProps() ret.empty())";
            }
            break;
        } while (retry-- > 0);
    }
};

struct UpdateExecutor {
    TossEnvironment* env_;
    cpp2::NewEdge edge_;
    std::vector<cpp2::UpdatedProp> updatedProps_;
    bool insertable_{false};
    std::vector<std::string> returnProps_;
    std::string condition_;

public:
    explicit UpdateExecutor(cpp2::NewEdge edge) : edge_(std::move(edge)) {
        env_ = TossEnvironment::getInstance();
        CHECK_GT(edge_.key.edge_type, 0);
        std::swap(edge_.key.src, edge_.key.dst);
        edge_.key.edge_type = 0 - edge_.key.edge_type;
        prepareParameters();
        rpc();
    }

    void prepareParameters() {
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("c1");
        ConstantExpression val1(edge_.props[0].getInt());
        uProp1.set_value(Expression::encode(val1));
        updatedProps_.emplace_back(uProp1);

        cpp2::UpdatedProp uProp2;
        uProp2.set_name("c2");
        ConstantExpression val2(edge_.props[1].getStr());
        uProp2.set_value(Expression::encode(val2));

        updatedProps_.emplace_back(uProp2);
    }

    void rpc() {
        for (;;) {
            auto f = env_->sClient_->updateEdge(
                env_->iSpaceId_, edge_.key, updatedProps_, insertable_, returnProps_, condition_);
            f.wait();
            CHECK(f.hasValue());
            if (!f.value().ok()) {
                LOG(FATAL) << f.value().status().toString();
            }
            storage::cpp2::UpdateResponse resp = f.value().value();
            if (resp.result.get_failed_parts().empty()) {
                break;
            }

            CHECK_EQ(resp.result.get_failed_parts().size(), 1U);
            cpp2::PartitionResult result =  resp.result.get_failed_parts().back();
            if (result.code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                LOG(INFO) << "update edge leader changed, retry";
                continue;
            }
            LOG(ERROR) << "update edge err: " << cpp2::_ErrorCode_VALUES_TO_NAMES.at(result.code);
            break;
        }
    }
};

}  // namespace storage
}  // namespace nebula
