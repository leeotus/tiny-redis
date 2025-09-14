/**
 * @file tiny_redis/replica_client.hpp
 * @brief TinyRedis从节点
 */

#ifndef __TINY_REDIS_REPLICA_CLIENT_HPP__
#define __TINY_REDIS_REPLICA_CLIENT_HPP__

#include <string>
#include <thread>

#include "tiny_redis/config.hpp"

namespace tiny_redis {

    class ReplicaClient {
    public:
        explicit ReplicaClient(const ServerConfig &cfg);
        ~ReplicaClient();
        void start();
        void stop();

    private:
        void threadMain();

        const ServerConfig &cfg_;
        std::thread th_;
        bool running_ = false;
        int64_t last_offset_ = 0;       // 从节点维护的复制偏移量
    };

}   // namespace tiny_redis

#endif
