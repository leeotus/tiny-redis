#ifndef __TINY_REDIS_AOF_HPP__
#define __TINY_REDIS_AOF_HPP__

#include <atomic>
#include <optional>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <chrono>
#include "tiny_redis/config.hpp"

namespace tiny_redis {

    class KeyValueStore;

    // @brief AOF日志类, 用于写入tiny-redis运行过程中执行的命令
    class AofLogger {
    public:
        AofLogger() = default;
        ~AofLogger();

        // @brief AOF初始化
        bool init(const AofOptions &opts, std::string &err);

        // @brief 停止AOF记录
        void shutdown();

        // @brief 加载已有的AOF文件
        bool load(KeyValueStore &store, std::string &err);

        // @brief 往AOF里记录命令
        bool appendCommand(const std::vector<std::string> &parts);

        // @brief 往AOF记录原生的RESP协议命令
        bool appendRaw(const std::string &raw_resp);

        bool isEnabled() const { return opts_.enabled; }
        AofMode mode() const { return opts_.mode; }

        // @brief 写入的AOF文件的路径
        std::string path() const;

        bool bgRewrite(KeyValueStore &store, std::string &err);

    private:
        int fd_ = -1;
        AofOptions opts_;
        std::atomic<bool> running_{false};
        int timer_fd_ = -1;

        struct AofItem {
            std::string data;
            int64_t seq;
        };

        std::thread writer_thread_;
        std::mutex mtx_;
        std::condition_variable cv_;
        std::condition_variable cv_commit_;
        std::deque<AofItem> queue_;
        std::atomic<bool> stop_{false};
        size_t pending_bytes_ = 0;
        std::chrono::steady_clock::time_point last_sync_tp_{std::chrono::steady_clock::now()};
        std::atomic<int64_t> seq_gen_{0};
        int64_t last_synced_seq_ = 0;

        std::atomic<bool> rewriting_{false};
        std::thread rewriter_thread_;
        std::mutex incr_mtx_;
        std::vector<std::string> incr_cmds_;

        std::atomic<bool> pause_writer_{false};
        std::mutex pause_mtx_;
        std::condition_variable cv_pause_;
        bool writer_is_paused_ = false;

        void writerLoop();
        void rewriterLoop(KeyValueStore *store);
    };

    std::string toRespArray(const std::vector<std::string> &parts);
} // namespace tiny_redis

#endif
