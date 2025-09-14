#ifndef __TINY_REDIS_RDB_HPP__
#define __TINY_REDIS_RDB_HPP__

#include <string>
#include "tiny_redis/config.hpp"

namespace tiny_redis {

    class KeyValueStore;

    class Rdb {
    public:
        Rdb() = default;
        ~Rdb() = default;

        explicit Rdb(const RdbOptions &opts) : opts_(opts) {}
        void setOptions(const RdbOptions &opts) { opts_ = opts; }

        /**
         * @brief 将KV存储里的所有数据保存到RDB文件中
         * @param store kv存储数据库
         * @param err 错误信息, 如果保存错误则将信息写入到err返回
         * @return 写入成功返回true, 失败返回false
         */
        bool save(const KeyValueStore &store, std::string &err) const;

        /**
         * @brief 加载RDB文件的信息到kv存储数据库中
         * @param store 需要加载数据的kv存储数据库
         * @param err 如果出现错误则通过该参数返回
         * @return 记载成功返回true, 否则返回false
         */
        bool load(KeyValueStore &store, std::string &err) const;

        // @brief 返回RDB文件保存的路径
        std::string path() const;

    private:
        // RDB配置信息
        RdbOptions opts_{};
    };

}   // namespace tiny_redis

#endif
