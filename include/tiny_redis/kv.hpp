/**
 * @file tiny_redis/kv.hpp
 * @author leeotus (leeotus@163.com)
 * @brief KV存储
 */
#ifndef __TINY_REDIS_KV_HPP__
#define __TINY_REDIS_KV_HPP__

#include <unordered_map>
#include <string>
#include <memory>
#include <vector>
#include <optional>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include "tiny_redis/skiplist.hpp"

namespace tiny_redis {

    struct ValueRecord {
        std::string value;
        int64_t expire_at_ms = -1;
    };

    struct HashRecord {
        std::unordered_map<std::string, std::string> fields;
        int64_t expire_at_ms = -1;
    };

    // @note: 小集合使用vector; 超过阈值使用skiplist跳表
    struct ZSetRecord {
        bool use_skiplist = false;
        std::vector<std::pair<double, std::string>> items; // 当use_skiplist=false
        std::unique_ptr<Skiplist> sl; // 当use_skiplist=true
        std::unordered_map<std::string, double> member_to_score;
        int64_t expire_at_ms = -1;
    };


    // @note 用于保存zmap_数据的快照结构体
    struct ZSetFlat
    {
        std::string key;
        std::vector<std::pair<double, std::string>> items;
        int64_t expire_at_ms;
    };

    class KeyValueStore {
    public:
        /**
         * @brief Redis设置key-value对
         * @param key 键
         * @param value 值
         * @param ttl_ms 过期时间, 单位ms
         * @return 设置成功返回true, 否则返回false
         */
        bool set(const std::string &key, const std::string &value, std::optional<int64_t> ttl_ms = std::nullopt);

        // @brief 同set函数, 只不过必须指定过期TTL时间戳, 以ms为精度
        bool setWithExpireAtMs(const std::string &key, const std::string &value, int64_t expire_at_ms);

        // @brief 获取key对应的value
        std::optional<std::string> get(const std::string &key);

        /**
         * @brief 删除kv存储里的数据
         * @param keys 要删除的key集合
         * @return int 返回成功被删除的key的个数
         */
        int del(const std::vector<std::string> &keys);

        // @brief 在Hash, ZSet中查看是否有key键值对存在
        bool exists(const std::string &key);

        /**
         * @brief 设置key-value对的数据的保存时间
         * @param key 需要重新设置时间的key
         * @param ttl_seconds 需要延长的过期时间
         * @return 设置成功返回true, 负责返回false
         */
        bool expire(const std::string &key, int64_t ttl_seconds);

        // @brief 获取key对应的过期时间, 以s为精度单位
        int64_t ttl(const std::string &key);

        size_t size() const { return map_.size(); }

        /**
         * @brief 至多删除max_steps个键值对
         * @return 返回被删除的个数
         * @note 定时扫描删除机制
         */
        int expireScanStep(int max_steps);

        // @brief 快照
        std::vector<std::pair<std::string, ValueRecord>> snapshot() const;
        std::vector<std::pair<std::string, HashRecord>> snapshotHash() const;

        std::vector<ZSetFlat> snapshotZSet() const;

        // @brief 获取所有的key, 保存到数组中返回
        std::vector<std::string> listKeys() const;

        // Hash APIs
        int hset(const std::string &key, const std::string &field, const std::string &value);
        std::optional<std::string> hget(const std::string &key, const std::string &field);
        int hdel(const std::string &key, const std::vector<std::string> &fields);
        bool hexists(const std::string &key, const std::string &field);
        // return flatten [field, value, field, value, ...]
        std::vector<std::string> hgetallFlat(const std::string &key);
        int hlen(const std::string &key);
        bool setHashExpireAtMs(const std::string &key, int64_t expire_at_ms);

        // ZSet APIs
        // returns number of new elements added
        int zadd(const std::string &key, double score, const std::string &member);
        // returns number of members removed
        int zrem(const std::string &key, const std::vector<std::string> &members);
        // return members between start and stop (inclusive), negative indexes allowed
        std::vector<std::string> zrange(const std::string &key, int64_t start, int64_t stop);

        // @brief 获取对应ZSet中key对应的数据的分数score
        std::optional<double> zscore(const std::string &key, const std::string &member);
        // @brief 设置ZSet中以key为关键字的排序数组的过期时间
        bool setZSetExpireAtMs(const std::string &key, int64_t expire_at_ms);

    private:
        std::unordered_map<std::string, ValueRecord> map_;
        std::unordered_map<std::string, ValueRecord>::iterator scan_it_ = map_.end();
        std::unordered_map<std::string, HashRecord> hmap_;
        std::unordered_map<std::string, HashRecord>::iterator hscan_it_ = hmap_.end();
        std::unordered_map<std::string, ZSetRecord> zmap_;
        std::unordered_map<std::string, ZSetRecord>::iterator zscan_it_ = zmap_.end();
        std::unordered_map<std::string, int64_t> expire_index_;
        mutable std::mutex mu_;

        // @brief 时间戳函数, 精度到ms级别
        static int64_t nowMs();

        // @brief 判断field-value pairs是否过期的系列函数
        static bool isExpired(const ValueRecord &r, int64_t now_ms);
        static bool isExpired(const HashRecord &r, int64_t now_ms); // Hash
        static bool isExpired(const ZSetRecord &r, int64_t now_ms); // ZSet

        // @brief 清理过期field-value pairs的系列函数
        void cleanupIfExpired(const std::string &key, int64_t now_ms);
        void cleanupIfExpiredHash(const std::string &key, int64_t now_ms); // Hash
        void cleanupIfExpiredZSet(const std::string &key, int64_t now_ms); // ZSet

        static constexpr size_t kZsetVectorThreshold = 128; // 使用ZSET的阈值
    };

    extern KeyValueStore g_store;  // 单例

} // namespace tiny_redis

#endif
