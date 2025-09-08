#include "kv.hpp"
#include <memory>
#include <algorithm>
#include <mutex>
#include <chrono>

namespace tiny_redis
{

    bool tiny_redis::KeyValueStore::set(const std::string &key,
                                        const std::string &value,
                                        std::optional<int64_t> ttl_ms)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t expire_at = -1;
        if (ttl_ms.has_value())
        {
            expire_at = nowMs() + *ttl_ms;
        }
        map_[key] = ValueRecord{value, expire_at};
        if (expire_at >= 0)
        {
            expire_index_[key] = expire_at;
        }
        else
        {
            expire_index_.erase(key);
        }
        return true;
    }

    bool tiny_redis::KeyValueStore::setWithExpireAtMs(const std::string &key,
                                                      const std::string &value,
                                                      int64_t expire_at_ms)
    {
        std::lock_guard<std::mutex> lk(mu_);
        map_[key] = ValueRecord{value, expire_at_ms};
        if (expire_at_ms >= 0)
        {
            expire_index_[key] = expire_at_ms;
        }
        return true;
    }

    std::optional<std::string> KeyValueStore::get(const std::string &key) {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpired(key, now);
        auto it = map_.find(key);
        if(it == map_.end()) {
            return std::nullopt;
        }
        return it->second.value;
    }

    int KeyValueStore::del(const std::vector<std::string> &keys)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int removed = 0;  // 记录被删除的key的个数
        int64_t now = nowMs();
        for(const auto &k : keys) {
            cleanupIfExpired(k, now);
            auto it = map_.find(k);
            if(it != map_.end()) {
                map_.erase(it);
                expire_index_.erase(k);
                ++removed;
            }
        }
        return removed;
    }

    bool KeyValueStore::exists(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpired(key, now);
        return map_.find(key) != map_.end() || hmap_.find(key) != hmap_.end() || zmap_.find(key) != zmap_.end();
    }

    bool KeyValueStore::expire(const std::string &key, int64_t ttl_seconds)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpired(key, now);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return false;
        }
        if(ttl_seconds < 0) {
            // 设置key-value无过期时间
            it->second.expire_at_ms = -1;
            expire_index_.erase(key);
            return true;
        }
        // 延长过期时间, 以ms为精度
        it->second.expire_at_ms = now + ttl_seconds * 1000;
        expire_index_[key] = it->second.expire_at_ms;
        return true;
    }

    int64_t KeyValueStore::ttl(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpired(key, now);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return -2; // key不存在
        }
        if (it->second.expire_at_ms < 0) {
            return -1; // key值没有过期时间
        }
        int64_t ms_left = it->second.expire_at_ms - now;
        // 在执行上述操作的时候出现了过期:
        if(ms_left <= 0) {
            return -2;
        }
        return ms_left / 1000; // 以精度为秒返回剩余时间
    }

    int KeyValueStore::expireScanStep(int max_steps)
    {
        std::lock_guard<std::mutex> lk(mu_);
        if(max_steps <= 0 || expire_index_.empty()) return 0;
        int removed = 0;
        int64_t now = nowMs();
        auto it = expire_index_.begin();
        // 随机挑选开始删除的节点, 然后尝试删除至多max_steps的数据
        std::advance(it, static_cast<long>(std::rand() % expire_index_.size()));
        for(int i=0;i<max_steps && !expire_index_.empty(); ++i) {
            if(it == expire_index_.end()) it = expire_index_.begin();
            const std::string key = it->first;
            int64_t when = it->second;
            if(when >= 0 && now >= when) {
                map_.erase(key);
                hmap_.erase(key);
                zmap_.erase(key);
                it = expire_index_.erase(it);
                ++removed;
            } else {
                ++it;
            }
        }
        return removed;
    }

    std::vector<std::pair<std::string, ValueRecord>> KeyValueStore::snapshot() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::pair<std::string, ValueRecord>> out;
        out.reserve(map_.size());  // 预分配内存
        for(const auto &kv : map_) {
            out.emplace_back(kv.first, kv.second);
        }
        return out;
    }

    std::vector<std::pair<std::string, HashRecord>> KeyValueStore::snapshotHash() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::pair<std::string, HashRecord>> out;
        out.reserve(hmap_.size());
        for(const auto &kv : hmap_) {
            out.emplace_back(kv.first, kv.second);
        }
        return out;
    }

    std::vector<ZSetFlat> KeyValueStore::snapshotZSet() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<ZSetFlat> out;
        out.reserve(zmap_.size());
        for(const auto &kv : zmap_) {
            ZSetFlat flat;
            flat.key = kv.first;
            flat.expire_at_ms = kv.second.expire_at_ms;
            if(!kv.second.use_skiplist) {
                flat.items = kv.second.items; // 拷贝
            } else {
                // 如果是跳表, 那么将跳表转换为vector放入到结果集
                kv.second.sl->toVector(flat.items);
            }
            out.emplace_back(std::move(flat));
        }
    }

    std::vector<std::string> KeyValueStore::listKeys() const
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::string> out{};
        out.reserve(map_.size() + hmap_.size() + zmap_.size());
        for(const auto &kv : map_) {
            out.push_back(kv.first);
        }
        for(const auto &kv : hmap_) {
            out.push_back(kv.first);
        }
        for(const auto &kv : zmap_) {
            out.push_back(kv.first);
        }
        // 去重
        std::sort(out.begin(), out.end());

        /**
         * `std::unique(out.begin(), out.end())` 这个算法会遍历已排序的容器,
         * 将连续重复的元素移动到容器的末尾, 并返回一个指向第一个重复元素的迭代器
         * @note 该函数并不真正删除元素, 只是将重复元素移到后面
         */
        out.erase(std::unique(out.begin(), out.end()), out.end());
        return out;
    }

    int KeyValueStore::hset(const std::string &key, const std::string &field, const std::string &value)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredHash(key, now);
        auto &rec = hmap_[key];
        auto it = rec.fields.find(field);
        if(it == rec.fields.end()) {
            // 更新操作:
            rec.fields[field] = value;
            return 1;
        }
        it->second = value;
        return 0;
    }

    std::optional<std::string> KeyValueStore::hget(const std::string &key, const std::string &field)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredHash(key, now);
        auto it = hmap_.find(key);
        if (it == hmap_.end())
            return std::nullopt;
        auto itf = it->second.fields.find(field);
        if (itf == it->second.fields.end())
            return std::nullopt;
        return itf->second;
    }

    int KeyValueStore::hdel(const std::string &key, const std::vector<std::string> &fields)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredHash(key, now);
        auto it = hmap_.find(key);
        if (it == hmap_.end())
            return 0;
        int removed = 0;
        for (const auto &f : fields)
        {
            auto itf = it->second.fields.find(f);
            if (itf != it->second.fields.end())
            {
                it->second.fields.erase(itf);
                ++removed;
            }
        }
        if (it->second.fields.empty())
        {
            hmap_.erase(it);
        }
        return removed;
    }

    bool KeyValueStore::hexists(const std::string &key, const std::string &field)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredHash(key, now);
        auto it = hmap_.find(key);
        if(it == hmap_.end()) {
            return false;
        }
        return it->second.fields.find(field) != it->second.fields.end();
    }

    std::vector<std::string> KeyValueStore::hgetallFlat(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredHash(key, now);
        std::vector<std::string> out;
        auto it = hmap_.find(key);
        if (it == hmap_.end())
            return out;
        out.reserve(it->second.fields.size() * 2);
        // 存放格式: 一个key紧接着一个value
        for (const auto &kv : it->second.fields)
        {
            out.push_back(kv.first);
            out.push_back(kv.second);
        }
        return out;
    }

    int KeyValueStore::hlen(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredHash(key, now);
        auto it = hmap_.find(key);
        if(it == hmap_.end()) {
            return 0;
        }
        return static_cast<int>(it->second.fields.size());
    }

    bool KeyValueStore::setHashExpireAtMs(const std::string &key, int64_t expire_at_ms)
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = hmap_.find(key);
        if (it == hmap_.end()) {
            return false;
        }
        it->second.expire_at_ms = expire_at_ms;
        if(expire_at_ms >= 0) {
            expire_index_[key] = expire_at_ms;
        } else {
            expire_index_.erase(key);
        }
        return true;
    }

    int KeyValueStore::zadd(const std::string &key, double score, const std::string &member)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredZSet(key, now);
        auto &rec = zmap_[key];
        auto mit = rec.member_to_score.find(member);
        if (mit == rec.member_to_score.end())
        {
            if (!rec.use_skiplist)
            {
                // 沒有使用跳表
                auto &vec = rec.items;
                /**
                 * NOTE: `std::lower_bound`是一个二分查找法, 用于在保存的范围内查找第一个不小于给定值代元素
                 * 第三个参数传入代是要查找的数值, 其类型必须和`vec`内的元素类型一致(因此是`make_pair(...)`)
                 */
                auto it = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member), [](const auto &a, const auto &b)
                                           {
                    if(abs(a.first-b.first) > kDelta) {
                        return a.first < b.first;
                    }
                    return a.second < b.second; });
                vec.insert(it, std::make_pair(score, member));
                if (vec.size() > kZsetVectorThreshold)
                {
                    // 超过了只使用vector存储数据的阈值, 需要改用跳表
                    rec.use_skiplist = true;
                    rec.sl = std::make_unique<Skiplist>();
                    for (const auto &ptr : vec)
                    {
                        // TODO: RESEARCH: 是否要考虑使用渐进式迁移?
                        rec.sl->insert(ptr.first, ptr.second);
                    }
                    // 清理内存
                    // rec.items.clear();
                    // rec.items.shrink_to_fit();
                    std::vector<std::pair<double, std::string>>().swap(rec.items);
                }
            }
            else
            {
                // 使用跳表:
                rec.sl->insert(score, member);
            }
            rec.member_to_score.emplace(member, score);
            return 1;
        }
        else
        {
            // TODO: 如果数据已经在ZSet里存在, 同样需要考虑两种情况(使用跳表与否)
            double old = mit->second;
            if (old == score)
            {
                // 不需要做出修改
                return 0;
            }
            if (!rec.use_skiplist)
            {
                auto &vec = rec.items;
                for (auto vit = vec.begin(); vit != vec.end(); ++vit)
                {
                    if (vit->first == old && vit->second == member)
                    {
                        vec.erase(vit);
                        break;
                    }
                }
                auto it = std::lower_bound(vec.begin(), vec.end(), std::make_pair(score, member), [](const auto &a, const auto &b)
                                           {
                    if(abs(a.first-b.first) > kDelta) {
                        return a.first < b.first;
                    }
                    return a.second < b.second; });
                vec.insert(it, std::make_pair(score, member));
                if (vec.size() > kZsetVectorThreshold)
                {
                    rec.use_skiplist = true;
                    rec.sl = std::make_unique<Skiplist>();
                    for (const auto &pr : vec)
                    {
                        rec.sl->insert(pr.first, pr.second);
                    }
                    std::vector<std::pair<double, std::string>>().swap(rec.items);
                }
            }
            else
            {
                rec.sl->erase(old, member);
                rec.sl->insert(score, member);
            }
            mit->second = score;
            return 0;
        }
    }

    int KeyValueStore::zrem(const std::string &key, const std::vector<std::string> &members)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredZSet(key, now);
        auto it = zmap_.find(key);
        if (it == zmap_.end()) {
            return 0;
        }
        int removed = 0;
        for(const auto &m : members) {
            auto mit = it->second.member_to_score.find(m);
            if(mit == it->second.member_to_score.end()) {
                continue;
            }
            double sc = mit->second;
            it->second.member_to_score.erase(mit);
            if(!it->second.use_skiplist) {
                // 沒有使用跳表
                auto &vec = it->second.items;
                for(auto vit = vec.begin(); vit != vec.end(); ++vit) {
                    // O(n)复杂度, 只能在数据量少的时候采用
                    if(vit->first == sc && vit->second == m) {
                        vec.erase(vit);
                        ++removed;
                        break;
                    }
                }
            } else {
                // 使用的是跳表, 直接调用跳表的erase函数删除数据
                if(it->second.sl->erase(sc, m)) {
                    ++removed;
                }
            }
        }

        // 如果ZSet中没有数据存在了, 则直接删除整个ZSet
        if(!it->second.use_skiplist) {
            if(it->second.items.empty()) {
                zmap_.erase(it);
            }
        } else {
            if(it->second.sl->size() == 0) {
                zmap_.erase(it);
            }
        }
        return removed;
    }

    std::vector<std::string> KeyValueStore::zrange(const std::string &key, int64_t start, int64_t stop)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredZSet(key, now);
        std::vector<std::string> out{};

        auto it = zmap_.find(key);
        if(it == zmap_.end()) {
            // 没有找到对应的ZSet
            return out;
        }
        if(!it->second.use_skiplist) {
            // 没有使用跳表, 而是使用vector作为存储端(Redis7里使用的是listpack, 这里用的是vector替代)
            const auto &vec = it->second.items;
            int64_t n = static_cast<int64_t>(vec.size());
            if(n == 0) {
                // ZSet内没有数据
                return out;
            }
            auto norm = [&](int64_t idx) {
                if(idx < 0) {
                    idx = n+idx;
                }
                if(idx < 0) {
                    idx = 0;
                }
                if(idx >= n) {
                    idx = n-1;
                }
                return idx;
            };
            int64_t s = norm(start);
            int64_t e = norm(stop);
            if(s > e) {
                // 起始和终止范围不正确
                return out;
            }
            out.reserve(e-s);
            for(int64_t i=s; i<=e; ++i) {
                out.push_back(vec[static_cast<size_t>(i)].second);
            }
        } else {
            // 如果使用的是跳表作为存储端
            it->second.sl->rangeByRank(start, stop, out);
        }
        return out;
    }

    std::optional<double> KeyValueStore::zscore(const std::string &key, const std::string &member)
    {
        std::lock_guard<std::mutex> lk(mu_);
        int64_t now = nowMs();
        cleanupIfExpiredZSet(key, now);
        auto it = zmap_.find(key);
        if (it == zmap_.end()) {
            // 如果ZSet集合不存在key键
            return std::nullopt;
        }
        // 找到对应成员的分数:
        auto mit = it->second.member_to_score.find(member);
        if(mit == it->second.member_to_score.end()) {
            return std::nullopt;
        }
        return mit->second;
    }

    bool KeyValueStore::setZSetExpireAtMs(const std::string &key, int64_t expire_at_ms)
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = zmap_.find(key);
        if (it == zmap_.end()) {
            return false;
        }
        it->second.expire_at_ms = expire_at_ms;
        if(expire_at_ms >= 0) {
            expire_index_[key] = expire_at_ms;
        } else {
            expire_index_.erase(key);
        }
        return true;
    }

    int64_t tiny_redis::KeyValueStore::nowMs()
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    bool tiny_redis::KeyValueStore::isExpired(const ValueRecord &r,
                                              int64_t now_ms)
    {
        return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
    }

    bool tiny_redis::KeyValueStore::isExpired(const HashRecord &r, int64_t now_ms)
    {
        return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
    }

    bool tiny_redis::KeyValueStore::isExpired(const ZSetRecord &r, int64_t now_ms)
    {
        return r.expire_at_ms >= 0 && now_ms >= r.expire_at_ms;
    }

    void tiny_redis::KeyValueStore::cleanupIfExpired(const std::string &key,
                                                     int64_t now_ms)
    {
        auto it = map_.find(key);
        if (it == map_.end())
        {
            return;
        }
        if (isExpired(it->second, now_ms))
        {
            map_.erase(it);
            expire_index_.erase(key);
        }
    }

    void tiny_redis::KeyValueStore::cleanupIfExpiredHash(const std::string &key,
                                                         int64_t now_ms)
    {
        auto it = hmap_.find(key);
        if (it == hmap_.end())
        {
            return;
        }
        if (isExpired(it->second, now_ms))
        {
            hmap_.erase(it);
            expire_index_.erase(key);
        }
    }

    void tiny_redis::KeyValueStore::cleanupIfExpiredZSet(const std::string &key,
                                                         int64_t now_ms)
    {
        auto it = zmap_.find(key);
        if (it == zmap_.end())
            return;
        if (isExpired(it->second, now_ms))
        {
            zmap_.erase(it);
            expire_index_.erase(key);
        }
    }

} // namespace tiny_redis
