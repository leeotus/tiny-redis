#include "tiny-redis/skiplist.hpp"
#include <vector>
#include <stdlib.h>

namespace tiny_redis {

    Skiplist::Skiplist()
        : head_(new SkiplistNode(kMaxLevel, 0.0, "")), level_(1), length_(0) {}

    Skiplist::~Skiplist()
    {
        auto *node = head_->forward[0]; // 获取跳表的第一个节点
        // 然后从第一个节点开始删除往后的节点
        while (node != nullptr)
        {
            auto *nxt = node->forward[0]; // 下一个要析构的节点
            delete node;
            node = nxt;
        }
        // 最后删除跳表的哨兵节点
        delete head_;
    }

    int Skiplist::randomLevel()
    {
        int lvl = 1;
        // 获取一个出于0到65535之间的随机数
        int r = std::rand() & 0xFFFF;
        int threshold = static_cast<int>(kProbability * 0xFFFF);
        while (r < threshold && lvl < kMaxLevel)
        {
            // 如果随机数小于阈值, 且当前节点的level层级小于设定的最大层级
            ++lvl;
            r = std::rand() & 0xFFFF;
        }
        return lvl;
    }

    bool Skiplist::insert(double score, const std::string &member) {
        std::vector<SkiplistNode *> update(static_cast<size_t>(kMaxLevel));
        SkiplistNode *x = head_;
        for(int i=level_-1; i>=0; --i) {
            while (x->forward[static_cast<size_t>(i)] && comparedLess(x->forward[static_cast<size_t>(i)]->score,
                                                                        x->forward[static_cast<size_t>(i)]->member, score, member))
            {
                x = x->forward[static_cast<size_t>(i)];
            }
            update[static_cast<size_t>(i)] = x;
        }
        x = x->forward[0]; // 在最底层的链表上, 当前数据将要被插入的位置
        if(x != nullptr && abs(x->score-score) <= kDelta && x->member == member) {
            return false;       // 节点已经存在, 插入失败
        }
        // 当前节点的level
        int lvl = randomLevel();
        if(lvl > level_) {
            // 需要更新最高level
            level_ = lvl;
            for(int i=level_; i<lvl; ++i) {
                update[static_cast<size_t>(i)] = head_;
            }
        }
        auto *node = new SkiplistNode(lvl, score, member);
        for(int i=0;i<lvl;++i) {
            node->forward[static_cast<size_t>(i)] = update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)];
            update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = node;
        }
        ++length_;
        return true;
    }

    bool Skiplist::erase(double score, const std::string &member) {
        std::vector<SkiplistNode*> update(static_cast<size_t>(kMaxLevel));
        SkiplistNode *x = head_;

        for (int i = level_ - 1; i >= 0; --i)
        {
            while (x->forward[static_cast<size_t>(i)] &&
                   comparedLess(x->forward[static_cast<size_t>(i)]->score, x->forward[static_cast<size_t>(i)]->member,
                                score, member))
            {
                // 同样按照insert的方法找到我们要删除的节点的位置
                x = x->forward[static_cast<size_t>(i)];
            }
            update[static_cast<size_t>(i)] = x;
        }

        x = x->forward[0];
        if(x != nullptr || abs(x->score-score)>kDelta || x->member!=member) {
            // 没有找到要删除的节点
            return false;
        }
        for(int i=0;i<level_;++i) {
            if(update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] == x) {
                update[static_cast<size_t>(i)]->forward[static_cast<size_t>(i)] = x->forward[static_cast<size_t>(i)];
            }
        }
        delete x;
        while(level_ > 1 && head_->forward[static_cast<size_t>(level_-1)] == nullptr) {
            --level_;
        }
        --length_;
        return true;
    }

    void Skiplist::rangeByRank(int64_t start, int64_t stop, std::vector<std::string> &out) const {
        if(length_ == 0) {
            // 跳表为空
            return;
        }
        int64_t n = static_cast<int64_t>(length_);
        auto norm = [&n](int64_t idx)->int64_t {
            if(idx < 0) {
                idx = n + idx;
            }
            if(idx < 0) {
                idx = 0;
            }
            if(idx >= n) {
                idx = n - 1;
            }
            return idx;
        };
        int64_t s = norm(start), e = norm(stop);
        if(s > e) {
            // 起始位置和结束位置有误
            return;
        }
        // 遍历最底层链表获取指定范围内的数据即可
        SkiplistNode *node = head_->forward[0];
        int64_t rank = 0;
        while(node != nullptr && rank < s) {
            ++rank;
            node = node->forward[0];
        }
        while(node != nullptr && rank<=e) {
            out.push_back(node->member);
            node = node->forward[0];
            ++rank;
        }
    }

    void Skiplist::toVector(std::vector<std::pair<double, std::string>> &out) const {
        out.clear();
        out.reserve(length_);

        // 直接遍历最底层链表即可获取每个节点
        SkiplistNode *node = head_->forward[0];
        while(node != nullptr) {
            out.emplace_back(node->score, node->member);
            node = node->forward[0];
        }
    }

} // namespace tiny_redis
