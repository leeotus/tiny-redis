#ifndef __TINY_REDIS_SKIP_LIST_HPP__
#define __TINY_REDIS_SKIP_LIST_HPP__

#include <string>
#include <vector>
#include <stdlib.h>

namespace tiny_redis {

    static constexpr double kDelta = 0.000001;

// @brief 跳表节点
typedef struct _skiplist_node {
    double score;  // 跳表节点的分数
    std::string member; // 跳表节点保存的数据
    std::vector<struct _skiplist_node *> forward; // 指向后面不同层级的节点

    /**
     * @brief Construct a new skiplist node object
     * 跳表节点的构造函数
     * @param level 当前跳表节点所在的level
     * @param sc 当前跳表节点的score分数
     * @param mem 保存在当前节点的数据
     */
    _skiplist_node(int level, double sc, const std::string &mem)
        : score(sc), member(mem), forward(static_cast<size_t>(level), nullptr) {
    }
} SkiplistNode;

class Skiplist
{
public:
    Skiplist();
    ~Skiplist();

    // 不允许浅拷贝, 如果有需求应该重写拷贝函数做深拷贝
    Skiplist(const Skiplist &) = delete;
    Skiplist &operator=(const Skiplist &) = delete;

    /**
     * @brief 插入节点到跳表中
     * @param score 节点的分数
     * @param member 节点保存的数据
     * @return 插入失败返回false, 否则返回true
     * @note 插入失败的一种情况是score和member都一致的节点在跳表中已经存在
     */
    bool insert(double score, const std::string &member);

    // @brief 按照分数删除跳表里的某个节点
    bool erase(double score, const std::string &member);

    /**
     * @brief 获取范围在start和stop之间的跳表节点的数据
     * @param out 获取的结果保存在该数组中并返回
     */
    void rangeByRank(int64_t start, int64_t stop, std::vector<std::string> &out) const;

    /**
     * @brief 将跳表中保存的数据全部转换为数组并返回
     * @param out 返回的数据, 里面每个元素都是pair<double, string>, first为节点的分数, string为节点的数据
     */
    void toVector(std::vector<std::pair<double, std::string>> &out) const;

    size_t size() const { return length_; }

private:
    static constexpr int kMaxLevel = 32;
    static constexpr double kProbability = 0.25;

    // @brief 用于概率设置节点的level, 返回节点的level
    int randomLevel();

    size_t length_; // 跳表的长度
    int level_;     // 保存当前跳表的最大level

    // TODO: 改成智能指针
    SkiplistNode *head_; // 哨兵节点

    static constexpr auto comparedLess = [](double sc1, const std::string &m1,
                                            double sc2,
                                            const std::string &m2) -> bool
    {
        if (abs(sc1 - sc2) <= kDelta)
        {
            return m1 < m2;
        }
        return sc1 < sc2;
    };
};

} // namespace tiny_redis

#endif
