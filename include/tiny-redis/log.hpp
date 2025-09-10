#ifndef __TINY_REDIS_LOG_HPP__
#define __TINY_REDIS_LOG_HPP__

#include <chrono>
#include <ctime>
#include <iostream>
#include <string>

namespace tiny_redis
{

    // TODO: 当前简单完成一个logger, 之后可以考虑拓展
    // @brief 获取当前的时间
    inline std::string nowTime()
    {
        using namespace std::chrono;
        auto t = system_clock::to_time_t(system_clock::now()); // 获取当前系统的时间点
        char buf[64];
        std::strftime(buf, sizeof(buf), "%F %T", std::localtime(&t)); // 将时间格式化为字符串
        return std::string(buf);
    }

#define MR_LOG(level, msg)                                                           \
    do                                                                               \
    {                                                                                \
        std::cerr << "[" << nowTime() << "] [" << level << "] " << msg << std::endl; \
    } while (0)

} // namespace tiny_redis

#endif
