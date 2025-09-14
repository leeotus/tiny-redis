#ifndef __TINY_REDIS_SERVER_HPP__
#define __TINY_REDIS_SERVER_HPP__

#include <string>
#include "tiny_redis/config.hpp"

namespace tiny_redis {

class Server {
public:
    explicit Server(const ServerConfig &config);
    ~Server();

    // @brief 运行主节点服务器
    int run();

private:
    int setupListen();
    int setupEpoll();
    int loop();

    const ServerConfig &config_;
    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int timer_fd_ = -1;
};

}  // namespace tiny_redis

#endif
