#ifndef __TINY_REDIS_CONFIG_LOADER_HPP__
#define __TINY_REDIS_CONFIG_LOADER_HPP__

#include <string>
#include "tiny_redis/config.hpp"

namespace tiny_redis {

    bool loadConfigFromFile(const std::string &path, ServerConfig &cfg, std::string &err);

}  // namespace tiny_redis

#endif
