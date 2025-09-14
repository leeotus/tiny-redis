#include <iostream>
#include <chrono>
#include <csignal>
#include <cstring>
#include <string>
#include <thread>

#include "tiny_redis/server.hpp"
#include "tiny_redis/config.hpp"
#include "tiny_redis/config_loader.hpp"
#include "tiny_redis/aof.hpp"
#include "tiny_redis/rdb.hpp"

namespace tiny_redis
{

    static volatile std::sig_atomic_t g_should_stop = 0;

    void handle_signal(int signum)
    {
        (void)signum;
        g_should_stop = 1;
    }

    void print_usage(const char *argv0)
    {
        std::cout << "mini-redis usage:\n"
                  << "  " << argv0 << " [--port <port>] [--bind <ip>] [--config <file>]" << std::endl;
    }

    bool parse_args(int argc, char **argv, ServerConfig &out_config)
    {
        for (int i = 1; i < argc; ++i)
        {
            const std::string arg = argv[i];
            if (arg == "--port" && i + 1 < argc)
            {
                out_config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
            }
            else if (arg == "--bind" && i + 1 < argc)
            {
                out_config.bind_address = argv[++i];
            }
            else if (arg == "--config" && i + 1 < argc)
            {
                std::string file = argv[++i];
                std::string err;
                if (!tiny_redis::loadConfigFromFile(file, out_config, err))
                {
                    std::cerr << err << std::endl;
                    return false;
                }
            }
            else if (arg == "-h" || arg == "--help")
            {
                print_usage(argv[0]);
                return false;
            }
            else
            {
                std::cerr << "Unknown argument: " << arg << std::endl;
                print_usage(argv[0]);
                return false;
            }
        }
        return true;
    }

    int run_server(const ServerConfig &config)
    {
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);
        tiny_redis::Server srv(config);
        return srv.run();
    }

} // namespace tiny_redis

int main(int argc, char **argv) {
    tiny_redis::ServerConfig config;
    if(!tiny_redis::parse_args(argc, argv, config)) {
        return 1;
    }
    return tiny_redis::run_server(config);
}
