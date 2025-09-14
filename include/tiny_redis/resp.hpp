/**
 * @file tiny_redis/resp.hpp
 * @author leeotus (leeotus@163.com)
 * @brief Redis的RESP2通信协议 (Redis Serialization Protocol)
 */
#ifndef __TINY_REDIS_RESP_HPP__
#define __TINY_REDIS_RESP_HPP__

#include <string>
#include <vector>
#include <string_view>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

namespace tiny_redis {

enum class RespType {
    kSimpleString,  // `简单字符串`, 第一个字符响应`+`
    kError,         // `错误`, 第一个字节响应`-`
    kInteger,       // `整型`, 第一个字节响应`:`
    kBulkString,    // `批量字符串`, 第一个字节响应`$`
    kArray,         // `数组`, 第一个字节响应`*`
    kNull,
};

struct RespValue {
    RespType type = RespType::kNull;
    std::string bulk;
    std::vector<RespValue> array;
};

class RespParser {
public:
    void append(std::string_view data);
    std::optional<RespValue> tryParseOne();
    std::optional<std::pair<RespValue, std::string>> tryParseOneWithRaw();
private:
    std::string buffer_;

    bool parseLine(size_t &pos, std::string &out_line);

    // @brief 解析一个整型
    bool parseInteger(size_t &pos, int64_t &out_value);

    // @brief 解析一个批量字符串
    bool parseBulkString(size_t &pos, RespValue& out);

    // @brief 解析一个简单字符串
    bool parseSimple(size_t &pos, RespType t, RespValue &out);

    // @brief 解析一个数组
    bool parseArray(size_t &pos, RespValue &out);
};

std::string respSimpleString(std::string_view s);
std::string respError(std::string_view s);
std::string respBulk(std::string_view s);
std::string respNullBulk();
std::string respInteger(int64_t v);

} // namespace tiny_redis

#endif
