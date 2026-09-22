#ifndef CHAT_FRAME_CONFIG_H
#define CHAT_FRAME_CONFIG_H
#include <cstdlib>
#include <stdexcept>
#include <string>
namespace frame {
inline std::string Env(const char* key, const char* fallback) {
    const char* value = std::getenv(key); return value ? value : fallback;
}
inline int EnvInt(const char* key, int fallback, int minimum, int maximum) {
    const char* text = std::getenv(key);
    if (!text) return fallback;
    std::size_t used = 0;
    int value;
    try { value = std::stoi(text, &used); }
    catch (...) { throw std::invalid_argument(std::string("Invalid setting: ") + key); }
    if (used != std::string(text).size() || value < minimum || value > maximum)
        throw std::invalid_argument(std::string("Invalid setting: ") + key);
    return value;
}
}
#endif
