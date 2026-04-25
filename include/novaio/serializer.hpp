#pragma once

#include <vector>
#include <string>
#include <tuple>
#include <type_traits>
#include <cstddef>

namespace novaio {

template <typename T>
inline void append_bytes(std::vector<char>& buffer, const T& value) {
    const char* ptr = reinterpret_cast<const char*>(&value);
    buffer.insert(buffer.end(), ptr, ptr + sizeof(T));
}

template <typename T>
void serialize(const T& obj, std::vector<char>& buffer);

template <typename Tuple, std::size_t... Is>
void serialize_tuple_impl(const Tuple& t, std::vector<char>& buffer, std::index_sequence<Is...>) {
    (serialize(std::get<Is>(t), buffer),...);
}

template <typename T>
void serialize(const T& obj, std::vector<char>& buffer) {
    if constexpr (std::is_arithmetic_v<T>) {
        append_bytes(buffer, obj);
    } 
    else if constexpr (std::is_same_v<T, std::string>) {
        size_t len = obj.size();
        append_bytes(buffer, len);
        buffer.insert(buffer.end(), obj.begin(), obj.end());
    } 
    else {
        auto tuple_view = obj.tie();
        serialize_tuple_impl(tuple_view, buffer, 
            std::make_index_sequence<std::tuple_size_v<decltype(tuple_view)>>{});
    }
}

} // namespace novaio