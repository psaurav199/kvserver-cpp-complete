#pragma once
#include <unordered_map>
#include <list>
#include <string>
#include <mutex>

// Simple thread-safe LRU cache for string->string
class LRUCache {
public:
    explicit LRUCache(size_t capacity) : cap_(capacity) {
        if (cap_ == 0) cap_ = 1;
    }

    bool get(const std::string& key, std::string& out) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = map_.find(key);
        if (it == map_.end()) return false;
        // move to front
        order_.splice(order_.begin(), order_, it->second);
        out = it->second->second;
        return true;
    }

    void set(const std::string& key, const std::string& val) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second->second = val;
            order_.splice(order_.begin(), order_, it->second);
            return;
        }
        order_.emplace_front(key, val);
        map_[key] = order_.begin();
        if (map_.size() > cap_) {
            auto& back = order_.back();
            map_.erase(back.first);
            order_.pop_back();
        }
    }

    void erase(const std::string& key) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = map_.find(key);
        if (it == map_.end()) return;
        order_.erase(it->second);
        map_.erase(it);
    }

    size_t size() const {
        std::lock_guard<std::mutex> lk(mu_);
        return map_.size();
    }

private:
    size_t cap_;
    mutable std::mutex mu_;
    std::list<std::pair<std::string,std::string>> order_;
    std::unordered_map<std::string, decltype(order_.begin())> map_;
};
