// Pub/sub demo: Generator, Distributor, ClientA, ClientB — each on its own thread.
//
// The generator publishes a heap object (A or B) once a second. The distributor
// thread routes that pointer to the matching client queue. Each client thread
// pops its queue and calls print().
//
// Build:
//   cmake -S . -B build && cmake --build build
//   build\Debug\pubsub.exe 8          (MSVC)
//   build/pubsub 8                    (Ninja / Make)
// Or:
//   g++ -std=c++17 -pthread -O2 -o pubsub main.cpp && ./pubsub 8

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

namespace {

std::mutex g_io;

enum class Kind { A, B };

// Polymorphic payload. The generator hands out unique ownership of one of these.
class Message {
public:
    explicit Message(std::string name) : name_(std::move(name)) {}
    virtual ~Message() = default;

    Message(const Message&) = delete;
    Message& operator=(const Message&) = delete;

    virtual void print() const = 0;
    virtual Kind kind() const = 0;

    const std::string& name() const { return name_; }

protected:
    std::string name_;
};

class A final : public Message {
public:
    explicit A(std::string name) : Message(std::move(name)) {}

    void print() const override { std::cout << "A: " << name_ << std::endl; }

    Kind kind() const override { return Kind::A; }
};

class B final : public Message {
public:
    explicit B(std::string name) : Message(std::move(name)) {}

    void print() const override { std::cout << "B: " << name_ << std::endl; }

    Kind kind() const override { return Kind::B; }
};

// Thread-safe queue. pop() blocks until an item arrives or the queue is closed
// and drained. After close(), further pushes drop the item.
template <typename T>
class BlockingQueue {
public:
    BlockingQueue() = default;

    BlockingQueue(const BlockingQueue&) = delete;
    BlockingQueue& operator=(const BlockingQueue&) = delete;

    void push(T item) {
        {
            std::lock_guard lock(mutex_);
            if (closed_) {
                return;
            }
            queue_.push(std::move(item));
        }
        cv_.notify_one();
    }

    // Returns false when the queue is closed and empty.
    bool pop(T& out) {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) {
            return false;
        }
        out = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void close() {
        {
            std::lock_guard lock(mutex_);
            closed_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<T> queue_;
    bool closed_ = false;
};

class Distributor {
public:
    using Inbox = BlockingQueue<std::unique_ptr<Message>>;

    Distributor() = default;
    Distributor(const Distributor&) = delete;
    Distributor& operator=(const Distributor&) = delete;

    void subscribe(Kind kind, Inbox* queue) {
        std::lock_guard lock(subs_mu_);
        const auto [it, inserted] = subs_.emplace(kind, queue);
        if (!inserted) {
            throw std::logic_error("this kind already has a subscriber");
        }
        (void)it;
    }

    // Called from the generator thread. Takes ownership of the pointer.
    void publish(std::unique_ptr<Message> message) {
        incoming_.push(std::move(message));
    }

    void start() { thread_ = std::thread([this] { run(); }); }

    // Unblocks the distributor thread after it finishes messages already queued.
    void shutdown() { incoming_.close(); }

    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

private:
    void run() {
        {
            std::lock_guard lock(g_io);
            std::cout << "[Distributor] thread " << std::this_thread::get_id() << std::endl;
        }

        std::unique_ptr<Message> message;
        while (incoming_.pop(message)) {
            route(std::move(message));
        }

        // Clients block on their own queues; close them so their threads can exit.
        std::lock_guard lock(subs_mu_);
        for (auto& entry : subs_) {
            entry.second->close();
        }
    }

    void route(std::unique_ptr<Message> message) {
        Inbox* target = nullptr;
        {
            std::lock_guard lock(subs_mu_);
            const auto it = subs_.find(message->kind());
            if (it != subs_.end()) {
                target = it->second;
            }
        }

        {
            std::lock_guard lock(g_io);
            std::cout << "[Distributor] route "
                      << (message->kind() == Kind::A ? "A" : "B")
                      << " '" << message->name() << "'" << std::endl;
        }

        if (target == nullptr) {
            return;
        }
        target->push(std::move(message));
    }

    Inbox incoming_;
    std::mutex subs_mu_;
    std::unordered_map<Kind, Inbox*> subs_;
    std::thread thread_;
};

class Client {
public:
    Client(std::string label, Distributor& distributor, Kind kind)
        : label_(std::move(label)) {
        distributor.subscribe(kind, &inbox_);
    }

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    void start() { thread_ = std::thread([this] { run(); }); }

    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

private:
    void run() {
        {
            std::lock_guard lock(g_io);
            std::cout << "[" << label_ << "] thread " << std::this_thread::get_id() << std::endl;
        }

        std::unique_ptr<Message> message;
        while (inbox_.pop(message)) {
            std::lock_guard lock(g_io);
            std::cout << "[" << label_ << "] ";
            message->print();
        }
    }

    std::string label_;
    Distributor::Inbox inbox_;
    std::thread thread_;
};

class Generator {
public:
    explicit Generator(Distributor& distributor) : distributor_(distributor) {}

    Generator(const Generator&) = delete;
    Generator& operator=(const Generator&) = delete;

    void start() { thread_ = std::thread([this] { run(); }); }

    void stop() {
        {
            std::lock_guard lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
    }

    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

private:
    void run() {
        {
            std::lock_guard lock(g_io);
            std::cout << "[Generator] thread " << std::this_thread::get_id() << std::endl;
        }

        std::mt19937 rng{std::random_device{}()};
        std::uniform_int_distribution<int> pick(0, 1);
        int seq = 0;

        for (;;) {
            {
                std::lock_guard lock(mu_);
                if (stop_) {
                    return;
                }
            }

            std::unique_ptr<Message> message;
            const std::string name = std::to_string(++seq);
            if (pick(rng) == 0) {
                message = std::make_unique<A>(name);
            } else {
                message = std::make_unique<B>(name);
            }

            {
                std::lock_guard lock(g_io);
                std::cout << "[Generator] publish ";
                message->print();
            }
            distributor_.publish(std::move(message));

            std::unique_lock lock(mu_);
            if (cv_.wait_for(lock, std::chrono::seconds(1), [this] { return stop_; })) {
                return;
            }
        }
    }

    Distributor& distributor_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_ = false;
    std::thread thread_;
};

int run_seconds(int argc, char** argv) {
    if (argc < 2) {
        return 10;
    }
    const int seconds = std::atoi(argv[1]);
    return seconds > 0 ? seconds : 10;
}

}  // namespace

int main(int argc, char** argv) {
    const int seconds = run_seconds(argc, argv);

    Distributor distributor;
    Client client_a("ClientA", distributor, Kind::A);
    Client client_b("ClientB", distributor, Kind::B);
    Generator generator(distributor);

    distributor.start();
    client_a.start();
    client_b.start();
    generator.start();

    {
        std::lock_guard lock(g_io);
        std::cout << "Running for " << seconds
                  << "s. Pass a different duration as the first argument." << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::seconds(seconds));

    generator.stop();
    generator.join();
    distributor.shutdown();
    distributor.join();
    client_a.join();
    client_b.join();

    std::cout << "Stopped." << std::endl;
    return 0;
}
