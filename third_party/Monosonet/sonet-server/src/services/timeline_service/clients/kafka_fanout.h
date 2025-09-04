#pragma once

#include <string>
#include <vector>
#include <memory>

namespace sonet {
namespace timeline {
namespace fanout {

class FanoutPublisher {
public:
    virtual ~FanoutPublisher() = default;
    virtual void PublishNewNote(const std::string& author_id, const std::string& note_id) = 0;
};

class FanoutConsumer {
public:
    virtual ~FanoutConsumer() = default;
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

class StubFanoutPublisher : public FanoutPublisher {
public:
    void PublishNewNote(const std::string& /*author_id*/, const std::string& /*note_id*/) override {}
};

class StubFanoutConsumer : public FanoutConsumer {
public:
    void Start() override {}
    void Stop() override {}
};

#ifdef SONET_USE_KAFKA
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>

// Full Kafka implementation using librdkafka
class KafkaFanoutPublisher : public FanoutPublisher {
public:
    KafkaFanoutPublisher(const std::string& brokers, const std::string& topic);
    ~KafkaFanoutPublisher();
    
    void PublishNewNote(const std::string& author_id, const std::string& note_id) override;
    void PublishUserUpdate(const std::string& user_id, const std::string& update_type);
    void PublishGroupChange(const std::string& group_id, const std::string& change_type);
    
    bool IsConnected() const { return producer_ != nullptr; }
    std::string GetLastError() const { return last_error_; }

private:
    std::string brokers_;
    std::string topic_;
    std::unique_ptr<RdKafka::Producer> producer_;
    std::string last_error_;
    mutable std::mutex error_mutex_;
    
    bool InitializeProducer();
    void HandleDeliveryReport(RdKafka::Message& message);
};

class KafkaFanoutConsumer : public FanoutConsumer {
public:
    KafkaFanoutConsumer(const std::string& brokers, const std::string& topic, const std::string& group);
    ~KafkaFanoutConsumer();
    
    void Start() override;
    void Stop() override;
    void SetMessageHandler(std::function<void(const std::string&, const std::string&)> handler);
    
    bool IsConnected() const { return consumer_ != nullptr; }
    std::string GetLastError() const { return last_error_; }

private:
    std::string brokers_;
    std::string topic_;
    std::string group_;
    std::unique_ptr<RdKafka::Consumer> consumer_;
    std::unique_ptr<RdKafka::Topic> kafka_topic_;
    std::thread consumer_thread_;
    std::atomic<bool> running_{false};
    std::string last_error_;
    mutable std::mutex error_mutex_;
    std::function<void(const std::string&, const std::string&)> message_handler_;
    
    bool InitializeConsumer();
    void ConsumerLoop();
    void HandleMessage(RdKafka::Message* message);
    void SetError(const std::string& error);
};

#else
// Fallback implementation when Kafka is not available
class KafkaFanoutPublisher : public FanoutPublisher {
public:
    KafkaFanoutPublisher(const std::string& brokers, const std::string& topic)
        : brokers_(brokers), topic_(topic) {}
    
    void PublishNewNote(const std::string& author_id, const std::string& note_id) override {
        // Log the event for debugging
        spdlog::info("Kafka not available - would publish note {} from user {} to topic {}", 
                     note_id, author_id, topic_);
    }
    
private:
    std::string brokers_;
    std::string topic_;
};

class KafkaFanoutConsumer : public FanoutConsumer {
public:
    KafkaFanoutConsumer(const std::string& brokers, const std::string& topic, const std::string& group)
        : brokers_(brokers), topic_(topic), group_(group) {}
    
    void Start() override {
        spdlog::info("Kafka not available - consumer would start for topic {} in group {}", topic_, group_);
    }
    
    void Stop() override {
        spdlog::info("Kafka consumer stopped");
    }
    
private:
    std::string brokers_;
    std::string topic_;
    std::string group_;
};
#endif

} // namespace fanout
} // namespace timeline
} // namespace sonet