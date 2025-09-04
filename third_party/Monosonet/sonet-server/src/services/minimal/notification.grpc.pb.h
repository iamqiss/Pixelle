#pragma once

#include "../../../proto/grpc_stub.h"
#include "../../../proto/services/stub_protos.h"

namespace sonet {
namespace notification {
    struct SendNotificationRequest {
        std::string user_id_;
        std::string title_;
        std::string body_;
        std::string type_;
        std::string user_id() const { return user_id_; }
        std::string title() const { return title_; }
        std::string body() const { return body_; }
        std::string type() const { return type_; }
        void set_user_id(const std::string& id) { user_id_ = id; }
        void set_title(const std::string& t) { title_ = t; }
        void set_body(const std::string& b) { body_ = b; }
        void set_type(const std::string& ty) { type_ = ty; }
    };
    
    struct SendNotificationResponse {
        bool success_;
        std::string notification_id_;
        bool success() const { return success_; }
        std::string notification_id() const { return notification_id_; }
        void set_success(bool s) { success_ = s; }
        void set_notification_id(const std::string& id) { notification_id_ = id; }
    };
    
    struct ListNotificationsRequest {
        std::string user_id_;
        int32_t page_;
        int32_t page_size_;
        std::string user_id() const { return user_id_; }
        int32_t page() const { return page_; }
        int32_t page_size() const { return page_size_; }
        void set_user_id(const std::string& id) { user_id_ = id; }
        void set_page(int32_t p) { page_ = p; }
        void set_page_size(int32_t size) { page_size_ = size; }
    };

    struct ListNotificationsResponse {
        std::vector<std::string> notification_ids_;
        int32_t total_count_;
        bool success_;
        std::string error_message_;
        const std::vector<std::string>& notification_ids() const { return notification_ids_; }
        int32_t total_count() const { return total_count_; }
        bool success() const { return success_; }
        std::string error_message() const { return error_message_; }
        void add_notification_ids(const std::string& id) { notification_ids_.push_back(id); }
        void set_total_count(int32_t count) { total_count_ = count; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& msg) { error_message_ = msg; }
    };

    struct MarkNotificationReadRequest {
        std::string user_id_;
        std::string notification_id_;
        std::string user_id() const { return user_id_; }
        std::string notification_id() const { return notification_id_; }
        void set_user_id(const std::string& id) { user_id_ = id; }
        void set_notification_id(const std::string& id) { notification_id_ = id; }
    };

    struct MarkNotificationReadResponse {
        bool success_;
        std::string error_message_;
        bool success() const { return success_; }
        std::string error_message() const { return error_message_; }
        void set_success(bool s) { success_ = s; }
        void set_error_message(const std::string& msg) { error_message_ = msg; }
    };

    class NotificationService {
    public:
        class Service {
        public:
            virtual ~Service() = default;
            virtual ::grpc::Status SendNotification(::grpc::ServerContext* context,
                                                    const SendNotificationRequest* request,
                                                    SendNotificationResponse* response) = 0;
            virtual ::grpc::Status ListNotifications(::grpc::ServerContext* context,
                                                     const ListNotificationsRequest* request,
                                                     ListNotificationsResponse* response) = 0;
            virtual ::grpc::Status MarkNotificationRead(::grpc::ServerContext* context,
                                                        const MarkNotificationReadRequest* request,
                                                        MarkNotificationReadResponse* response) = 0;
        };
    };
}
}