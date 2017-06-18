#ifndef __RABBITMQ_H_
#define __RABBITMQ_H_

#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>

// This object should not be shared among multi-threads

namespace AMQP {

static const char* NULL_CTX = "[NULL_CTX]";

class RabbitMQHelper {

public:
    // amqp://user:passwd@host:5672/vhostname
    RabbitMQHelper(const std::string& connect_uri, int frame_max = 131072 /*128K*/):
        connect_uri_(connect_uri), frame_max_(frame_max),
        is_connected_(false) {
        ::memset(connect_ids_, 0, sizeof(connect_ids_));
    }

    ~RabbitMQHelper() {
        if (is_connected_) {
            amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
            amqp_destroy_connection(connection_);
        }
    }

    bool connect();

    int declareExchange(amqp_channel_t channel, const std::string &exchange_name,
                        const std::string &exchange_type,
                        bool passive, bool durable, bool auto_delete);

    int deleteExchange(amqp_channel_t channel, const std::string &exchange_name,
                       bool if_unused);

    int bindExchange(amqp_channel_t channel, const std::string &destination,
                     const std::string &source, const std::string &routing_key);

    int unbindExchange(amqp_channel_t channel, const std::string &destination,
                       const std::string &source, const std::string &routing_key);

    int declareQueue(amqp_channel_t channel, const std::string &queue_name,
                     uint32_t &message_count, uint32_t &consumer_count,
                     bool passive, bool durable,
                     bool exclusive, bool auto_delete);

    int deleteQueue(amqp_channel_t channel, const std::string &queue_name,
                    bool if_unused, bool if_empty);

    int bindQueue(amqp_channel_t channel, const std::string &queue_name,
                  const std::string &exchange_name,
                  const std::string &routing_key);

    int unbindQueue(amqp_channel_t channel, const std::string &queue_name,
                    const std::string &exchange_name, const std::string &routing_key);

    int purgeQueue(amqp_channel_t channel, const std::string &queue_name);

    int basicAck(amqp_channel_t channel, uint64_t delivery_tag);

    int basicReject(amqp_channel_t channel, uint64_t delivery_tag,
                    bool multiple, bool requeue);

    int basicQos(amqp_channel_t channel, const std::string &consumer_tag,
                 uint16_t message_prefetch_count, bool global_set);

    int basicCancel(amqp_channel_t channel, const std::string &consumer_tag);

    int basicRecover(amqp_channel_t channel, const std::string &consumer);

    int basicPublish(amqp_channel_t channel, const std::string &exchange_name,
                     const std::string &routing_key, bool mandatory, bool immediate,
                     const std::string &message);
#if 0
    int basicGet(amqp_channel_t channel,
                 amqp_envelope_t* pEnvelope, const std::string &queue,
                 bool no_ack);
#endif
    int basicConsume(amqp_channel_t channel, const std::string &queue,
                     const std::string &consumer_tag,
                     bool no_local, bool no_ack, bool exclusive);

    int basicConsumeMessage(std::string &strRet,
                            struct timeval *timeout, int flags);


    amqp_channel_t createChannel() {
        amqp_channel_t new_channel = getChannelId();
        if (new_channel <= 0 || new_channel == 1) {
            printf("Can't alloc channel_id");
            return -1;
        }

        amqp_channel_open_ok_t *r = amqp_channel_open(connection_, new_channel);
        amqp_rpc_reply_t res = amqp_get_rpc_reply(connection_);
        if (amqp_error_check(res)) {
            printf("rabbitmq channel open error!");
            freeChannelId(new_channel);
            return -1;
        }

        return new_channel;
    }

    bool isChannelOpen(amqp_channel_t channel) {
        return connect_ids_[channel] != 0;
    }

    void closeChannel(amqp_channel_t channel) {
        freeChannelId(channel);
        amqp_channel_close(connection_, channel, AMQP_REPLY_SUCCESS); // reserved for RPC msg
    }

    bool isConnectionOpen() {
        return is_connected_;
    }

    void closeConnection() {
        amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
        is_connected_ = false;
    }

private:
    amqp_channel_t getChannelId() {
        for (int i=2; (int)i<sizeof(connect_ids_); ++i) {
            if (connect_ids_[i] == 0) {
                connect_ids_[i] = 1;
                return i;
            }
        }
        return -1;
    }

    int freeChannelId(amqp_channel_t channel) {
        if (connect_ids_[channel] != 0)
            connect_ids_[channel] = 1;
        else
            printf("free already free: %d", channel);

        return 0;
    }

    int amqp_error_check(amqp_rpc_reply_t x, amqp_channel_t channel = -1, char const *context = NULL_CTX);

private:
    std::string connect_uri_;
    int frame_max_;

    amqp_connection_state_t connection_;
    bool is_connected_;

    // 0 free, 1 used, first 1 reserved for RPC
    char connect_ids_[2048];   // hardcode

    //std::vector<amqp_channel_t> channels;
    //std::vector<amqp_frame_t> frame_queue_t;
   // std::map<amqp_channel_t, frame_queue_t> channel_map_t;
};

} // namespace AMQP


#endif
