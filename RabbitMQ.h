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

/**
 * 消息持久化的三要素：
 * 消息的投递模式为持久、发送到持久化的交换器、到达持久化的队列
 */

class RabbitChannel;

struct RabbitMessage {
public:
    RabbitMessage() :dirt(false) {
        memset(&envelope, 0,  sizeof(envelope));
        dirt = false;
    }

    ~RabbitMessage() {
        safe_clear();
    }

    void safe_clear() {
        if (dirt) {

            // avoid use destroy_xxx
            // for avoid the usage of amqp_poll
            amqp_bytes_free(envelope.message.body);
            amqp_bytes_free(envelope.routing_key);
            amqp_bytes_free(envelope.exchange);
            amqp_bytes_free(envelope.consumer_tag);

            memset(&envelope, 0,  sizeof(envelope));

            dirt = false;
        }
    }

    void touch() {
        dirt = true;
    }

public:
    amqp_envelope_t envelope;
private:
    bool dirt;   // 如果envelope带有malloc的数据，就将此置位
};



class RabbitMQHelper {

    friend class RabbitChannel;

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

    bool doConnect();
    bool isConnectionOpen() {
        return is_connected_;
    }

    void closeConnection() {
        amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
        is_connected_ = false;
    }

    int basicConsumeMessage(std::string &strRet,
                            struct timeval *timeout, int flags);

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
        if (channel <= 0) {
            if (connect_ids_[channel] != 0)
                connect_ids_[channel] = 1;
            else
                printf("free already free: %d", channel);
        }
        return 0;
    }

private:
    std::string connect_uri_;
    int frame_max_;

    amqp_connection_state_t connection_;
    bool is_connected_;

    // 0 free, 1 used, first 1 reserved
    char connect_ids_[2048];   // hardcode
};


class RabbitChannel {
public:
    RabbitChannel(RabbitMQHelper& mqHelper)
        :is_connected_(false), id_(-1),
         mqHelper_(mqHelper) {

    }

    ~RabbitChannel() {
        closeChannel();
    }

    int initChannel() {
        amqp_channel_t t = mqHelper_.getChannelId();
        if (t <=0 ) {
            printf("rabbitmq channel request error!");
            return -1;
        }
        amqp_channel_open_ok_t *r = amqp_channel_open(mqHelper_.connection_, id_);
        amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
        if( amqpErrorCheck(res) < 0) {
            printf("rabbitmq channel open error!");
            return -1;
        }
        is_connected_ = true;
        return 0;
    }

    // direct 直接根据路由键匹配
    // fanout 每条消息会广播到绑定到交换器上面的所有队列
    // topic  路由键规则匹配，'.'分割各个字段，'*'字段通配符，'#'表示任意字段，
    // durable 和Queue一样，虽然本身表示Broker重启时候能否重建Exchange和Queue，但也是消息持久化的前提
    int declareExchange(const std::string &exchange_name,
                        const std::string &exchange_type,
                        bool passive, bool durable, bool auto_delete);

    // if_unused 只有未被使用的时候，才允许删除
    int deleteExchange(const std::string &exchange_name,
                       bool if_unused);

    int bindExchange(const std::string &destination,
                     const std::string &source, const std::string &routing_key);

    int unbindExchange(const std::string &destination,
                       const std::string &source, const std::string &routing_key);


    // passive 如果队列不存在，是否创建队列；如果为true，指明的队列不存在的话会失败返回
    //         如果发送的消息路由不到队列，就会丢弃消息，所以如果确保消息不被丢弃，生产者和消费者都应该尝试创建队列
    // exclusive 表示私有队列，此时只有本应用程序才能消费队列消息
    // auto_delete 表示最后一个消费者取消订阅的时候，队列会被自动删除
    int declareQueue(const std::string &queue_name,
                     uint32_t &message_count, uint32_t &consumer_count,
                     bool passive, bool durable,
                     bool exclusive, bool auto_delete);

    int deleteQueue(const std::string &queue_name,
                    bool if_unused, bool if_empty);

    int bindQueue(const std::string &queue_name,
                  const std::string &exchange_name,
                  const std::string &routing_key);

    int unbindQueue(const std::string &queue_name,
                    const std::string &exchange_name, const std::string &routing_key);

    // 删除指定队列上的所有消息
    int purgeQueue(const std::string &queue_name);

    // 确认消息，delivery_tag是服务器返回的ID
    // 如果是multiple，会确认之前所有的消息，默认不要使用
    int basicAck(uint64_t delivery_tag, bool multiple = false);

    // 在收到无法处理的消息，或者服务端发送消息过快的时候可以使用
    // requeue 确定该消息是否会重新入队列，还是被丢弃，此处需要根据dead letter来决定处理方式
    int basicReject(uint64_t delivery_tag, bool requeue);
    // 相比basicReject，可以批量的否决
    int basicNack(uint64_t delivery_tag, bool requeue, bool multiple  /* = false */ );


    // 取消一个consumer，broker此后就不会再向这个consumer_tag指定的consumer发送消息了
    int basicCancel(const std::string &consumer_tag);

    // prefetch_count = 1 运行客户端最多允许未确认的消息数目，1表示只能有1个不被确认的消息，可能会影响性能
    // global_set 作用于整个connection的所有channel
    int basicQos(uint16_t message_prefetch_count, bool global_set);

    // 让服务器重发所有未确认的消息到指定的channel
    int basicRecover(const std::string &consumer);

    // mandatory 如果消息无法路由到队列中去，是让broker返回消息无法路由信息，还是直接丢弃(false)
    // immediate 针对消息无法路由，
    int basicPublish(const std::string &exchange_name,
                     const std::string &routing_key, bool mandatory, bool immediate,
                     const std::string &message);

    // 获取单条消息，会进行订阅消息-获取消息-取消订阅，连续消费消息的话性能较低

    int basicGet(RabbitMessage& rMessage, const std::string &queue,
                 bool no_ack);

    // consumer_tag是给callback的，用于区分各个消费者，同时该函数还会返回对应的consumer_tag供确认
    // no_ack 告知服务器不要expect ack/noack消息
    // exclusive 不允许其他消费者消费该队列的消息
    int basicConsume(const std::string &queue,
                     const std::string &consumer_tag,
                     bool no_local, bool no_ack, bool exclusive);

    bool isChannelOpen() {
        return is_connected_;
    }

    void closeChannel() {
        mqHelper_.freeChannelId(id_);
        is_connected_ = false;
        amqp_channel_close(mqHelper_.connection_, id_, AMQP_REPLY_SUCCESS); // reserved for RPC msg
    }

private:

    void closeConnection() {
        mqHelper_.closeConnection();
    }

    int amqpErrorCheck(amqp_rpc_reply_t x, const char* context = NULL_CTX);

private:
    bool is_connected_;
    amqp_channel_t id_;
    RabbitMQHelper& mqHelper_;
};


} // namespace AMQP


#endif
