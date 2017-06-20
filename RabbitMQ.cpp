#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>

#include "RabbitMQ.h"

namespace AMQP {

// class RabbitChannel



int RabbitChannel::amqpErrorCheck(amqp_rpc_reply_t x, const char* context) {
    int retCode = -1;

    switch (x.reply_type) {
        case AMQP_RESPONSE_NORMAL:
            retCode = 0;
            break;

        case AMQP_RESPONSE_NONE:
            fprintf(stderr, "%s missing RPC reply type!\n", context);
            break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            // If we're getting this likely is the socket is already closed
            fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
            break;

        case AMQP_RESPONSE_SERVER_EXCEPTION:
            switch (x.reply.id) {
                case AMQP_CONNECTION_CLOSE_METHOD: {
                    amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
                    fprintf(stderr, "%s: server connection error %uh, message: %.*s\n",
                            context,
                            m->reply_code,
                            (int) m->reply_text.len, (char *) m->reply_text.bytes);

                    fprintf(stderr, "Close connection");
                    closeConnection();
                    break;
                }

                case AMQP_CHANNEL_CLOSE_METHOD: {
                    amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
                    fprintf(stderr, "%s: server channel error %uh, message: %.*s\n",
                            context,
                            m->reply_code,
                            (int) m->reply_text.len, (char *) m->reply_text.bytes);
                    break;
                    if (id_ != -1) {
                        fprintf(stderr, "Close channel: %d", id_);
                        closeChannel();
                    }
                }
                default:
                    fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
                    break;
            }
            break;
    }

    return retCode;
}

int RabbitChannel::setConfirmSelect(){
    if (!isChannelOpen())
        return -1;

    amqp_confirm_select_t select = {};
    select.nowait = false;

    amqp_confirm_select_ok_t *r = amqp_confirm_select(mqHelper_.connection_, id_);

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        return -1;
    }

    if (res.reply.id != AMQP_CONFIRM_SELECT_OK_METHOD) {
        printf("expecting AMQP_CONFIRM_SELECT_OK_METHOD, but get reply.id: %d", res.reply.id);
        return -1;
    }

    is_publish_confirm_ = true;
    printf("Channel will work in confirm mode...");
    return 0;
}


int RabbitChannel::declareExchange(const std::string &exchange_name,
                                   const std::string &exchange_type,
                                   bool passive, bool durable, bool auto_delete) {

    amqp_exchange_declare_t declare = {};
    declare.exchange = amqp_cstring_bytes(exchange_name.c_str());
    declare.type = amqp_cstring_bytes(exchange_type.c_str());  // direct fanout topic
    declare.passive = passive;
    declare.durable = durable;
    declare.auto_delete = auto_delete;
    declare.internal = false;
    declare.nowait = false;

#if 1 //AMQP_VERSION_MINOR == 4
    amqp_exchange_declare_ok_t *r = amqp_exchange_declare(mqHelper_.connection_, id_, declare.exchange,
                                                          declare.type, declare.passive, declare.durable, amqp_empty_table);
#else
    amqp_exchange_declare_ok_t *r = amqp_exchange_declare(mqHelper_.connection_, id_, declare.exchange,
                                                          declare.type, declare.passive, declare.durable,
                                                          declare.auto_delete, declare.internal, amqp_empty_table);
#endif
    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        return -1;
    }

    return 0;
}

int RabbitChannel::deleteExchange(const std::string &exchange_name,
                                  bool if_unused) {

    amqp_exchange_delete_t del = {};
    del.exchange = amqp_cstring_bytes(exchange_name.c_str());
    del.if_unused = if_unused;
    del.nowait = false;

    amqp_exchange_delete_ok_t *r = amqp_exchange_delete(mqHelper_.connection_, id_, del.exchange, del.if_unused);

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        return -1;
    }

    return 0;
}

int RabbitChannel::bindExchange(const std::string &destination,
                                const std::string &source, const std::string &routing_key) {

    amqp_exchange_bind_t bind = {};
    bind.destination = amqp_cstring_bytes(destination.c_str());
    bind.source = amqp_cstring_bytes(source.c_str());
    bind.routing_key = amqp_cstring_bytes(routing_key.c_str());
    bind.nowait = false;

    amqp_exchange_bind_ok_t *r = amqp_exchange_bind(mqHelper_.connection_, id_, bind.destination,
                                                 bind.source, bind.routing_key ,
                                                 amqp_empty_table);
    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}

int RabbitChannel::unbindExchange(const std::string &destination,
                                  const std::string &source, const std::string &routing_key) {

    amqp_exchange_unbind_t unbind = {};
    unbind.destination = amqp_cstring_bytes(destination.c_str());
    unbind.source = amqp_cstring_bytes(source.c_str());
    unbind.routing_key = amqp_cstring_bytes(routing_key.c_str());
    unbind.nowait = false;

    amqp_exchange_unbind_ok_t *r = amqp_exchange_unbind(mqHelper_.connection_, id_, unbind.destination,
                                                        unbind.source, unbind.routing_key,
                                                        amqp_empty_table);
    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}


// QUEUE
int RabbitChannel::declareQueue(const std::string &queue_name,
                                uint32_t &message_count, uint32_t &consumer_count,
                                bool passive, bool durable,
                                bool exclusive, bool auto_delete) {

    amqp_queue_declare_t declare = {};
    declare.queue = amqp_cstring_bytes(queue_name.c_str());
    declare.passive = passive;
    declare.durable = durable;
    declare.exclusive = exclusive;
    declare.auto_delete = auto_delete;
    declare.nowait = false;

    amqp_queue_declare_ok_t *r = amqp_queue_declare(mqHelper_.connection_, id_, declare.queue, declare.passive, declare.durable,
                                                    declare.exclusive, declare.auto_delete, amqp_empty_table);
    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    std::string ret((char *)r->queue.bytes, r->queue.len);
    message_count = r->message_count;
    consumer_count = r->consumer_count;

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}

int RabbitChannel::deleteQueue(const std::string &queue_name,
                               bool if_unused, bool if_empty) {

    amqp_queue_delete_t del = {};
    del.queue = amqp_cstring_bytes(queue_name.c_str());
    del.if_unused = if_unused;
    del.if_empty = if_empty;
    del.nowait = false;

    amqp_queue_delete_ok_t *r = amqp_queue_delete(mqHelper_.connection_, id_, del.queue, del.if_unused, del.if_empty);

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}

int RabbitChannel::bindQueue(const std::string &queue_name,
                             const std::string &exchange_name,
                             const std::string &routing_key) {

    amqp_queue_bind_t bind = {};
    bind.queue = amqp_cstring_bytes(queue_name.c_str());
    bind.exchange = amqp_cstring_bytes(exchange_name.c_str());
    bind.routing_key = amqp_cstring_bytes(routing_key.c_str());
    bind.nowait = false;

    amqp_queue_bind_ok_t *r = amqp_queue_bind(mqHelper_.connection_, id_, bind.queue, bind.exchange, bind.routing_key, amqp_empty_table);

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}

int RabbitChannel::unbindQueue(const std::string &queue_name,
                               const std::string &exchange_name, const std::string &routing_key) {

    amqp_queue_unbind_t unbind = {};
    unbind.queue = amqp_cstring_bytes(queue_name.c_str());
    unbind.exchange = amqp_cstring_bytes(exchange_name.c_str());
    unbind.routing_key = amqp_cstring_bytes(routing_key.c_str());

    amqp_queue_unbind_ok_t *r = amqp_queue_unbind(mqHelper_.connection_, id_, unbind.queue, unbind.exchange, unbind.routing_key, amqp_empty_table);

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}

int RabbitChannel::purgeQueue(const std::string &queue_name) {

    amqp_queue_purge_t purge = {};
    purge.queue = amqp_cstring_bytes(queue_name.c_str());
    purge.nowait = false;

    amqp_queue_purge_ok_t *r = amqp_queue_purge(mqHelper_.connection_, id_, purge.queue);

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        return -1;
    }

    return 0;
}


int RabbitChannel::basicAck(uint64_t delivery_tag, bool multiple /* = false */ ) {

    amqp_basic_ack_t info = {};
    info.delivery_tag = delivery_tag;
    info.multiple = multiple;

    int retCode = amqp_basic_ack(mqHelper_.connection_, id_, info.delivery_tag, info.multiple);
    if (retCode == 0)
        return 0;

    return -1;
}

int RabbitChannel::basicReject(uint64_t delivery_tag, bool requeue) {
    if (!isChannelOpen())
        return -1;

    amqp_basic_reject_t req;
    req.delivery_tag = delivery_tag;
    req.requeue = requeue;

    int retCode = amqp_basic_reject(mqHelper_.connection_, id_, req.delivery_tag, req.requeue);
    if (retCode == 0)
        return 0;

    return -1;
}

// 相比basicReject，可以批量的否决
int RabbitChannel::basicNack(uint64_t delivery_tag, bool requeue, bool multiple  /* = false */ ) {
    if (!isChannelOpen())
        return -1;

    amqp_basic_nack_t req;
    req.delivery_tag = delivery_tag;
    req.requeue = requeue;
    req.multiple = multiple;

    int retCode = amqp_basic_nack(mqHelper_.connection_, id_, req.delivery_tag, req.multiple, req.requeue);
    if (retCode == 0)
        return 0;

    return -1;
}


int RabbitChannel::basicQos(uint16_t message_prefetch_count, bool global_set) {

    if (!isChannelOpen())
        return -1;

    amqp_basic_qos_t qos = {};
    qos.prefetch_size = 0;  // not implemented for RabbitMQ
    qos.prefetch_count = message_prefetch_count;
    qos.global = global_set;

    if (qos.prefetch_count != 1) {
        printf("Attention: qos.prefetch_count = %d", qos.prefetch_count);
    }

    amqp_basic_qos_ok_t *r = amqp_basic_qos(mqHelper_.connection_, id_, qos.prefetch_size, qos.prefetch_count, qos.global);
    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}

int RabbitChannel::basicCancel(const std::string &consumer_tag) {

    if (!isChannelOpen())
        return -1;

    amqp_basic_cancel_t cancel = {};
    cancel.consumer_tag = amqp_cstring_bytes(consumer_tag.c_str());
    cancel.nowait = false;

    amqp_basic_cancel_ok_t *r = amqp_basic_cancel(mqHelper_.connection_, id_, cancel.consumer_tag);
    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    closeChannel();

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}

int RabbitChannel::basicRecover(const std::string &consumer) {

    if (!isChannelOpen())
        return -1;

    amqp_basic_recover_t recover = {};
    recover.requeue = true;

    amqp_basic_recover_ok_t *r = amqp_basic_recover(mqHelper_.connection_, id_, recover.requeue);
    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}


int RabbitChannel::basicPublish(const std::string &exchange_name,
                                const std::string &routing_key, bool mandatory, bool immediate,
                                const std::string &message) {
    if (!isChannelOpen())
        return -1;

    amqp_bytes_t message_bytes;
    message_bytes.bytes = (void *)(message.c_str());
    message_bytes.len = message.size();

    int retCode = amqp_basic_publish(mqHelper_.connection_, id_,
                                 amqp_cstring_bytes(exchange_name.c_str()),
                                 amqp_cstring_bytes(routing_key.c_str()),
                                 mandatory, immediate,
                                 NULL, message_bytes);

    if (retCode < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    if (!is_publish_confirm_) {
        return 0;
    }

    // Publish Confirm mode
    // - basic.ack - our channel is in confirm mode, messsage was 'dealt with' by the broker
    // - basic.return then basic.ack - the message wasn't delievered, but was dealt with

    amqp_frame_t frame;
    if (AMQP_STATUS_OK != amqp_simple_wait_frame(mqHelper_.connection_, &frame)) {
        printf("publish ok, but confirm may fail!");
        return -1;
    }
    if (frame.payload.method.id == AMQP_BASIC_ACK_METHOD)
        // Broker ACK message
        return 0;
    } else if(frame.payload.method.id == AMQP_BASIC_RETURN_METHOD) {
        // read the return message
        {
            amqp_message_t message;
            amqp_rpc_reply_t res = amqp_read_message(mqHelper_.connection_, frame.channel, &message, 0);
            if (AMQP_RESPONSE_NORMAL == res.reply_type)
                amqp_destroy_message(&message);
        }
        printf("basic.rturn called!");
        return -1;
    } else {
        printf("Unexpeced method.id: %d", frame.payload.method.id);
        return -1;
    }
}


int RabbitChannel::basicGet(RabbitMessage& rMessage, const std::string &queue,
                            bool no_ack) {

    if (!isChannelOpen())
        return -1;

    rMessage.safe_clear();
#if 0
    //
    amqp_rpc_reply_t res = amqp_read_message(mqHelper_.connection_, id_, &rMessage.envelope.message, 0);
#endif

    amqp_basic_get_t get = {};
    get.queue = amqp_cstring_bytes(queue.c_str());
    get.no_ack = no_ack;

    amqp_rpc_reply_t res = amqp_basic_get(mqHelper_.connection_, id_, get.queue, get.no_ack);
    if( amqpErrorCheck(res) < 0 ||
        res.reply_type == AMQP_RESPONSE_NONE ||
        AMQP_BASIC_GET_EMPTY_METHOD == res.reply.id) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    if (res.reply.id != AMQP_BASIC_GET_OK_METHOD) {
        printf("unexpeced reply.id: %d", res.reply.id);
        return -1;
    }

    amqp_basic_get_ok_t *get_ok = (amqp_basic_get_ok_t *)res.reply.decoded;
    if (!get_ok) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    rMessage.safe_clear();
    rMessage.envelope.delivery_tag = get_ok->delivery_tag;
    rMessage.envelope.redelivered = get_ok->redelivered;
    rMessage.envelope.exchange = amqp_bytes_malloc_dup(get_ok->exchange);   // not checked
    rMessage.envelope.routing_key = amqp_bytes_malloc_dup(get_ok->routing_key);
    rMessage.touch();

    amqp_frame_t frame;
    // check first frame header
    amqp_maybe_release_buffers(mqHelper_.connection_);
    if(amqp_simple_wait_frame(mqHelper_.connection_, &frame) != AMQP_STATUS_OK){
        printf("wait for frame header error!");
        return -1;
    }

    if (frame.frame_type != AMQP_FRAME_HEADER){
        printf("expecting AMQP_FRAME_HEADER, but get: %d", frame.frame_type);
        return -1;
    }

    rMessage.envelope.channel = frame.channel;
    amqp_basic_properties_t * properties = reinterpret_cast<amqp_basic_properties_t *>(frame.payload.properties.decoded);
    size_t body_size = static_cast<size_t>(frame.payload.properties.body_size);
    size_t received_size = 0;
    rMessage.envelope.message.body = amqp_bytes_malloc(body_size);  // already set body.len
    if (!rMessage.envelope.message.body.bytes) {
        rMessage.safe_clear();
        return -1;
    }

    while (received_size < body_size) {
        if(amqp_simple_wait_frame(mqHelper_.connection_, &frame) < 0){
            printf("wait for frame header error!");
            rMessage.safe_clear();
            amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
            return -1;
        }

        if (frame.frame_type != AMQP_FRAME_BODY) {
            printf("expecting AMQP_FRAME_BODY, but get: %d", frame.frame_type);
            rMessage.safe_clear();
            amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
            return -1;
        }

        // copy and store message
        void *body_ptr = reinterpret_cast<char *>(rMessage.envelope.message.body.bytes) + received_size;
        memcpy(body_ptr, frame.payload.body_fragment.bytes, frame.payload.body_fragment.len);
        received_size += frame.payload.body_fragment.len;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}


int RabbitChannel::basicConsume(const std::string &queue,
                                const std::string &consumer_tag,
                                bool no_local, bool no_ack, bool exclusive) {
    if (!isChannelOpen())
        return -1;

    amqp_basic_consume_t consume = {};
    consume.queue = amqp_cstring_bytes(queue.c_str());
    consume.consumer_tag = amqp_cstring_bytes(consumer_tag.c_str());
    consume.no_local = no_local;
    consume.no_ack = no_ack;
    consume.exclusive = exclusive;
    consume.nowait = false;

    amqp_basic_consume_ok_t *r = amqp_basic_consume(mqHelper_.connection_, id_, consume.queue, consume.consumer_tag,
                                                    consume.no_local, consume.no_ack, consume.exclusive, amqp_empty_table);

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
        amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
        return -1;
    }

    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;
}


int RabbitMQHelper::basicConsumeMessage(RabbitMessage& rabbit_msg,
                                        struct timeval *timeout, int flags) {

    int retCode = 0;
    amqp_rpc_reply_t ret;
    amqp_frame_t frame;

    amqp_maybe_release_buffers(connection_);
    rabbit_msg.safe_clear();
    ret = amqp_consume_message(connection_, &rabbit_msg.envelope, timeout/*blocking*/, 0);

    // un-normal condition
    if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
        if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type && AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {
            if (AMQP_STATUS_OK != amqp_simple_wait_frame(connection_, &frame)) {
                return -1;
            }

            if (AMQP_FRAME_METHOD == frame.frame_type) {
                switch (frame.payload.method.id) {
                    case AMQP_BASIC_ACK_METHOD:
                        /* if we've turned publisher confirms on, and we've published a message
                         * here is a message being confirmed
                         */
                        retCode = 0;
                        break;

                    case AMQP_BASIC_RETURN_METHOD:
                       /* if a published message couldn't be routed and the mandatory flag was set
                        * this is what would be returned. The message then needs to be read.
                        */
                        {
                            amqp_message_t message;
                            ret = amqp_read_message(connection_, frame.channel, &message, 0);
                            if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
                                retCode = -1;
                                break;
                            }

                            amqp_destroy_message(&message);
                        }

                        break;

                    case AMQP_CHANNEL_CLOSE_METHOD:
                      /* a channel.close method happens when a channel exception occurs, this
                       * can happen by publishing to an exchange that doesn't exist for example
                       *
                       * In this case you would need to open another channel redeclare any queues
                       * that were declared auto-delete, and restart any consumers that were attached
                       * to the previous channel
                       */

                        return -1;

                    case AMQP_CONNECTION_CLOSE_METHOD:
                      /* a connection.close method happens when a connection exception occurs,
                       * this can happen by trying to use a channel that isn't open for example.
                       *
                       * In this case the whole connection must be restarted.
                       */
                        return -1;

                    default:
                        fprintf(stderr ,"An unexpected method was received %u\n", frame.payload.method.id);
                        return -1;
                } // switch
            } // AMQP_FRAME_METHOD
        }

    } else { //AMQP_RESPONSE_NORMAL

        //
    }

    return 0;
}

// class RabbitMQHelper

bool RabbitMQHelper::doConnect() {

    if (connect_uri_.empty() || frame_max_ <= 0) {
        printf("invalid argument!");
        return false;
    }

    amqp_connection_info info;
    amqp_default_connection_info(&info);
    char uri[2048] = {0, };
    strncpy(uri, connect_uri_.c_str(), sizeof(uri));
    if (amqp_parse_url(uri, &info) != 0) {
        printf("prase connect_uri failed: %s", uri);
        return false;
    }

    connection_ = amqp_new_connection();
    if (!connection_) {
        printf("rabbitmq new connect error!");
        return false;
    }

    amqp_socket_t *socket = amqp_tcp_socket_new(connection_);
    int sock = amqp_socket_open(socket, info.host, info.port);
    if (sock < 0) {
        printf("rabbitmq socket open error!");
        goto error1;
    }

    {
        amqp_rpc_reply_t res = amqp_login(connection_, info.vhost, 0, frame_max_, 0,
                             AMQP_SASL_METHOD_PLAIN, info.user, info.password);
        if (AMQP_RESPONSE_NORMAL != res.reply_type) {
            printf("rabbitmq login error!");
            goto error2;
        }
    }

    printf("rabbitmq client connect to %s:%d/%s ok!", info.host, info.port, info.vhost);
    is_connected_ = true;
    return true;

error2:
    amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
error1:
    amqp_destroy_connection(connection_);
    is_connected_ = false;
    return false;
}



} // namespace AMQP

