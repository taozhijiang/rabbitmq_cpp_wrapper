#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>
#include <boost/algorithm/string.hpp>

#include "RabbitMQ.h"

// https://github.com/alanxz/rabbitmq-c/issues/211
// Additionally: you should call amqp_maybe_release_buffers_on_channel() after consuming each message.
//
// 总体来说，rabbitmq-c内部的内存缓存，除非在发消息、收消息这种高频重复要主动调用
// amqp_maybe_release_buffers(_on_channel)来主动释放防止占用过多内存，其他情况下
// 调用这种函数收效甚微，而且在关闭的connection上调用该函数会crash
// 所以目前的策略是：仅在成功操作的情况下主动调用该函数，防止库导致服务异常挂起

namespace AMQP {

static int amqp_bytes_malloc_dup_failed(amqp_bytes_t bytes) {
    if (bytes.len != 0 && bytes.bytes == NULL) {
        return 1;
    }
    return 0;
}

std::string RabbitMQHelper::brokerVersion() {

    const amqp_table_t *properties = amqp_get_server_properties(connection_);
    const amqp_bytes_t version = amqp_cstring_bytes("version");
    amqp_table_entry_t *version_entry = NULL;

    for (int i = 0; i < properties->num_entries; ++i) {
        if (0 == strncmp((const char *)properties->entries[i].key.bytes, (const char *)version.bytes, version.len)) {
            version_entry = &properties->entries[i];
            break;
        }
    }

    if (NULL == version_entry)
        return std::string();

    std::string version_string(
      static_cast<char *>(version_entry->value.value.bytes.bytes),
      version_entry->value.value.bytes.len);

    return version_string;
}

// class RabbitChannel

amqp_channel_t RabbitMQHelper::createChannel() {
	amqp_channel_t t;
	if ( (t = getChannelId()) <= 0) {
		LOG_API("getChannelId failed!");
		return -1;
	}

    std::shared_ptr<RabbitChannel> pChannel;
	pChannel.reset(new RabbitChannel(t, *this));
	if (!pChannel || pChannel->initChannel() < 0) {
		freeChannelId(t);
		return -1;
	}

	channels_[t] = pChannel;	// insert it!!
	LOG_API("created channel: %d", t);
	return t;
}

int RabbitMQHelper::closeChannel(amqp_channel_t channel){
    std::map<amqp_channel_t, std::shared_ptr<RabbitChannel> >::iterator it;

	it = channels_.find(channel);
	if (it == channels_.end()) {
		return -1;
	}

    it->second->closeChannel();
    return 0;
}

int RabbitMQHelper::freeChannel(amqp_channel_t channel) {
    std::map<amqp_channel_t, std::shared_ptr<RabbitChannel> >::iterator it;

	it = channels_.find(channel);
	if (it == channels_.end()) {
		return -1;
	}

	channels_.erase(channel); // auto call closeChannel()
	freeChannelId(channel);

	return 0;
}

bool RabbitMQHelper::isChannelOpen(amqp_channel_t channel) {
    std::map<amqp_channel_t, std::shared_ptr<RabbitChannel> >::iterator it;
	it = channels_.find(channel);
	if (it == channels_.end())
        return false;

    return it->second->isChannelOpen();
}

void RabbitMQHelper::closeConnection() {

	if (!is_connected_)
        return;

    is_connected_ = false;

    // 我们必须在这里强迫先析构Channel，然后才能析构connection_，否则
    // 顺序倒了会导致Channel析构的时候段错误 SIGSEGV
    //
    // though when connection close, close all channel. though the lib did these implicity
    //
    std::map<amqp_channel_t, std::shared_ptr<RabbitChannel> >::iterator it;
    for (it=channels_.begin(); it!=channels_.end(); ++it) {
        if (it->second)
            it->second->closeChannel();
    }

	channels_.clear();
    LOG_API("Connection is closing...");

	amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
	amqp_destroy_connection(connection_); //atomatically free resources
}

int RabbitMQHelper::checkAndRepairChannel(amqp_channel_t& channel,
                                          RabbitChannelSetupFunc func, void* pArg){
	if (isConnectionOpen() && isChannelOpen(channel))
        return 0;

    if (!isConnectionOpen()) {
        if (!doConnect()) {
            LOG_API("Connect Failed!");
            return -1;
        }
    }

    if (!isChannelOpen(channel)) {
        freeChannel(channel);
        channel = createChannel();
        if (channel <= 0) {
            LOG_API("Create Channel Failed!");
            return -1;
        }

        if (!setupChannel(channel, func, pArg)) {
            freeChannel(channel);
            LOG_API("Setup Channel Failed!");
            return -1;
        }
    }

    return 0;
}

bool RabbitMQHelper::setupChannel(amqp_channel_t channel, RabbitChannelSetupFunc func, void* pArg){
	if (!isChannelOpen(channel))
        return false;

    return func(channelInstance(channel), pArg);
}

int RabbitMQHelper::basicRecover(amqp_channel_t channel, const std::string &consumer) {
	if (!isChannelOpen(channel))
        return -1;

    return channelInstance(channel)->basicRecover(consumer);
}

int RabbitMQHelper::basicPublish(amqp_channel_t channel, const std::string &exchange_name,
                                 const std::string &routing_key, bool mandatory, bool immediate,
                                 const std::string &message) {
	if (!isChannelOpen(channel))
        return -1;

    return channelInstance(channel)->basicPublish(exchange_name, routing_key, mandatory, immediate, message);
}

int RabbitMQHelper::basicGet(amqp_channel_t channel, RabbitMessage& rabbit_msg,
                             const std::string &queue, bool no_ack) {
    if (!isChannelOpen(channel))
        return -1;

    return channelInstance(channel)->basicGet(rabbit_msg, queue, no_ack);
}

int RabbitMQHelper::basicAck(amqp_channel_t channel, uint64_t delivery_tag,
                             bool multiple){
    if (!isChannelOpen(channel))
        return -1;

    return channelInstance(channel)->basicAck(delivery_tag, multiple);
}

int RabbitMQHelper::basicReject(amqp_channel_t channel, uint64_t delivery_tag,
                                bool requeue){
    if (!isChannelOpen(channel))
        return -1;

    return channelInstance(channel)->basicReject(delivery_tag, requeue);
}

int RabbitMQHelper::basicNack(amqp_channel_t channel, uint64_t delivery_tag,
                              bool requeue, bool multiple  /* = false */ ){
    if (!isChannelOpen(channel))
        return -1;

    return channelInstance(channel)->basicNack(delivery_tag, requeue, multiple);
}

//
// RabbitChannel
//

int RabbitChannel::amqpErrorCheck(amqp_rpc_reply_t x, const char* context) {
    int retCode = -1;

    switch (x.reply_type) {
        case AMQP_RESPONSE_NORMAL:
            retCode = 0;
            break;

        case AMQP_RESPONSE_NONE:
            LOG_API("%s missing RPC reply type!\n", context); // conn error
            break;

        case AMQP_RESPONSE_LIBRARY_EXCEPTION:
            // If we're getting this likely is the socket is already closed
            LOG_API("%s: %s\n", context, amqp_error_string2(x.library_error));
            closeConnection();
            break;

        case AMQP_RESPONSE_SERVER_EXCEPTION:
            switch (x.reply.id) {
                case AMQP_CONNECTION_CLOSE_METHOD: {
                    amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
                    LOG_API("%s: server connection error %uh, message: %.*s\n",
                            context,
                            m->reply_code,
                            (int) m->reply_text.len, (char *) m->reply_text.bytes);

                    LOG_API("Close connection");
                    closeConnection();
                    break;
                }

                case AMQP_CHANNEL_CLOSE_METHOD: {
                    amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
                    LOG_API("%s: server channel error %uh, message: %.*s\n",
                            context,
                            m->reply_code,
                            (int) m->reply_text.len, (char *) m->reply_text.bytes);

                    LOG_API("Close channel: %d", id_);
                    closeChannel();
                    break;
                }

                default: {
                    LOG_API("%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
                    closeConnection();
                    break;
                }
            }
			break;

		default:
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
		LOG_API("setConfirmSelect return with: %d", ret_code);
        return -1;
    }

    if (res.reply.id != AMQP_CONFIRM_SELECT_OK_METHOD) {
        LOG_API("expecting AMQP_CONFIRM_SELECT_OK_METHOD, but get reply.id: %d", res.reply.id);
        return -1;
    }

    is_publish_confirm_ = true;
    LOG_API("Channel will work in confirm mode...");
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_exchange_declare error with: %d", ret_code);
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_exchange_delete error with: %d", ret_code);
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_exchange_bind error with: %d", ret_code);
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_exchange_unbind error with: %d", ret_code);
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
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_queue_declare error with: %d", ret_code);
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_queue_delete error with: %d", ret_code);
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_queue_bind error with: %d", ret_code);
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_queue_unbind error with: %d", ret_code);
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_queue_purge error with: %d", ret_code);
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

	closeConnection();
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

    closeConnection();
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

    closeConnection();
    return -1;
}


int RabbitChannel::basicQos(uint16_t message_prefetch_count, bool global_set) {

    if (!isChannelOpen()){
        LOG_API("channel is not open");
        return -1;
    }

    amqp_basic_qos_t qos = {};
    qos.prefetch_size = 0;  // not implemented for RabbitMQ
    qos.prefetch_count = message_prefetch_count;
    qos.global = global_set;

    if (qos.prefetch_count != 1) {
        LOG_API("Attention: qos.prefetch_count = %d", qos.prefetch_count);
    }

    amqp_basic_qos_ok_t *r = amqp_basic_qos(mqHelper_.connection_, id_, qos.prefetch_size, qos.prefetch_count, qos.global);
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_basic_qos error with: %d", ret_code);
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    if( amqpErrorCheck(res) < 0) {
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
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_basic_recover error with: %d", ret_code);
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

	//消息持久化的三要素之一:消息的投递模式为持久
    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.delivery_mode = 2; /* persistent delivery mode */

    int retCode = amqp_basic_publish(mqHelper_.connection_, id_,
                                 amqp_cstring_bytes(exchange_name.c_str()),
                                 amqp_cstring_bytes(routing_key.c_str()),
                                 mandatory, immediate,
                                 &props, message_bytes);

    if (retCode < 0) {
        LOG_API("amqp_basic_publish fail! ret:%d", retCode);
		goto connection_err;
    }

    if (!is_publish_confirm_) {
        return 0;
    }

    // Publish Confirm mode
    // - basic.ack - our channel is in confirm mode, messsage was 'dealt with' by the broker
    // - basic.return then basic.ack - the message wasn't delievered, but was dealt with

    amqp_frame_t frame;
    if (AMQP_STATUS_OK != amqp_simple_wait_frame(mqHelper_.connection_, &frame)) {
        LOG_API("publish ok, but confirm may fail!");
		goto connection_err;
    }
    if (frame.payload.method.id == AMQP_BASIC_ACK_METHOD) {
        // Broker ACK message
        return 0;
    } else if(frame.payload.method.id == AMQP_BASIC_RETURN_METHOD) {
        /* Message was published with mandatory = true and the message
         * wasn't routed to a queue, so the message is returned */
        // read the return message
        {
            amqp_message_t message_dummy;
            amqp_rpc_reply_t res = amqp_read_message(mqHelper_.connection_, frame.channel, &message_dummy, 0);
            if (AMQP_RESPONSE_NORMAL == res.reply_type)
                amqp_destroy_message(&message_dummy);
        }
        LOG_API("basic.return called!");
		goto connection_err;
    } else {
        LOG_API("Unexpeced method.id: %d", frame.payload.method.id);
		goto connection_err;
    }

connection_err:
	closeConnection();
    return -1;
}


int RabbitChannel::basicGet(RabbitMessage& rabbit_msg, const std::string &queue,
                            bool no_ack) {

    if (!isChannelOpen())
        return -1;

    rabbit_msg.safe_clear();
#if 0
    //
    amqp_rpc_reply_t res = amqp_read_message(mqHelper_.connection_, id_, &rabbit_msg.envelope.message, 0);
#endif

    size_t received_size = 0;
    size_t body_size = 0;
    amqp_basic_properties_t * properties = NULL;
    amqp_basic_get_ok_t * get_ok = NULL;

    amqp_basic_get_t get = {};
    get.queue = amqp_cstring_bytes(queue.c_str());
    get.no_ack = no_ack;

    amqp_rpc_reply_t res = amqp_basic_get(mqHelper_.connection_, id_, get.queue, get.no_ack);
    if( amqpErrorCheck(res) < 0 ||
        res.reply_type == AMQP_RESPONSE_NONE ||
        AMQP_BASIC_GET_EMPTY_METHOD == res.reply.id) {
        goto error_out1;
    }

    if (res.reply.id != AMQP_BASIC_GET_OK_METHOD) {
        LOG_API("unexpeced reply.id: %d", res.reply.id);
        goto error_out1;
    }

    get_ok = (amqp_basic_get_ok_t *)res.reply.decoded;
    if (!get_ok) {
        goto error_out1;
    }

    rabbit_msg.safe_clear();
    rabbit_msg.envelope.delivery_tag = get_ok->delivery_tag;
    rabbit_msg.envelope.redelivered = get_ok->redelivered;
    rabbit_msg.envelope.exchange = amqp_bytes_malloc_dup(get_ok->exchange);
    rabbit_msg.envelope.routing_key = amqp_bytes_malloc_dup(get_ok->routing_key);

    if (amqp_bytes_malloc_dup_failed(rabbit_msg.envelope.exchange) ||
        amqp_bytes_malloc_dup_failed(rabbit_msg.envelope.routing_key)) {
        LOG_API("malloc failed!");
        goto error_out2;
    }

    amqp_frame_t frame;
    // check first frame header
    amqp_maybe_release_buffers(mqHelper_.connection_);
    if(amqp_simple_wait_frame(mqHelper_.connection_, &frame) != AMQP_STATUS_OK){
        LOG_API("wait for frame header error!");
        goto error_out2;
    }

    if (frame.frame_type != AMQP_FRAME_HEADER){
        LOG_API("expecting AMQP_FRAME_HEADER, but get: %d", frame.frame_type);
        goto error_out2;
    }

    rabbit_msg.envelope.channel = frame.channel;
    init_amqp_pool(&rabbit_msg.envelope.message.pool, 1); // init but not used
    properties = reinterpret_cast<amqp_basic_properties_t *>(frame.payload.properties.decoded);
    (void)properties;

    received_size = 0;
    body_size = static_cast<size_t>(frame.payload.properties.body_size);
    if (0 == frame.payload.properties.body_size) {
        rabbit_msg.envelope.message.body = amqp_empty_bytes;
    } else {
        rabbit_msg.envelope.message.body = amqp_bytes_malloc(body_size);  // already set body.len
        if (!rabbit_msg.envelope.message.body.bytes) {
            LOG_API("malloc for message body failed!");
            goto error_out3;
        }
    }

    while (received_size < body_size) {
        if(amqp_simple_wait_frame(mqHelper_.connection_, &frame) < 0){
            LOG_API("wait for frame header error!");
            goto error_out3;
        }

        if (frame.frame_type != AMQP_FRAME_BODY) {
            LOG_API("expecting AMQP_FRAME_BODY, but get: %d", frame.frame_type);
            goto error_out3;
        }

        // copy and store message
        void *body_ptr = reinterpret_cast<char *>(rabbit_msg.envelope.message.body.bytes) + received_size;
        memcpy(body_ptr, frame.payload.body_fragment.bytes, frame.payload.body_fragment.len);
        received_size += frame.payload.body_fragment.len;
    }

    rabbit_msg.touch();
    amqp_maybe_release_buffers_on_channel(mqHelper_.connection_, id_);
    return 0;

error_out3:
    empty_amqp_pool(&rabbit_msg.envelope.message.pool);

error_out2:
    amqp_bytes_free(rabbit_msg.envelope.message.body); // safe
    amqp_bytes_free(rabbit_msg.envelope.routing_key);
    amqp_bytes_free(rabbit_msg.envelope.exchange);

error_out1:
    return -1;
}


int RabbitChannel::basicConsume(const std::string &queue,
                                const std::string &consumer_tag,
                                bool no_local, bool no_ack, bool exclusive) {
    if (!isChannelOpen())
        return -1;

    amqp_basic_consume_t consume = {};
    consume.queue = amqp_cstring_bytes(queue.c_str());
    if (consumer_tag.empty() || consumer_tag == "*") {
        consume.consumer_tag = amqp_empty_bytes;
    } else {
        consume.consumer_tag = amqp_cstring_bytes(consumer_tag.c_str());
    }
    consume.no_local = no_local;
    consume.no_ack = no_ack;
    consume.exclusive = exclusive;
    consume.nowait = false;

    amqp_basic_consume_ok_t *r = amqp_basic_consume(mqHelper_.connection_, id_, consume.queue, consume.consumer_tag,
                                                    consume.no_local, consume.no_ack, consume.exclusive, amqp_empty_table);
    (void)r;

    amqp_rpc_reply_t res = amqp_get_rpc_reply(mqHelper_.connection_);
    int ret_code = amqpErrorCheck(res);
    if( ret_code < 0 ) {
        LOG_API("rabbitmq amqp_basic_consume error with: %d", ret_code);
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

    rabbit_msg.safe_clear();
    ret = amqp_consume_message(connection_, &rabbit_msg.envelope, timeout/*blocking*/, 0);

    // un-normal condition
    if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
        if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type && AMQP_STATUS_TIMEOUT == ret.library_error) {
			return WAIT_MSG_TIMEOUT;
		}
		else if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type && AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {

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

                        LOG_API("Close other channel: %d", frame.channel);
                        closeChannel(frame.channel);
                        return -1;

                    case AMQP_CONNECTION_CLOSE_METHOD:
                      /* a connection.close method happens when a connection exception occurs,
                       * this can happen by trying to use a channel that isn't open for example.
                       *
                       * In this case the whole connection must be restarted.
                       */

                        LOG_API("Close connection");
                        closeConnection();
                        return -1;

                    default:
                        LOG_API("An unexpected method was received %u\n", frame.payload.method.id);
                        return -1;
                } // switch
            } // AMQP_FRAME_METHOD
        }

    } else { //AMQP_RESPONSE_NORMAL

        rabbit_msg.touch();
    }

    amqp_maybe_release_buffers(connection_);
    return 0;
}

// class RabbitMQHelper

bool RabbitMQHelper::doConnect() {

    if (connect_uris_.empty() || frame_max_ <= 0) {
        LOG_API("invalid argument!");
        return false;
    }

    // when re-connect, free all channel
    channel_ids_.clear();
    channels_.clear();

    amqp_connection_info info;
    amqp_default_connection_info(&info);

    std::vector<std::string>::const_iterator it;
    for (it = connect_uris_.cbegin(); it != connect_uris_.cend(); ++it){

		char uri[2048] = {0, };
		strncpy(uri, it->c_str(), sizeof(uri));

		if (amqp_parse_url(uri, &info) != 0) {
			LOG_API("prase connect_uri failed: %s", uri);
			continue;
		}

		connection_ = amqp_new_connection();
		if (!connection_) {
			LOG_API("rabbitmq new connect error!");
			continue;
		}

		amqp_socket_t *socket = amqp_tcp_socket_new(connection_);
		int sock = amqp_socket_open(socket, info.host, info.port);
		if (sock < 0) {
			LOG_API("rabbitmq socket open error!");
			amqp_destroy_connection(connection_);
			continue;
		}

		amqp_rpc_reply_t res = amqp_login(connection_, info.vhost, 0, frame_max_, 0,
								 AMQP_SASL_METHOD_PLAIN, info.user, info.password);
		if (AMQP_RESPONSE_NORMAL != res.reply_type) {
			LOG_API("rabbitmq login error!");
			amqp_connection_close(connection_, AMQP_REPLY_SUCCESS);
			amqp_destroy_connection(connection_);
			continue;
		}

		LOG_API("rabbitmq client connect to %s:%d/%s ok!", info.host, info.port, info.vhost);
		is_connected_ = true;

		max_channel_id_ = amqp_get_channel_max(connection_);
		if (max_channel_id_ == 0 || max_channel_id_ > 2048 ) {
			max_channel_id_ = 2048;
		}
		LOG_API("current we support maxium channel: %d", max_channel_id_);

		return true;
    }

    LOG_API("We've tried %lu connection_uri, but failed!", connect_uris_.size());

    is_connected_ = false;
    return false;
}

// connect: amqp://tibank:%s@127.0.0.1:5672/tibank_host; ...
// passwd:  1234
bool mq_parse_connect_uris(const std::string& uris, const std::string& passwd, std::vector<std::string>& conn_vec) {
    
	std::vector<std::string> vec;
    boost::split(vec, uris, boost::is_any_of(";"));
    for (std::vector<std::string>::iterator it = vec.begin(); it != vec.cend(); ++it){
        std::string tmp = boost::trim_copy(*it);
        if (tmp.empty())
            continue;
		
        char connBuf[2048] = { 0, };
        snprintf(connBuf, sizeof(connBuf), tmp.c_str(), passwd.c_str());
        conn_vec.push_back(connBuf);
    }

    if (conn_vec.empty()) {
        LOG_API("Invalid or empty MQ connect url");
        return false;
    }
	
	return true;
}


// 默认的生产者、消费者的通道配置函数
bool mq_setup_channel_consume_default(RabbitChannelPtr pChannel, void* pArg) {

    if (!pChannel) {
        LOG_API("RabbitChannelPtr nullptr!");
        return false;
    }

    if (!pArg) {
        LOG_API("Empty pArg!");
        return false;
    }

    rabbitmq_character_t* p_rabbitmq = static_cast<rabbitmq_character_t *>(pArg);
    if (pChannel->declareExchange(p_rabbitmq->exchange_name_, "direct", false/*passive*/, true/*durable*/, false/*auto_delete*/) < 0) {
        LOG_API("declareExchange %s Error!", p_rabbitmq->exchange_name_.c_str());
        return false;
    }

    uint32_t msg_cnt = 0;
    uint32_t cons_cnt = 0;
    if (pChannel->declareQueue(p_rabbitmq->queue_name_, msg_cnt, cons_cnt, false/*passive*/, true/*durable*/, false/*exclusive*/, false/*auto_delete*/) < 0) {
        LOG_API("Declare Queue %s Failed!", p_rabbitmq->queue_name_.c_str());
        return false;
    }

    LOG_API("Broker report info: msg_cnt->%d, cons_cnt->%d", msg_cnt, cons_cnt);

    // 路邮键 paybill
    if (pChannel->bindQueue(p_rabbitmq->queue_name_, p_rabbitmq->exchange_name_, p_rabbitmq->route_key_)) {
        LOG_API("bindExchange Error!");
        return false;
    }

    if (pChannel->basicQos(1, true) < 0) {
        LOG_API("basicQos Failed!");
        return false;
    }

    if (pChannel->basicConsume(p_rabbitmq->queue_name_, "*", false/*no_local*/, false/*no_ack*/, false/*exclusive*/) < 0) {
        LOG_API("BasicConosume queue %s Failed!", p_rabbitmq->queue_name_.c_str());
        return false;
    }

    LOG_API("RabbitMQHandler init ok!");
    return true;
}


bool mq_setup_channel_publish_default(RabbitChannelPtr pChannel, void* pArg) {

    if (!pChannel) {
        LOG_API("RabbitChannelPtr nullptr!");
        return false;
    }

    if (!pArg) {
        LOG_API("Empty pArg!");
        return false;
    }

    rabbitmq_character_t* p_rabbitmq = static_cast<rabbitmq_character_t *>(pArg);
    if (pChannel->declareExchange(p_rabbitmq->exchange_name_, "direct", false/*passive*/, true/*durable*/, false/*auto_delete*/) < 0) {
        LOG_API("declareExchange %s Error!", p_rabbitmq->exchange_name_.c_str());
        return false;
    }

    uint32_t msg_cnt = 0;
    uint32_t cons_cnt = 0;
    if (pChannel->declareQueue(p_rabbitmq->queue_name_, msg_cnt, cons_cnt, false/*passive*/, true/*durable*/, false/*exclusive*/, false/*auto_delete*/) < 0) {
        LOG_API("Declare Queue %s Failed!", p_rabbitmq->queue_name_.c_str());
        return false;
    }

    LOG_API("Broker report info: msg_cnt->%d, cons_cnt->%d", msg_cnt, cons_cnt);

    // 路邮键 paybill
    if (pChannel->bindQueue(p_rabbitmq->queue_name_, p_rabbitmq->exchange_name_, p_rabbitmq->route_key_)) {
        LOG_API("bindExchange Error!");
        return false;
    }

    if (pChannel->setConfirmSelect() < 0) {
        std::cout << "Setting publish confirm failed!" << std::endl;
        return false;
    }

    LOG_API("PbiRabbitMQHandler init ok!");
    return true;
}



} // namespace AMQP

