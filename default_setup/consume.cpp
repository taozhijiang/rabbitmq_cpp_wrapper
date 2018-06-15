#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>

#include "../RabbitMQ.h"



int main(int argc, char* argv[]) {

    AMQP::RabbitMQHelper mq("amqp://tibank:1234@127.0.0.1:5672/tibank_host");
    if (mq.doConnect() < 0) {
        std::cout << "Connect Error!" << std::endl;
        return -1;
    }

	amqp_channel_t t = mq.createChannel();
	if (t <= 0) {
        std::cout << "Create channel failed!" << std::endl;
        return -1;
    }

    AMQP::rabbitmq_character_t character {};
    character.exchange_name_ = "hello-exchange";
    character.queue_name_ = "hello-queue";
    character.route_key_ = "hello-key";

    if (mq.setupChannel(t, 
                std::bind(AMQP::mq_setup_channel_consume_default, std::placeholders::_1, std::placeholders::_2), 
                &character) < 0) {
        std::cout << "Setup channel failed!" << std::endl;
		mq.freeChannel(t);
        return -1;
    }

    AMQP::RabbitMessage rabbitMsg;
    while (true) {
        if(mq.basicConsumeMessage(rabbitMsg, NULL, 0) < 0) {
retry_1:
            if (!mq.isConnectionOpen()) {
                if (mq.doConnect() < 0) {
                    std::cout << "Connect Error!" << std::endl;
                    ::sleep(1);
                    goto retry_1;
                }
            }
retry_2:
            if (!mq.isChannelOpen(t)) {
				mq.freeChannel(t);
				t = mq.createChannel();
                if (t <= 0) {
                    std::cout << "Create channel failed!" << std::endl;
                    ::sleep(1);
                    goto retry_2;
                }

                if (mq.setupChannel(t, 
                            std::bind(AMQP::mq_setup_channel_consume_default, std::placeholders::_1, std::placeholders::_2), 
                            &character) < 0) {
					mq.freeChannel(t);
                    std::cout << "Setup channel failed!" << std::endl;
                    ::sleep(1);
                    goto retry_2;
                }
            }
        }

        std::cout << "RECV:" <<  std::string((const char*)(rabbitMsg.content().bytes), rabbitMsg.content().len) << "]" << std::endl;
        mq.basicAck(t, rabbitMsg.envelope.delivery_tag);
        std::cout << "ACK:" << rabbitMsg.envelope.delivery_tag << std::endl;
        rabbitMsg.safe_clear();
    }

    return 0;
}
