#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>
#include <sstream>

#include "../RabbitMQ.h"


int main(int argc, char* argv[]) {

    ::srand((unsigned)::time(NULL));

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
                std::bind(AMQP::mq_setup_channel_publish_default, std::placeholders::_1, std::placeholders::_2), 
                &character) < 0) {
        std::cout << "Setup channel failed!" << std::endl;
		mq.freeChannel(t);
        return -1;
    }

    std::stringstream msg;
    msg << "default_publish桃子最帅+:" << ::rand() % 1000000;

    if(mq.basicPublish(t, "hello-exchange", "*", true/* mandatory */, false/* immediate */, msg.str()) < 0) {
        std::cout << "publis error!" << std::endl;
        return -1;
    }

    std::cout << "OK!" << std::endl;


    return 0;
}
