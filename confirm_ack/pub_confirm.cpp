#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>
#include <sstream>

#include "../RabbitMQ.h"

bool setupChannel(AMQP::RabbitChannelPtr pChannel, void* pArg) {

	if (!pChannel) {
		std::cout << "nullptr Error!" << std::endl;
		return false;
	}

    if (pChannel->setConfirmSelect() < 0) {
        std::cout << "Setting publish confirm failed!" << std::endl;
        return false;
    }

	return true;
}


int main(int argc, char* argv[]) {

    ::srand((unsigned)::time(NULL));

    //AMQP::RabbitMQ mq("amqp://paybank:paybank@127.0.0.1:5672/paybank");
    // sudo rabbitmqctl set_permissions -p paybank paybank ".*" ".*" ".*"
    AMQP::RabbitMQHelper mq("amqp://paybank:paybank@127.0.0.1:5672/paybank");
    if (mq.doConnect() < 0) {
        std::cout << "Connect Error!" << std::endl;
        return -1;
    }

	amqp_channel_t t = mq.createChannel();
	if (t <= 0) {
        std::cout << "Create channel failed!" << std::endl;
        return -1;
    }

    if (mq.setupChannel(t, setupChannel, NULL) < 0) {
        std::cout << "Setup channel failed!" << std::endl;
		mq.freeChannel(t);
        return -1;
    }

    std::stringstream msg;
    msg << "桃子最帅+:" << ::rand() % 1000000;

    if(mq.basicPublish(t, "hello-exchange", "*", true/* mandatory */, false/* immediate */, msg.str()) < 0) {
        std::cout << "publis error!" << std::endl;
        return -1;
    }

    std::cout << "OK!" << std::endl;


    return 0;
}
