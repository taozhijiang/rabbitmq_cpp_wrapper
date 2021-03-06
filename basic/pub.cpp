#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>

#include "../RabbitMQ.h"


int main(int argc, char* argv[]) {
    std::cout << "GB" << std::endl;
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

    std::string msg("taozj最帅了");

    if(mq.basicPublish(t, "hello-exchange", "*", false/* mandatory */, false/* immediate */, msg) < 0) {
        std::cout << "publish error!" << std::endl;
        return -1;
    }

    std::cout << "OK!" << std::endl;


    return 0;
}
