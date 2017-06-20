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

    //AMQP::RabbitMQ mq("amqp://paybank:paybank@127.0.0.1:5672/paybank");
    // sudo rabbitmqctl set_permissions -p paybank paybank ".*" ".*" ".*"
    AMQP::RabbitMQHelper mq("amqp://paybank:paybank@127.0.0.1:5672/paybank");
    if (mq.doConnect() < 0) {
        std::cout << "Connect Error!" << std::endl;
        return -1;
    }

    AMQP::RabbitChannel ch = AMQP::RabbitChannel(mq);
    if (ch.initChannel() < 0) {
        std::cout << "Create channel failed!" << std::endl;
        return -1;
    }
    if (ch.setConfirmSelect() < 0) {
        std::cout << "Setting publish confirm failed!" << std::endl;
        return -1;
    }

    std::stringstream msg;
    msg << "桃子最帅+:" << ::rand() % 1000000;

    if(ch.basicPublish("hello-exchange", "*", true/* mandatory */, false/* immediate */, msg.str()) < 0) {
        std::cout << "publis error!" << std::endl;
        return -1;
    }

    std::cout << "OK!" << std::endl;


    return 0;
}
