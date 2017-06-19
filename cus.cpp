#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>

#include "RabbitMQ.h"


amqp_channel_t setupChannel(AMQP::RabbitChannel& ch) {

    if( ch.declareExchange("hello-exchange", "direct", false, true, false) < 0) {
        std::cout << "declareExchange Error!" << std::endl;
        return -1;
    }

    uint32_t msg_cnt;
    uint32_t cons_cnt;
    if(ch.declareQueue("hello-queue", msg_cnt, cons_cnt, false, true, false, false) < 0){
        std::cout << "Declare Queue Failed!" << std::endl;
        return -1;
    }
    std::cout << ":" << msg_cnt << ", " << cons_cnt << std::endl;

    if (ch.bindQueue("hello-queue", "hello-exchange", "*")) {
        std::cout << "bindExchange Error!" << std::endl;
        return -1;
    }

    if (ch.basicQos(1, true) < 0) {
        std::cout << "basicQos Failed!" << std::endl;
        return -1;
    }

    if (ch.basicConsume("hello-queue", "*", 0, 1, 0) < 0) {
        std::cout << "BasicConosume Failed!" << std::endl;
        return -1;
    }

    return 0;
}

int main(int argc, char* argv[]) {

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

    if (setupChannel(ch) < 0) {
        std::cout << "Setup channel failed!" << std::endl;
        return -1;
    }

    std::string retStr;
    while (true) {
        if(mq.basicConsumeMessage(retStr, NULL, 0) < 0) {
retry_1:
            if (!mq.isConnectionOpen()) {
                if (mq.doConnect() < 0) {
                    std::cout << "Connect Error!" << std::endl;
                    ::sleep(1);
                    goto retry_1;
                }
            }
retry_2:
            if (!ch.isChannelOpen()) {
                if (ch.initChannel() < 0) {
                    std::cout << "Create channel failed!" << std::endl;
                    ::sleep(1);
                    goto retry_2;
                }

                if (setupChannel(ch) < 0) {
                    std::cout << "Setup channel failed!" << std::endl;
                    ::sleep(1);
                    goto retry_2;
                }
            }
        }

        std::cout << "RECV:" <<  retStr << std::endl;
    }

    return 0;
}
