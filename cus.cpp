#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>

#include "RabbitMQ.h"


amqp_channel_t setupChannel(AMQP::RabbitMQHelper& mq) {

    amqp_channel_t t = mq.createChannel();
    if (t <= 0) {
        std::cout << "Create channel failed!" << std::endl;
        return -1;
    }

    if( mq.declareExchange(t, "hello-exchange", "direct", false, true, false) < 0) {
        std::cout << "declareExchange Error!" << std::endl;
        return -1;
    }

    uint32_t msg_cnt;
    uint32_t cons_cnt;
    if(mq.declareQueue(t, "hello-queue", msg_cnt, cons_cnt, false, true, true, false) < 0){
        std::cout << "Declare Queue Failed!" << std::endl;
        return -1;
    }
    std::cout << ":" << msg_cnt << ", " << cons_cnt << std::endl;

    if (mq.bindQueue(t, "hello-queue", "hello-exchange", "*")) {
        std::cout << "bindExchange Error!" << std::endl;
        return -1;
    }

    if (mq.basicQos(t, "*", 1, 1) < 0) {
        std::cout << "basicQos Failed!" << std::endl;
        return -1;
    }

    if (mq.basicConsume(t, "hello-queue", "*", 0, 1, 0) < 0) {
        std::cout << "BasicConosume Failed!" << std::endl;
        return -1;
    }

    return 0;
}

int main(int argc, char* argv[]) {

    //AMQP::RabbitMQ mq("amqp://uuuu:pppp@127.0.0.1:5672/zzzz");
    // sudo rabbitmqctl set_permissions -p zzzz uuuu ".*" ".*" ".*"
    AMQP::RabbitMQHelper mq("amqp://uuuu:uuuu@127.0.0.1:5672/zzzz");
    if (mq.connect() < 0) {
        std::cout << "Connect Error!" << std::endl;
        return -1;
    }

    amqp_channel_t t;
    if ((t = setupChannel(mq)) < 0) {
        std::cout << "Setup Channel Error!" << std::endl;
        return -1;
    }

    std::string retStr;
    while (true) {
        if(mq.basicConsumeMessage(retStr, NULL, 0) < 0) {
retry_1:
            if (!mq.isConnectionOpen()) {
                if (mq.connect() < 0) {
                    std::cout << "Connect Error!" << std::endl;
                    ::sleep(1);
                    goto retry_1;
                }
            }
retry_2:
            if ( (t<0) || ( t>=1 && !mq.isChannelOpen(t)) ) {
                if ((t = setupChannel(mq)) < 0) {
                    std::cout << "Setup Channel Error!" << std::endl;
                    ::sleep(1);
                    goto retry_2;
                }
            }
        }

        std::cout << "RECV:" <<  retStr << std::endl;
    }

    return 0;
}
