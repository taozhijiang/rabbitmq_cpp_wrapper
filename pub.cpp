#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>

#include "RabbitMQ.h"


int main(int argc, char* argv[]) {
    std::cout << "GB" << std::endl;

    //AMQP::RabbitMQ mq("amqp://uuuu:pppp@127.0.0.1:5672/zzzz");
    // sudo rabbitmqctl set_permissions -p zzzz uuuu ".*" ".*" ".*"
    AMQP::RabbitMQHelper mq("amqp://uuuu:uuuu@127.0.0.1:5672/zzzz");
    if (mq.connect() < 0) {
        std::cout << "Connect Error!" << std::endl;
        return -1;
    }

    amqp_channel_t t = mq.createChannel();
    if (t > 0) {
        std::cout << "Created channel " << t << std::endl;
    } else {
        std::cout << "Create channel failed!" << std::endl;
        return -1;
    }

    std::string msg("taozj最帅了");

    if(mq.basicPublish(t, "hello-exchange", "*", 0, 0, msg) < 0) {
        std::cout << "publis error!" << std::endl;
        return -1;
    }

    std::cout << "OK!" << std::endl;



    return 0;
}
