#include <iostream>
#include <cstdio>
#include <string>
#include <cstring>
#include <cstdint>
#include <vector>
#include <map>

#include <sstream>

#include "RabbitMQ.h"

volatile unsigned long long test_count = 0;
bool start = false;
bool end = false;

void* thread_run(void* arg) {

    AMQP::RabbitMQHelper mq("amqp://paybank:paybank@127.0.0.1:5672/paybank");
    if (mq.doConnect() < 0) {
        std::cout << "Connect Error!" << std::endl;
        return NULL;
    }

    AMQP::RabbitChannel ch = AMQP::RabbitChannel(mq);
    if (ch.initChannel() < 0) {
        std::cout << "Create channel failed!" << std::endl;
        return NULL;
    }

    do {

        if(!start) {
            ::usleep(20);
            continue;
        }

        if(end) break;

        std::stringstream msg;
        msg << "桃子最帅+:" << ::rand() % 1000000;
        ++ test_count;

        if(ch.basicPublish("hello-exchange", "*", 0, 0, msg.str()) < 0) {
            std::cout << "publish error!" << std::endl;
            ::abort();
        }
    } while (true);

    return NULL;
}

int main(int argc, char *argv[]) {

    ::srand((unsigned)::time(NULL));

    pthread_t thread_ids[30];
    for (int i=0; i<30; i++) {
        pthread_create(&(thread_ids[i]), NULL,
                       (void *(*)(void *))thread_run , (void *)NULL);
	}
    ::sleep(2);
    int round = 3*60;
    start = true;
    time_t start= ::time(NULL);
    while(-- round)
        sleep(1);

    end = true;
    sleep(1);
    time_t t = ::time(NULL) - start;
    for (int i=0; i<30; i++) {
        pthread_join(thread_ids[i], NULL);
	}

    printf("!!!!!\nPerf: %f Q/Sec\n!!!!!\n", (double)test_count/t );
    printf("Total trans: %lld\n", test_count);

    return 0;
}
