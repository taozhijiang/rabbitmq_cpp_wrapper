#!/bin/bash

g++ -g -O0 -I./include/ -std=c++0x RabbitMQ.cpp -c || exit

echo "basic"
g++ -g -O0 -I./include/ -L./ -std=c++0x basic/pub.cpp -o pub RabbitMQ.o -lrabbitmq -lpthread -lrt || exit
g++ -g -O0 -I./include/ -L./ -std=c++0x basic/cus.cpp -o cus RabbitMQ.o -lrabbitmq -lpthread -lrt || exit
g++ -g -O0 -I./include/ -L./ -std=c++0x basic/cus_timeout.cpp -o cus_timeout RabbitMQ.o -lrabbitmq -lpthread -lrt || exit

echo "batch_perf"
g++ -g -O0 -I./include/ -L./ -std=c++0x batch_perf/batch_pub.cpp -o batch_pub RabbitMQ.o -lrabbitmq -lpthread -lrt || exit

echo "get"
g++ -g -O0 -I./include/ -L./ -std=c++0x basic_get/get.cpp -o get RabbitMQ.o -lrabbitmq -lpthread -lrt || exit

echo "confirm_ack"
g++ -g -O0 -I./include/ -L./ -std=c++0x confirm_ack/pub_confirm.cpp -o pub_confirm RabbitMQ.o -lrabbitmq -lpthread -lrt || exit
g++ -g -O0 -I./include/ -L./ -std=c++0x confirm_ack/batch_pub_confirm.cpp -o batch_pub_confirm RabbitMQ.o -lrabbitmq -lpthread -lrt || exit
g++ -g -O0 -I./include/ -L./ -std=c++0x confirm_ack/cus_ack.cpp -o cus_ack RabbitMQ.o -lrabbitmq -lpthread -lrt || exit
g++ -g -O0 -I./include/ -L./ -std=c++0x confirm_ack/cus_ack2.cpp -o cus_ack2 RabbitMQ.o -lrabbitmq -lpthread -lrt || exit


echo "Done!"
