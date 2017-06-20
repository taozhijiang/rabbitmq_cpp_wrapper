#!/bin/bash

g++ -g -O0 -std=c++0x RabbitMQ.cpp -c || exit

echo "basic"
g++ -g -O0 -std=c++0x basic/pub.cpp -o pub RabbitMQ.o -lrabbitmq || exit
g++ -g -O0 -std=c++0x basic/cus.cpp -o cus RabbitMQ.o -lrabbitmq || exit

echo "batch_perf"
g++ -g -O0 -std=c++0x batch_perf/batch_pub.cpp -o batch_pub RabbitMQ.o -lrabbitmq -lpthread || exit

echo "get"
g++ -g -O0 -std=c++0x basic_get/get.cpp -o get RabbitMQ.o -lrabbitmq -lpthread || exit

echo "Done!"
