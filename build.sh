#!/bin/bash

g++ -g -O0 -std=c++0x RabbitMQ.cpp -c 
g++ -g -O0 -std=c++0x pub.cpp -o pub RabbitMQ.o -lrabbitmq
g++ -g -O0 -std=c++0x cus.cpp -o cus RabbitMQ.o -lrabbitmq
g++ -g -O0 -std=c++0x batch_pub.cpp -o batch_pub RabbitMQ.o -lrabbitmq -lpthread
