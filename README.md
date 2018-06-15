Another RabbitMQ C++ client wrapper based on librabbitmq-c.   
Already used in production environment.    

No document, please read examples for reference, but I really recommend it to you!    

Append: Better usage example, please refer to the project [tibank](https://github.com/taozhijiang/tibank).    

### test environment setup:   
```bash
[nicol@centos_boost rabbitmq_cpp_wrapper]$ sudo rabbitmqctl add_user tibank 1234            
Creating user "tibank" ...
[nicol@centos_boost rabbitmq_cpp_wrapper]$ sudo rabbitmqctl set_user_tags tibank administrator                  
Setting tags for user "tibank" to [administrator] ...
[nicol@centos_boost rabbitmq_cpp_wrapper]$ sudo rabbitmqctl add_vhost tibank_host
Creating vhost "tibank_host" ...
[nicol@centos_boost rabbitmq_cpp_wrapper]$ sudo rabbitmqctl set_permissions -p tibank_host tibank ".*" ".*" ".*"    
Setting permissions for user "tibank" in vhost "tibank_host" ...
[nicol@centos_boost rabbitmq_cpp_wrapper]$ sudo rabbitmq-plugins enable rabbitmq_management
Plugin configuration unchanged.

Applying plugin configuration to rabbit@centos_boost... nothing to do.

[nicol@centos_boost rabbitmq_cpp_wrapper]$ 
```

character.exchange_name_ = "hello-exchange";    
character.queue_name_ = "hello-queue";    
character.route_key_ = "hello-key";    