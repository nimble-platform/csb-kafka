    The CSB module contains 3 initial classes
    1. Consumer - subscribes handlers to topics - start method needs to be called
    2. Producer - Send data to topics (in bulks or immidiate)
    3. TopicCreator - Used for creating a topic (in a diferent blocking thread)

#### Basic REST producer consumes flow
When a consumer is subscribing to a topic named 'test_topic' then CSB will store the given url  
of the handler and when a message has been sent (by the producer) to the 'test_topic' CSB will  
activate the URL callback path and append the sent data to it. if for example if the handler  
URL was `http://127.0.0.1:9191/handler/test_topic/` and data 'test_data' then it will create  
a post command `http://127.0.0.1:9191/handler/test_topic/test_data` 

#### REST CSB
---
#### Registering a handler to a specific topic
By sending a post command with one of the following formats
* [server_ip:server_port]/consumer/register?topic=[topic_name]&handler=[handler_url]
* [server_ip:server_port]/consumer/register/[topic_name]?handler=[handler_url]

For example `http://127.0.0.1:4567/consumer/register/rest_topic?handler=http://127.0.0.1:9191/handler/rest_topic/`

#### Sending data to a specific topic
By sending a post command with one of the following formats
* [server_ip:server_port]/producer/send?topic=[topic_name]&message=[my_message]
* [server_ip:server_port]/producer/send/[topic_name]/[my_message]

For example `http://127.0.0.1:4567/producer/send?topic=rest_topic&message=test%20message`
