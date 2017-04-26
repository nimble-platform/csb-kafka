## CSB Service
    The CSB service contains 3 initial classes
    1. Consumer - subscribes handlers to topics - start method needs to be called
    2. Producer - Sends data to topics (in bulks or immidiate)
    3. TopicCreator - Used for creating a topic (in a diferent blocking thread)

#### Short description of REST producer consumes flow
When a consumer is subscribing to a topic named 'my_topic' then CSB will store the given url  
of the handler and when a message has been sent to that topic (by the producer) CSB will  
activate the URL callback path and append the sent data to it. if for example the handler  
URL was `http://127.0.0.1:9191/handler/test_topic/` and data 'test_data' then it will create  
a POST command on `http://127.0.0.1:9191/handler/test_topic/test_data` 

#### REST CSB
---
#### Checking the service and up
Send a GET command on `[csb_url]/` Will return "Hello from CSB-Service"

#### To Get the list of the available topics 
Send a GET command on `[csb_url]/consumer/topics`

#### To create a new topic
Send a POST command on `[csb_url]/create/[topic_name]`

#### To register a handler to a topic
Send a POST command on `[csb_url]/consumer/subscribe`
With query parameters:
* topic - The topic to subscribe to
* handler - The handler url (which will receive notifications)  
e.g. `[csb_url]/consumer/subscribe?topic=test_topic&handler=127.0.0.1:9991/test_topic`  

#### To send data to a topic
Send a POST command on `[csb_url]/send/{topic_name}`
With topic_name as path parameter and message as query parameter  
e.g. `[csb_url]/producer/test_topic?message=my_test_message`
