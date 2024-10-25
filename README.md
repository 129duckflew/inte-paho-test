This repository contains a minimal reproducible example to demonstrate the issue described in my StackOverflow question.
question link: https://stackoverflow.com/questions/79112339/paho-client-exceptioninternal-error-caused-by-no-new-message-ids-being-availab

## Test method:
1. Start an mqtt broker instance on port 1883 of the local machine. I am using emqx here.
    ```yaml
    version: '3.1'
    networks:
    services:
      emqx:
        container_name: emqx
        image: emqx/emqx:latest
        ports:
          - 1883:1883
          - 8083:8083
          - 8084:8084
          - 18083:18083
        volumes:
          - emqx_local:/opt/emqx
        restart: always
    volumes:
      emqx_local:
     ```
2. Update the IP address of the mqtt broker server in the SpringBoot configuration file and the IP address corresponding to the docker service.
    ```yaml
    mqtt:
      client-id-inbound: rms-inbound-test
      client-id-outbound: rms-outbound-test
      url: tcp://${your_ip}:1883
      username: rms
      password: 123456
    ```
3. Start the SpringBoot program
4. Request the /test interface on port 8080 of the local machine and pass in the parameter num=100,000
Follow the steps above and we will receive a `Caused by: Internal error, caused by no new message IDs being available (32001)`
```bash
curl -X POST "http://127.0.0.1:8080/test?num=100000"

```
