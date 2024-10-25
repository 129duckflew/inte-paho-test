This repository contains a minimal reproducible example to demonstrate the issue described in my StackOverflow question.
question link: https://stackoverflow.com/questions/79112339/paho-client-exceptioninternal-error-caused-by-no-new-message-ids-being-availab

## Test method:
1. Start an mqtt broker instance on port 1883 of the local machine. I am using emqx here.
2. Start the SpringBoot program
3. Request the /test interface on port 8080 of the local machine and pass in the parameter num=100,000
Follow the steps above and we will receive a `Caused by: Internal error, caused by no new message IDs being available (32001)`
