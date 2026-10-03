#!/bin/bash

case "$1" in
    start)
        docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
        ;;
    stop)
        docker stop rabbitmq
        ;;
    logs)
        docker logs -f rabbitmq
        ;;
    *)
        echo "Usage: $0 {start|stop|logs}"
        exit 1
esac
