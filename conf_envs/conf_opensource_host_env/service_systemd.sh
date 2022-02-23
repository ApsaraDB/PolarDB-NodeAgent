#!/bin/bash

SERVICE="pdb_ue"

###############

USAGE="Usage: $0 {start|stop|restart|status|check}"

start_app() {
    systemctl start $SERVICE
}

stop_app() {
    systemctl stop $SERVICE
}

status_app() {
    systemctl status $SERVICE
}

case "$1" in
    start)
        start_app
    ;;

    stop)
        stop_app
    ;;

    restart)
        stop_app
        start_app
    ;;

    status)
        status_app
    ;;

    check)
        status_app
    ;;

    *)
        echo $USAGE
        exit 1
    ;;
esac
