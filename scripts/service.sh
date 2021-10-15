#!/bin/bash
set -e

APP_DIR="{BASEPATH}"
PID_DIR="/run"
START_LOG_FILE="/tmp/db-monitor.log"
DAEMON_USER="root"

BIN_NAME="universe"
SERVICE="db-monitor"

###############

USAGE="Usage: $0 {start|stop|restart|status|check} [submitter|monitor] [--force]"
FORCE_OP=true

pid_file_exists() {
    [ -f "$PID_FILE" ]
}

get_pid() {
    echo "$(cat "$PID_FILE")"
}

is_running() {
    PID=$(get_pid)
    ! [ -z "$(ps aux | awk '{print $2}' | grep "^$PID$")" ]
}

start_it() {
    mkdir -p "$PID_DIR"
    echo -e "\nStarting $SERVICE ..."
    cd ${APP_DIR}
    GOMAXPROCS=4 ./bin/$BIN_NAME $SERVICE &

    sleep 2
    pid=$(ps aux | grep $SERVICE | grep Sl | awk '{print $2}')
    if [[ "${pid}" == "" ]] ; then
        echo "$SERVICE start FAILED!!!"
        exit 1
    else
        echo -e "$pid" > "$PID_FILE"
    fi
    echo "$SERVICE started with pid $(cat "$PID_FILE")"
}

stop_process() {
    if pid_file_exists
    then
        PID=$(get_pid)
        echo "pid file $PID_FILE exists, we kill $PID"
        kill -9 $PID
    else
        echo "no pid file $PID_FILE exists, we kill $BIN_NAME directly"
        kill -9 $BIN_NAME
    fi
    sleep 1
}

remove_pid_file() {
    rm -f "$PID_FILE"
}

start_app() {
    if pid_file_exists
    then
        if is_running
        then
            PID=$(get_pid)
            echo "$SERVICE already running with pid $PID"
            exit 1
        else
            echo "$SERVICE stopped, but pid file exists"
            echo "Forcing start anyways"
            remove_pid_file
            start_it
        fi
    else
        start_it
    fi
}

stop_app() {
    echo -e ""
    if pid_file_exists
    then
        if is_running
        then
            echo "Stopping $SERVICE ..."
            stop_process
            remove_pid_file
            echo "$SERVICE stopped"
        else
            echo "$SERVICE already stopped, but pid file exists"
            if [ $FORCE_OP = true ]
            then
                echo "Forcing stop anyways ..."
                remove_pid_file
                echo "$SERVICE stopped"
            # else
            #     exit 1
            fi
        fi
    else
        echo "$SERVICE already stopped, pid file does not exist"
        # exit 1
    fi
}

status_app() {
    if pid_file_exists
    then
        if is_running
        then
            PID=$(get_pid)
            echo "$SERVICE running with pid $PID"
            exit 0
        else
            echo "$SERVICE stopped, but pid file exists"
        fi
    else
        echo "$SERVICE stopped"
    fi
    exit 1
}


PID_FILE="$PID_DIR/$SERVICE.pid"
case "$1" in
    start)
        start_app
    ;;

    stop)
        stop_app
    ;;

    restart)
        FORCE_OP=true
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
