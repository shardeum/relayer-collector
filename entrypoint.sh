#!/bin/bash -x

NO_OF_SERVERS=${NO_OF_SERVERS:-1}

case ${1} in
  "server")
    SERVER_PORT=6001
    for i in $(seq $NO_OF_SERVERS); do
      pm2 start --daemon --name ldrpc-server-$i npm -- run server ${SERVER_PORT}
      SERVER_PORT=$(( SERVER_PORT  1 ))
    done
    ;;

  "collector" | "log_server")
    pm2 start --daemon --name ldrpc-${1} npm -- run ${1}
    ;;
  *)
    echo "Service ${1} is not recognizable."
    exit 1
    ;;
esac

exec pm2 logs
