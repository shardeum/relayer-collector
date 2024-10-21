#!/bin/bash -x

# Default number of servers to start if not set in the environment
NO_OF_SERVERS=${NO_OF_SERVERS:-1}

# Function to start multiple collector servers using PM2
start_collector_server() {
  local server_port=6001

  for i in $(seq "$NO_OF_SERVERS"); do
    pm2 start --daemon --name "ldrpc-server-$i" npm -- run server "$server_port"
    server_port=$((server_port + 1))
  done
}

# Function to start a collector using PM2
start_collector() {
  pm2 start --daemon --name "ldrpc-collector" npm -- run collector
}

# Function to start a log server using PM2
start_log_server() {
  pm2 start --daemon --name "ldrpc-log_server" npm -- run log_server
}

# Main script execution based on the input argument
case "$1" in
"server")
  start_collector_server
  ;;
"collector")
  start_collector
  ;;
"log_server")
  start_log_server
  ;;
*)
  echo "Error: Service '$1' is not recognizable."
  exit 1
  ;;
esac

# Tail PM2 logs to keep the Docker container running
exec pm2 logs
