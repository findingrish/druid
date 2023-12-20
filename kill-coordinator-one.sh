#!/bin/bash

# Get the process ID (PID) of the Java process on the given port
pid=$(lsof -iTCP:8081 -sTCP:LISTEN | awk '{print $2}')

# Check if the PID was found
if [[ -z "$pid" ]]; then
  echo "No Java process found on port $1."
  exit 1
fi

# Kill the Java process with SIGTERM (graceful termination)
kill -TERM $pid

# Check if the process was successfully terminated
if [[ $? -eq 0 ]]; then
  echo "Java process on port $1 terminated successfully."
else
  echo "Failed to terminate Java process on port $1. Try with SIGKILL (-9) if necessary."
fi

# Optionally, kill the process forcefully with SIGKILL (if SIGTERM fails)
kill -9 $pid