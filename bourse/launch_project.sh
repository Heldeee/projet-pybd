#!/bin/bash

# Default option
option="start"

# Parse command-line arguments
while getopts ":o:" opt; do
  case ${opt} in
    o )
      option="$OPTARG"
      ;;
    \? )
      echo "Usage: $0 [-o option]" >&2
      exit 1
      ;;
    : )
      echo "Invalid option: $OPTARG requires an argument" >&2
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

# Modify the Python file based on the selected option
if [ "$option" == "reload" ]; then
  sed -i '/^    load_everything()$/s/^/#/' analyzer/analyzer.py
elif [ "$option" == "start" ]; then
  sed -i 's/^#    load_everything()$/    load_everything()/' analyzer/analyzer.py
else
  echo "Invalid option: $option. Valid options are 'start' or 'reload'."
  exit 1
fi

cd docker
docker compose down
echo "Shutting down docker compose images. Done."

echo -n "Make: analyzer. "
cd analyzer
make > /dev/null 2>&1
echo "Done."

echo -n "Make: dashboard. "
cd ../dashboard
make > /dev/null 2>&1
echo "Done."

cd ..
docker compose up