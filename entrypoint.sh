#!/bin/sh
# No args: run the bridge with the config and registration mounted at /data, or,
# if there's no config yet, print an example config to stdout and exit.
# With args (e.g. -c /path -r /path -g): run the bridge with those args.
if [ $# -eq 0 ]; then
	if [ -f /data/config.yaml ]; then
		exec /app/jmpchat-matrix-bridge -c /data/config.yaml -r /data/registration.yaml
	fi
	echo "No config found at /data/config.yaml. Outputting example config to stdout." >&2
	echo "Save this to a file, edit it, then run the container with the config and registration mounted at /data." >&2
	exec /app/jmpchat-matrix-bridge -e -c -
fi
exec /app/jmpchat-matrix-bridge "$@"
