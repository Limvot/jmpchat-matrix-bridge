#!/bin/sh
# If no args: when default config is missing, output example config and exit.
# With args (e.g. -c /path -r /path -g): run the bridge with those args.
if [ $# -eq 0 ]; then
	if [ ! -f /app/config.yaml ]; then
		echo "No config found at /app/config.yaml. Outputting example config to stdout." >&2
		echo "Save this to a file, edit it, then run the container with the config and registration mounted." >&2
		exec /app/mautrix-jabber -e -c -
	fi
fi
exec /app/mautrix-jabber "$@"
