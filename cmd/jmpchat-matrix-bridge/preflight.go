// Adapted from github.com/keplersj/mautrix-xmpp (MIT, see LICENSE-THIRD-PARTY).

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"

	"golang.org/x/sys/unix"
	flag "maunium.net/go/mauflag"
)

// preflight checks the config file before mxmain loads it, so the common deployment mistakes get a
// clear message, and so a read-only config (for example a Kubernetes Secret mount) is used without
// trying to rewrite it. Flags that don't load a config (help, version, example config, "-c -") skip it.
func preflight() {
	args := os.Args[1:]
	for _, a := range args {
		switch a {
		case "-h", "--help", "-v", "--version", "--version-json", "-e", "--generate-example-config":
			return
		}
	}
	path, ok := configPathFromArgs(args)
	if !ok {
		path = "config.yaml"
	}
	if path == "-" {
		return
	}
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		fail("config file %s does not exist. Generate one with `jmpchat-matrix-bridge -e` "+
			"(Docker: run the image without arguments to print an example config).", path)
	} else if err != nil {
		fail("can't read config file %s: %v", path, err)
	} else if info.IsDir() {
		fail("config path %s is a directory, expected a YAML file", path)
	}
	if f, err := os.Open(path); err != nil {
		fail("can't read config file %s: %v", path, err)
	} else {
		_ = f.Close()
	}
	// mxmain refuses -g together with --no-update, so for -g let it report the write failure itself.
	generating := slices.Contains(args, "-g") || slices.Contains(args, "--generate-registration")
	if !generating && !slices.Contains(args, "-n") && !slices.Contains(args, "--no-update") && !writable(path) {
		_, _ = fmt.Fprintln(os.Stderr, "jmpchat-matrix-bridge: config file is read-only, running with --no-update")
		// mauflag copied os.Args at init time, so update its copy too.
		os.Args = append(os.Args, "--no-update")
		flag.DefaultSet().InputArgs = os.Args[1:]
	}
}

func writable(path string) bool {
	if unix.Access(path, unix.W_OK) != nil {
		return false
	}
	dir := "."
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		dir = path[:i+1]
	}
	return unix.Access(dir, unix.W_OK) == nil
}

func configPathFromArgs(args []string) (string, bool) {
	for i, a := range args {
		switch {
		case a == "-c" || a == "--config":
			if i+1 < len(args) {
				return args[i+1], true
			}
		case strings.HasPrefix(a, "--config="):
			return strings.TrimPrefix(a, "--config="), true
		case strings.HasPrefix(a, "-c") && len(a) > 2 && !strings.HasPrefix(a, "--"):
			return strings.TrimPrefix(strings.TrimPrefix(a, "-c"), "="), true
		}
	}
	return "", false
}

func fail(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "jmpchat-matrix-bridge: "+format+"\n", args...)
	os.Exit(10)
}
