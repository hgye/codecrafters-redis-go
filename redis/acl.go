package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
)

type aclUser struct {
	Flags     []string
	Passwords []string
	Commands  string
	Keys      string
	Channels  string
	Selectors []string
}

var (
	aclMu    sync.RWMutex
	aclUsers = map[string]aclUser{
		"default": {
			Flags:     []string{"on", "nopass", "allkeys", "allchannels", "allcommands"},
			Passwords: []string{},
			Commands:  "+@all",
			Keys:      "~*",
			Channels:  "&*",
			Selectors: []string{},
		},
	}
)

func aclWrongPassError() error {
	return errors.New("WRONGPASS invalid username-password pair or user is disabled")
}

func aclInitialAuthenticatedUser() string {
	aclMu.RLock()
	defer aclMu.RUnlock()

	user, ok := aclUsers["default"]
	if !ok {
		return ""
	}
	if !containsFlag(user.Flags, "on") {
		return ""
	}
	if containsFlag(user.Flags, "nopass") {
		return "default"
	}
	return ""
}

func aclAuthenticate(username, password string) error {
	aclMu.RLock()
	user, ok := aclUsers[username]
	aclMu.RUnlock()
	if !ok || !containsFlag(user.Flags, "on") {
		return aclWrongPassError()
	}

	if containsFlag(user.Flags, "nopass") {
		return nil
	}

	hashed := aclPasswordHash(password)
	for _, p := range user.Passwords {
		if p == hashed {
			return nil
		}
	}

	return aclWrongPassError()
}

func HandleAuth(args []string, currentUser *string) ([]byte, error) {
	if len(args) != 1 && len(args) != 2 {
		return nil, errors.New("ERR wrong number of arguments for 'auth' command")
	}

	username := "default"
	password := args[0]
	if len(args) == 2 {
		username = args[0]
		password = args[1]
	}

	if err := aclAuthenticate(username, password); err != nil {
		return nil, err
	}

	*currentUser = username
	return EncodeSimpleString("OK"), nil
}

func HandleACL(args []string, currentUser string) ([]byte, error) {
	if len(args) < 1 {
		return nil, errors.New("ERR wrong number of arguments for 'acl' command")
	}

	subcommand := strings.ToUpper(args[0])
	switch subcommand {
	case "WHOAMI":
		if len(args) != 1 {
			return nil, errors.New("ERR wrong number of arguments for 'acl|whoami' command")
		}
		if currentUser == "" {
			return EncodeBulkString("default"), nil
		}
		return EncodeBulkString(currentUser), nil
	case "GETUSER":
		if len(args) != 2 {
			return nil, errors.New("ERR wrong number of arguments for 'acl|getuser' command")
		}

		aclMu.RLock()
		user, ok := aclUsers[args[1]]
		aclMu.RUnlock()
		if !ok {
			return EncodeNullArray(), nil
		}
		return encodeACLUser(user), nil
	case "SETUSER":
		if len(args) < 2 {
			return nil, errors.New("ERR wrong number of arguments for 'acl|setuser' command")
		}

		username := args[1]
		aclMu.Lock()
		user, ok := aclUsers[username]
		if !ok {
			user = aclUser{Flags: []string{}, Passwords: []string{}, Commands: "", Keys: "", Channels: "", Selectors: []string{}}
		}

		for _, token := range args[2:] {
			tok := strings.ToLower(token)
			switch {
			case tok == "on":
				user.Flags = addFlag(removeFlag(user.Flags, "off"), "on")
			case tok == "off":
				user.Flags = addFlag(removeFlag(user.Flags, "on"), "off")
			case tok == "nopass":
				user.Flags = addFlag(user.Flags, "nopass")
				user.Passwords = []string{}
			case tok == "resetpass":
				user.Flags = removeFlag(user.Flags, "nopass")
				user.Passwords = []string{}
			case tok == "allkeys":
				user.Flags = addFlag(user.Flags, "allkeys")
				user.Keys = "~*"
			case tok == "resetkeys":
				user.Flags = removeFlag(user.Flags, "allkeys")
				user.Keys = ""
			case tok == "allchannels":
				user.Flags = addFlag(user.Flags, "allchannels")
				user.Channels = "&*"
			case tok == "resetchannels":
				user.Flags = removeFlag(user.Flags, "allchannels")
				user.Channels = ""
			case tok == "allcommands":
				user.Flags = addFlag(user.Flags, "allcommands")
				user.Commands = "+@all"
			case tok == "nocommands":
				user.Flags = removeFlag(user.Flags, "allcommands")
				user.Commands = ""
			case strings.HasPrefix(token, "~"):
				user.Flags = removeFlag(user.Flags, "allkeys")
				user.Keys = token
			case strings.HasPrefix(token, "&"):
				user.Flags = removeFlag(user.Flags, "allchannels")
				user.Channels = token
			case strings.HasPrefix(token, ">"):
				user.Flags = removeFlag(user.Flags, "nopass")
				user.Passwords = append(user.Passwords, aclPasswordHash(token[1:]))
			case strings.HasPrefix(token, "+") || strings.HasPrefix(token, "-"):
				user.Flags = removeFlag(user.Flags, "allcommands")
				if user.Commands == "" {
					user.Commands = token
				} else {
					user.Commands = user.Commands + " " + token
				}
			case tok == "reset":
				user = aclUser{Flags: []string{}, Passwords: []string{}, Commands: "", Keys: "", Channels: "", Selectors: []string{}}
			}
		}

		aclUsers[username] = user
		aclMu.Unlock()
		return EncodeSimpleString("OK"), nil
	default:
		return nil, errors.New("ERR unsupported ACL subcommand")
	}
}

func aclPasswordHash(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}

func containsFlag(flags []string, target string) bool {
	for _, f := range flags {
		if f == target {
			return true
		}
	}
	return false
}

func removeFlag(flags []string, target string) []string {
	out := make([]string, 0, len(flags))
	for _, f := range flags {
		if f != target {
			out = append(out, f)
		}
	}
	return out
}

func addFlag(flags []string, target string) []string {
	if containsFlag(flags, target) {
		return flags
	}
	return append(flags, target)
}

func encodeACLUser(u aclUser) []byte {
	flags := make([][]byte, 0, len(u.Flags))
	for _, flag := range u.Flags {
		flags = append(flags, EncodeBulkString(flag))
	}
	passwords := make([][]byte, 0, len(u.Passwords))
	for _, pwd := range u.Passwords {
		passwords = append(passwords, EncodeBulkString(pwd))
	}
	selectors := make([][]byte, 0, len(u.Selectors))
	for _, sel := range u.Selectors {
		selectors = append(selectors, EncodeBulkString(sel))
	}

	return EncodeRESPArray([][]byte{
		EncodeBulkString("flags"),
		EncodeRESPArray(flags),
		EncodeBulkString("passwords"),
		EncodeRESPArray(passwords),
		EncodeBulkString("commands"),
		EncodeBulkString(u.Commands),
		EncodeBulkString("keys"),
		EncodeBulkString(u.Keys),
		EncodeBulkString("channels"),
		EncodeBulkString(u.Channels),
		EncodeBulkString("selectors"),
		EncodeRESPArray(selectors),
	})
}
