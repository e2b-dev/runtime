//go:build unix

package handler

import (
	"os"
	"os/user"
)

// baseEnv is the environment a child starts from before the sandbox's own
// variables and the request's are layered on: only PATH from envd's process,
// which should ideally be set in the environment, plus the user's identity.
func baseEnv(u *user.User) []string {
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + u.HomeDir,
		"USER=" + u.Username,
		"LOGNAME=" + u.Username,
	}
}
