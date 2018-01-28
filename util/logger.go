package util

import (
	"os"
	// "os/user"

	logger "github.com/Sirupsen/logrus"
)

var Logger *logger.Entry

func init() {
	logger.SetFormatter(&logger.JSONFormatter{})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logger.WarnLevel)

	// `user: Current` not implemented on linux/amd64 for go 1.9 yet.
	// executor, _ := user.Current()
	// username := executor.Username

	Logger = logger.WithFields(logger.Fields{
		"username": "root",
	})
}
