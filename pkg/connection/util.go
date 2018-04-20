package connection

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"k8s.io/api/core/v1"
)

func SanitizeDriverName(driver string) string {
	re := regexp.MustCompile("[^a-zA-Z0-9-]")
	name := re.ReplaceAllString(driver, "-")
	if name[len(name)-1] == '-' {
		// name must not end with '-'
		name = name + "X"
	}
	return name
}

// getFinalizerName returns Snapshotter name suitable to be used as finalizer
func GetFinalizerName(driver string) string {
	return "external-snapshotter/" + SanitizeDriverName(driver)
}
