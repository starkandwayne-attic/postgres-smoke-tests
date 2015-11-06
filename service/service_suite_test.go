package service_test

import (
	"testing"

	. "postgres-smoke-tests/Godeps/_workspace/src/github.com/onsi/ginkgo"
	. "postgres-smoke-tests/Godeps/_workspace/src/github.com/onsi/gomega"
)

func TestService(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RDPG Smoke Tests")
}
