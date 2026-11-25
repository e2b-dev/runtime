package main

import "github.com/e2b-dev/infra/packages/api/pkg/apiserver"

var (
	commitSHA                  string
	expectedMigrationTimestamp string
	serviceVersion             = "0.30.0" // x-release-please-version
)

func main() {
	apiserver.Run(apiserver.Options{
		Version:                    serviceVersion,
		CommitSHA:                  commitSHA,
		ExpectedMigrationTimestamp: expectedMigrationTimestamp,
	})
}
