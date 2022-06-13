package kopia

import (
	"gopkg.in/check.v1"
)

type KopiaUtilsTestSuite struct{}

var _ = check.Suite(&KopiaUtilsTestSuite{})

func (s *KopiaUtilsTestSuite) TestParseMaintenanceOwnerOutput(c *check.C) {
	for _, tc := range []struct {
		output        string
		expectedOwner string
	}{
		{
			output:        "",
			expectedOwner: "",
		},
		{
			output: `Owner: username@hostname
			Quick Cycle:
			  scheduled: true
			  interval: 1h0m0s
			  next run: now
			Full Cycle:
			  scheduled: false
			Recent Maintenance Runs:
			`,
			expectedOwner: "username@hostname",
		},
	} {
		owner := parseOutput(tc.output)
		c.Assert(owner, check.Equals, tc.expectedOwner)
	}
}
