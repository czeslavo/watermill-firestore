module github.com/czeslavo/watermill-firestore/cmd/fireinspect

go 1.14

replace github.com/czeslavo/watermill-firestore => ../../

require (
	cloud.google.com/go/firestore v1.3.0
	github.com/ThreeDotsLabs/watermill v1.1.1
	github.com/czeslavo/watermill-firestore v0.0.0-00010101000000-000000000000
	github.com/urfave/cli/v2 v2.2.0
)
