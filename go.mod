module github.com/bufbuild/prototransform

go 1.25.0

require (
	buf.build/gen/go/bufbuild/reflect/connectrpc/go v1.20.0-20240117202343-bf8f65e8876c.1
	buf.build/gen/go/bufbuild/reflect/protocolbuffers/go v1.36.12-20240117202343-bf8f65e8876c.1
	connectrpc.com/connect v1.20.0
	github.com/bradfitz/gomemcache v0.0.0-20230905024940-24af94b03874
	github.com/gomodule/redigo v1.9.3
	github.com/google/go-cmp v0.7.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.12.1
	google.golang.org/protobuf v1.36.12
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect
