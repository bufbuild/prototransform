![The Buf logo](./.github/buf-logo.svg)

# Prototransform

[![Build](https://github.com/bufbuild/prototransform/actions/workflows/ci.yaml/badge.svg?branch=main)][badges_ci]
[![Report Card](https://goreportcard.com/badge/github.com/bufbuild/prototransform)][badges_goreportcard]
[![GoDoc](https://pkg.go.dev/badge/github.com/bufbuild/prototransform.svg)][badges_godoc]

### Convert protobuf message data to alternate formats

Use the `prototransform` library to simplify your data transformation &
collection. Our simple package allows the caller to convert a given message data
blob from one format to another by referring to a type schema on the Buf Schema
Registry.

* No need to bake in proto files
* Supports Binary, JSON and Text formats
* Extensible for other/custom formats

## Getting started

`prototransform` is designed to be flexible enough to fit quickly into your
development environment.

Here's an example of how you could use `prototransform` to transform messages
received from a PubSub topic...

### Transform Messages from a Topic

Whilst `prototransform` has various applications, converting messages off some
kind of message queue is a primary use-case. This can take many forms, for the
purposes of simplicity we will look at this abstractly in a pub/sub model where
we want to:

1. Open a subscription to a topic with the Pub/Sub service of your choice
2. Start a `SchemaWatcher` to fetch a module from the Buf Schema Registry
3. Receive, Transform and Acknowledge messages from the topic

#### Opening a Subscription & Schema Watcher

```go
import (
	"context"
	"fmt"

	"github.com/bufbuild/prototransform"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/<driver>"
)
...
	subs, err := pubsub.OpenSubscription(ctx, "<driver-url>")
	if err != nil {
		return fmt.Errorf("could not open topic subscription: %v", err)
	}
	defer subs.Shutdown(ctx)
	// Supply auth credentials to the BSR
	fileDescriptorSetServiceClient := prototransform.NewDefaultFileDescriptorSetServiceClient("<bsr-token>")
	// Configure the module for schema watcher
	cfg := &prototransform.Config{
		Client:  fileDescriptorSetServiceClient,
		Module:  "buf.build/someuser/somerepo", // BSR module
		Version: "some-tag", // tag or draft name or leave blank for "latest"
	}
	watcher, err := prototransform.NewSchemaWatcher(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create schema watcher: %v", err)
	}
	defer watcher.Stop()
	// before we start processing messages, make sure the schema has been
	// downloaded
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := watcher.AwaitReady(ctx); err != nil {
		return fmt.Errorf("schema watcher never became ready: %v", err)
	}
...
```

A `SchemaWatcher` is the entrypoint of `prototransform`. This is created first
so your code can connect to the Buf Schema Registry and fetch a schema to be used
to transform and/or filter payloads.

#### Prepare a converter

A `Converter` implements the functionality to convert payloads to different formats
and optionally filter/mutate messages during this transformation. In the following
example, we have initialized a `*prototransform.Converter` which expects a binary
input and will return JSON.

```go
...
    converter := &prototransform.Converter{
        Resolver:       schemaWatcher,
        InputFormat:    prototransform.BinaryInputFormat(proto.UnmarshalOptions{}),
        OutputFormat:   prototransform.JSONOutputFormat(protojson.MarshalOptions{}),
    }
...
```

Out of the box, you can supply `proto`, `protojson` and `prototext` here but
feel free to supply your own custom formats as-well.

| FORMAT                                                                                  | InputFormat                          | OutputFormat                          |
|-----------------------------------------------------------------------------------------|--------------------------------------|---------------------------------------|
| [JSON](https://pkg.go.dev/google.golang.org/protobuf/encoding/protojson#MarshalOptions) | `prototransform.JSONInputFormat()`   | `prototransform.JSONOutputFormat()`   |
| [TEXT](https://pkg.go.dev/google.golang.org/protobuf/encoding/prototext#MarshalOptions) | `prototransform.TEXTInputFormat()`   | `prototransform.TEXTOutputFormat()`   |
| [Binary](https://pkg.go.dev/google.golang.org/protobuf/proto#MarshalOptions)            | `prototransform.BinaryInputFormat()` | `prototransform.BinaryOutputFormat()` |

#### Receiving and Transforming Messages

Now that we have an active subscription, schema watcher, and converter, we can
start processing messages. A simple subscriber that transforms received messages
looks like this:

```go
...
    // Loop on received messages.
    for {
        msg, err := subscription.Receive(ctx)
        if err != nil {
            log.Printf("Receiving message: %v", err)
            break
        }
        // Do transformation based on the message name
        convertedMessage, err := converter.ConvertMessage("<message-name>", msg.Body)
        if err != nil {
            log.Printf("Converting message: %v", err)
            break
        }
        fmt.Printf("Converted message: %q\n", convertedMessage)

        msg.Ack()
    }
...
```

For illustrative purposes, let's assume that the topic we have subscribed to is
`buf.connect.demo.eliza.v1`, we have the module stored on the BSR [here](https://buf.build/bufbuild/eliza).
We would configure the message name as `buf.connect.demo.eliza.v1.ConverseRequest`.

## Options

### Cache

A `SchemaWatcher` can be configured with a user-supplied cache
implementation, to act as a fallback when fetching schemas. The interface is of
the form:

```go
type Cache interface {
    Load(key string) ([]byte, error)
    Save(key string, data []byte) error
}
```

### Filters

A use-case exists where the values within the output message should differ from
the input given some set of defined rules. For example, Personally Identifiable
Information(PII) may want to be removed from a message before it is piped into a
sink. For this reason, we have supplied Filters.

Here's an example where we have defined a custom annotation to mark fields
as `sensitive`:

```protobuf
syntax = "proto3";
package foo.v1;
// ...
extend google.protobuf.FieldOptions {
  bool sensitive = 30000;
}
// ...
message FooMessage {
  string name = 1 [(sensitive) = true];
}
```

We then use `prototransform.Redact()` to create a filter and
supply it to our converter via its `Filters` field:

```go
...
isSensitive := func (in protoreflect.FieldDescriptor) bool {
    return proto.GetExtension(in.Options(), foov1.E_Sensitive).(bool)
}
filter := prototransform.Redact(isSensitive)
converter.Filters = prototransform.Filters{filter}
...
```

Now, any attribute marked as "sensitive" will be omitted from the output
produced by the converter.

## Community

For help and discussion around Protobuf, best practices, and more, join us
on [Slack][badges_slack].

## Status

This project is currently in **alpha**. The API should be considered unstable and likely to change.

## Legal

Offered under the [Apache 2 license][license].

[badges_ci]: https://github.com/bufbuild/prototransform/actions/workflows/ci.yaml
[badges_goreportcard]: https://goreportcard.com/report/github.com/bufbuild/prototransform
[badges_godoc]: https://pkg.go.dev/github.com/bufbuild/prototransform
[badges_slack]: https://join.slack.com/t/bufbuild/shared_invite/zt-f5k547ki-dW9LjSwEnl6qTzbyZtPojw
[license]: https://github.com/bufbuild/prototransform/blob/main/LICENSE.txt
