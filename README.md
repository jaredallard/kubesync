# kubesync

Library for creating mutexes backed by [Kubernetes leases].

## Usage

```bash
go get github.com/jaredallard/kubesync
```

Basic example:

```go
instanceID, err := os.Hostname()
if err != nil {
  // handle err
}

sync := kubesync.New(k, corev1.NamespaceDefault, instanceID)
m := sync.NewMutex("im-a-lock")

if err := m.Lock(ctx); err != nil {
  // handle err
}

// don't forget to unlock
m.Unlock(ctx)
```

See [the examples](./example_test.go) for a more complete example of
usage, or the [pkg.go.dev documentation](https://pkg.go.dev/github.com/jaredallard/kubesync)

## License

LGPL-3.0

[Kubernetes leases]: https://kubernetes.io/docs/concepts/architecture/leases
