// Copyright 2023-2024 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prototransform

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	prototransformv1alpha1 "github.com/bufbuild/prototransform/internal/proto/gen/buf/prototransform/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// ErrLeaseStateNotYetKnown is an error that may be returned by Lease.IsHeld to
// indicate that the leaser has not yet completed querying for the lease's initial
// state.
var ErrLeaseStateNotYetKnown = errors.New("haven't completed initial lease check yet")

//nolint:gochecknoglobals
var (
	startNanos = uint64(time.Now().UnixNano())

	currentProcessInit sync.Once
	currentProcessVal  *prototransformv1alpha1.LeaseHolder
	currentProcessErr  error //nolint:errname
)

// Leaser provides access to long-lived distributed leases, for
// leader election or distributed locking. This can be used by a
// SchemaWatcher so that only a single "leader" process polls the
// remote source for a schema and the others ("followers") just
// get the latest schema from a shared cache.
type Leaser interface {
	// NewLease tries to acquire the given lease name. This
	// returns a lease object, which represents the state of
	// the new lease, and whether the current process holds
	// it or not.
	//
	// Implementations should monitor the lease store so that
	// if the lease is not held but suddenly becomes available
	// (e.g. current leaseholder releases it or crashes),
	// another process can immediately pick it up. The given
	// leaseHolder bytes represent the current process and may
	// be persisted in the lease store if necessary. This is
	// particularly useful if the lease store has no other way
	// to identify connected clients or entry "owners", in which
	// case a lease implementation can compare the persisted
	// lease state to this value to determine if the current
	// client holds the lease.
	//
	// This may start background goroutines. In order to release
	// any such resources associated with the lease, callers must
	// call Lease.Cancel() or cancel the given context.
	NewLease(ctx context.Context, leaseName string, leaseHolder []byte) Lease
}

// Lease represents a long-lived distributed lease. This allows
// the current process to query if the lease is currently held
// or not as well as to configure callbacks for when the lease
// is acquired or released.
type Lease interface {
	// IsHeld returns whether the current process holds the
	// lease. If it returns an error, then it is not known who
	// holds the lease, and the error indicates why not. Polling
	// for a schema will be suspended unless/until this method
	// returns (true, nil).
	IsHeld() (bool, error)
	// SetCallbacks configures the given functions to be called
	// when the lease is acquired or released. The initial state
	// of a lease is "not held". So if the lease is not held at
	// the time this method is invoked, neither callback is
	// invoked. But if the lease IS held at the time this method
	// is invoked, the onAcquire callback will be immediately
	// invoked. A lease must synchronize invocations of the
	// callbacks so that there will never be multiple concurrent
	// calls.
	SetCallbacks(onAcquire, onRelease func())
	// Cancel cancels this lease and frees any associated
	// resources (which may include background goroutines). If
	// the lease is currently held, it will be immediately
	// released, and any onRelease callback will be invoked.
	// IsHeld will return false from that moment. If the
	// same lease needs to be re-acquired later, use the
	// Leaser to create a new lease with the same name.
	Cancel()
}

func getLeaseHolder(userProvidedData []byte) ([]byte, error) {
	var leaseEntry prototransformv1alpha1.LeaseEntry
	if userProvidedData != nil {
		leaseEntry.Holder = &prototransformv1alpha1.LeaseEntry_UserProvided{
			UserProvided: userProvidedData,
		}
	} else {
		leaseHolder, err := currentProcess()
		if err != nil {
			return nil, fmt.Errorf("failed to compute current process bytes for lease: %w", err)
		}
		leaseEntry.Holder = &prototransformv1alpha1.LeaseEntry_Computed{
			Computed: leaseHolder,
		}
	}
	leaseData, err := proto.Marshal(&leaseEntry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal current process info to bytes for lease: %w", err)
	}
	return leaseData, nil
}

func currentProcess() (*prototransformv1alpha1.LeaseHolder, error) {
	currentProcessInit.Do(func() {
		var errs []error
		hostname, hostnameErr := os.Hostname()
		if hostnameErr != nil {
			errs = append(errs, hostnameErr)
		}
		// UDP isn't stateful, so this does not actually connect to anything.
		// But this is a reliable way to see the preferred network interface
		// and IP of the host, by examining the client IP of the socket.
		conn, connErr := net.Dial("udp", "8.8.8.8:53")
		if connErr != nil {
			errs = append(errs, connErr)
		}

		var ipAddress net.IP
		var macAddress net.HardwareAddr
		if connErr == nil { //nolint:nestif
			if udpAddr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
				ipAddress = udpAddr.IP
			}
			if len(ipAddress) == 0 || ipAddress.IsLoopback() {
				ipAddress = nil // don't use loopback addresses!
			} else {
				// look at network interfaces to find the MAC address
				// associated with this IP address
				ifaces, ifaceErr := net.Interfaces()
				if ifaceErr != nil {
					errs = append(errs, ifaceErr)
				}
				var macAddrErr error
				for _, iface := range ifaces {
					if len(iface.HardwareAddr) == 0 {
						// no MAC address on this one
						continue
					}
					addrs, err := iface.Addrs()
					if err != nil {
						// remember one of the address errors to report
						// in case we can't find the IP address on any
						// of the interfaces
						macAddrErr = err
						continue
					}
					for _, addr := range addrs {
						ipNet, ok := addr.(*net.IPNet)
						if !ok {
							continue
						}
						if ipNet.IP.Equal(ipAddress) {
							macAddress = iface.HardwareAddr
							break
						}
					}
					if len(macAddress) > 0 {
						// found it
						break
					}
				}
				if len(macAddress) == 0 && macAddrErr != nil {
					errs = append(errs, macAddrErr)
				}
			}
		}

		// We need at least the host name or the IP address. If we have neither, then
		// we report all errors.
		if hostname == "" && len(ipAddress) == 0 {
			switch len(errs) {
			case 0:
				currentProcessErr = errors.New("internal: could not compute non-empty hostname or IP address for client process ID")
			case 1:
				currentProcessErr = errs[0]
			default:
				currentProcessErr = multiErr(errs)
			}
			return
		}

		currentProcessVal = &prototransformv1alpha1.LeaseHolder{
			Hostname:   hostname,
			IpAddress:  ipAddress,
			MacAddress: macAddress,
			Pid:        uint64(os.Getpid()),
			StartNanos: startNanos,
		}
	})
	return currentProcessVal, currentProcessErr
}

type multiErr []error //nolint:errname

func (m multiErr) Error() string {
	var buf bytes.Buffer
	for i, err := range m {
		if i > 0 {
			buf.WriteRune('\n')
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

func (m multiErr) Unwrap() error {
	return m[0]
}
