// Package mount abstracts OS-level mount and unmount operations needed by
// node-agent.
package mount

import "context"

// Mounter handles filesystem mount and unmount operations.
type Mounter interface {
	// Mount mounts device at target read-only.  target is created if it does
	// not exist.  The call is idempotent — mounting an already-mounted target
	// is not an error.
	Mount(ctx context.Context, device, target string) error

	// Unmount unmounts the filesystem at target and removes the mount-point
	// directory.  The call is idempotent — unmounting a path that is not
	// mounted is not an error.
	Unmount(ctx context.Context, target string) error
}
