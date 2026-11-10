//go:build !linux

package api

import "errors"

var errNFSUnsupported = errors.New("NFS volumes are only supported on Linux")

func (execNFSMounter) Unmount(string) error { return errNFSUnsupported }

func (execNFSMounter) MountID(string) (uint64, error) { return 0, errNFSUnsupported }
