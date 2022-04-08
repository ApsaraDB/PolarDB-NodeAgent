package collector

import (
	"fmt"
	"testing"
)

func TestGetIO(t *testing.T) {
	c := New()
	err := c.cgroupIo.InitPath("/sys/fs/cgroup/blkio/docker/68372bca8298f530f1d435eb0999590f5b206cb229a1d81901016c4be19b0e6a", true)
	// io
	fmt.Println("###3ioitPath", err)

}
