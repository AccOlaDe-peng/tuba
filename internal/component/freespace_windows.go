//go:build windows

package component

import "golang.org/x/sys/windows"

func freeSpace(path string) (uint64, error) {
	var freeToCaller, total, totalFree uint64
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	if err := windows.GetDiskFreeSpaceEx(name, &freeToCaller, &total, &totalFree); err != nil {
		return 0, err
	}
	return freeToCaller, nil
}
