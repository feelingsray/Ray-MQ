package utils

import (
	"hash/fnv"
	"net"
)

func MachineIDFromIPHash() (uint16, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return 0, err
	}

	h := fnv.New32a()

	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}

		ip := ipNet.IP.To16()
		if ip == nil {
			continue
		}

		h.Write(ip)
		break
	}

	return uint16(h.Sum32() & 0xFF), nil // 8 bits
}
