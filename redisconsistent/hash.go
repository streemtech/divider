package redis_consistent

import (
	"crypto/md5"
	"encoding/binary"
)

//StringToIntHash takes in a string and converts it to the md5-hashed version of it.
//It then converts that hash to a uin64,
//This value is then distributed between -2^53 and 2^53 (redis zset min/max)
func StringToIntHash(in string) int64 {
	s := md5.Sum([]byte(in))
	u := binary.BigEndian.Uint64(s[:])
	return shrink(u, 53)
}

//in: the input number that I want to cut down to space.
//bits: the bit count of the minimum and maximum that I want
// ie -2^53 - 2^53 would put in 53
func shrink(in uint64, bits int) int64 {
	return int64((in & ((2 << uint64(bits)) - 1))) - (2 << uint64(bits-1))
}
