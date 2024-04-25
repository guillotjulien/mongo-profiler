package murmur3

import "encoding/binary"

const (
	c1    int64 = -8663945395140668459 // 0x87c37b91114253d5
	c2    int64 = 5545529020109919103  // 0x4cf5ad432745937f
	fmix1 int64 = -49064778989728563   // 0xff51afd7ed558ccd
	fmix2 int64 = -4265267296055464877 // 0xc4ceb9fe1a85ec53
)

// https://github.com/aappleby/smhasher/blob/61a0530f28277f2e850bfc39600ce61d02b518de/src/MurmurHash3.cpp#L39
func rotl(x int64, r uint8) int64 {
	return (x << r) | int64(uint64(x)>>(64-r))
}

// https://github.com/aappleby/smhasher/blob/61a0530f28277f2e850bfc39600ce61d02b518de/src/MurmurHash3.cpp#L81
func fmix(k int64) int64 {
	k ^= int64(uint64(k) >> 33)
	k *= fmix1
	k ^= int64(uint64(k) >> 33)
	k *= fmix2
	k ^= int64(uint64(k) >> 33)

	return k
}

func block(p byte) int64 {
	return int64(int8(p))
}

// Hash is an implementation of Murmur3 based on Austin Appleby implementation (https://github.com/aappleby/smhasher/blob/61a0530f28277f2e850bfc39600ce61d02b518de/src/MurmurHash3.cpp#L255).
func Hash(data []byte, seed uint64) (int64, int64) {
	length := len(data)

	h1 := int64(seed)
	h2 := int64(seed)

	// body
	nBlocks := length / 16
	for i := 0; i < nBlocks; i++ {
		// Thanks - https://github.com/scylladb/scylla-go-driver/blob/ce81923df69aad9b5f06d62f5bc9f50ad990bd7e/transport/murmur/murmur_appengine.go#L8
		k1 := int64(binary.LittleEndian.Uint64(data[i*16:]))
		k2 := int64(binary.LittleEndian.Uint64(data[(i*16)+8:]))

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1

		h1 = rotl(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		h2 = rotl(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	// tail
	tail := data[nBlocks*16:]

	var k1, k2 int64

	switch length & 15 {
	case 15:
		k2 ^= block(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= block(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= block(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= block(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= block(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= block(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= block(tail[8])

		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2

		fallthrough
	case 8:
		k1 ^= block(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= block(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= block(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= block(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= block(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= block(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= block(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= block(tail[0])

		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	// finalization

	h1 ^= int64(length)
	h2 ^= int64(length)

	h1 += h2
	h2 += h1

	h1 = fmix(h1)
	h2 = fmix(h2)

	h1 += h2
	h2 += h1

	return h1, h2
}
