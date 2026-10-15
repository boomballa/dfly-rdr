package decoder_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/dongmx/rdb"
	"github.com/stretchr/testify/assert"
	"github.com/xueqiu/rdr/decoder"
)

func createListpack(entries [][]byte) []byte {
	var body bytes.Buffer
	for _, e := range entries {
		// Store as 6-bit string if short, or 12-bit
		slen := len(e)
		var entryLen uint64
		if slen < 64 {
			body.WriteByte(byte(0x80 | slen))
			body.Write(e)
			entryLen = uint64(1 + slen)
		} else {
			body.WriteByte(byte(0xE0 | (slen >> 8)))
			body.WriteByte(byte(slen & 0xFF))
			body.Write(e)
			entryLen = uint64(2 + slen)
		}
		// Write backlen
		if entryLen <= 127 {
			body.WriteByte(byte(entryLen))
		} else if entryLen <= 16383 {
			body.WriteByte(byte(entryLen >> 7))
			body.WriteByte(byte((entryLen & 127) | 128))
		}
	}
	body.WriteByte(0xFF) // EOF

	totalBytes := uint32(4 + 2 + body.Len())
	numElements := uint16(len(entries))

	var lp bytes.Buffer
	binary.Write(&lp, binary.LittleEndian, totalBytes)
	binary.Write(&lp, binary.LittleEndian, numElements)
	lp.Write(body.Bytes())
	return lp.Bytes()
}

func writeRdbString(buf *bytes.Buffer, s []byte) {
	slen := len(s)
	if slen < 64 {
		buf.WriteByte(byte(slen))
	} else {
		buf.WriteByte(0x80)
		binary.Write(buf, binary.BigEndian, uint32(slen))
	}
	buf.Write(s)
}

func TestDecodeListpackTypes(t *testing.T) {
	var rdbData bytes.Buffer
	// Magic header: REDIS0010 (Redis 7.0 / RDB v10)
	rdbData.WriteString("REDIS0010")

	// SelectDB 0
	rdbData.WriteByte(254) // rdbOpCodeSelectDB
	rdbData.WriteByte(0)   // db 0

	// 1. ZSet Listpack (Type 17)
	zsetLP := createListpack([][]byte{
		[]byte("video_item_1"), []byte("10.5"),
		[]byte("video_item_2"), []byte("20.0"),
	})
	rdbData.WriteByte(17) // TypeZSetListpack
	writeRdbString(&rdbData, []byte("my_zset"))
	writeRdbString(&rdbData, zsetLP)

	// 2. Hash Listpack (Type 16)
	hashLP := createListpack([][]byte{
		[]byte("field1"), []byte("value1"),
		[]byte("field2"), []byte("value2"),
	})
	rdbData.WriteByte(16) // TypeHashListpack
	writeRdbString(&rdbData, []byte("my_hash"))
	writeRdbString(&rdbData, hashLP)

	// 3. Set Listpack (Type 20)
	setLP := createListpack([][]byte{
		[]byte("memberA"),
		[]byte("memberB"),
		[]byte("memberC"),
	})
	rdbData.WriteByte(20) // TypeSetListpack
	writeRdbString(&rdbData, []byte("my_set"))
	writeRdbString(&rdbData, setLP)

	// 4. Quicklist2 (Type 18)
	rdbData.WriteByte(18) // TypeListQuicklist2
	writeRdbString(&rdbData, []byte("my_list"))
	rdbData.WriteByte(1) // 1 quicklist node
	rdbData.WriteByte(2) // container = PACKED (2)
	listLP := createListpack([][]byte{
		[]byte("elem1"),
		[]byte("elem2"),
	})
	writeRdbString(&rdbData, listLP)

	// EOF
	rdbData.WriteByte(255) // rdbOpCodeEOF

	dec := decoder.NewDecoder()
	entries := make(map[string]*decoder.Entry)
	done := make(chan struct{})
	go func() {
		for e := range dec.Entries {
			entries[e.Key] = e
		}
		close(done)
	}()

	err := rdb.Decode(&rdbData, dec)
	assert.NoError(t, err)
	<-done

	// Verify ZSet Listpack
	zsetEntry, ok := entries["my_zset"]
	assert.True(t, ok)
	assert.Equal(t, "zset", zsetEntry.Type)
	assert.Equal(t, uint64(2), zsetEntry.NumOfElem)
	assert.True(t, zsetEntry.Bytes > 0)

	// Verify Hash Listpack
	hashEntry, ok := entries["my_hash"]
	assert.True(t, ok)
	assert.Equal(t, "hash", hashEntry.Type)
	assert.Equal(t, uint64(2), hashEntry.NumOfElem)
	assert.True(t, hashEntry.Bytes > 0)

	// Verify Set Listpack
	setEntry, ok := entries["my_set"]
	assert.True(t, ok)
	assert.Equal(t, uint64(3), setEntry.NumOfElem)
	assert.True(t, setEntry.Bytes > 0)

	// Verify List Quicklist2
	listEntry, ok := entries["my_list"]
	assert.True(t, ok)
	assert.Equal(t, "list", listEntry.Type)
	assert.Equal(t, uint64(2), listEntry.NumOfElem)
	assert.True(t, listEntry.Bytes > 0)
}
