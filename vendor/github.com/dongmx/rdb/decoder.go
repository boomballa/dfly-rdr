// Package rdb implements parsing and encoding of the Redis RDB file format.
package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/dongmx/rdb/crc64"
	"github.com/pkg/errors"
)

// A Decoder must be implemented to parse a RDB file.
type Decoder interface {
	// StartRDB is called when parsing of a valid RDB file starts.
	StartRDB()
	// StartDatabase is called when database n starts.
	// Once a database starts, another database will not start until EndDatabase is called.
	StartDatabase(n int)
	// AUX field
	Aux(key, value []byte)
	// ResizeDB hint
	ResizeDatabase(dbSize, expiresSize uint32)
	// Set is called once for each string key.
	Set(key, value []byte, expiry int64)
	// StartHash is called at the beginning of a hash.
	// Hset will be called exactly length times before EndHash.
	StartHash(key []byte, length, expiry int64)
	// Hset is called once for each field=value pair in a hash.
	Hset(key, field, value []byte)
	// EndHash is called when there are no more fields in a hash.
	EndHash(key []byte)
	// StartSet is called at the beginning of a set.
	// Sadd will be called exactly cardinality times before EndSet.
	StartSet(key []byte, cardinality, expiry int64)
	// Sadd is called once for each member of a set.
	Sadd(key, member []byte)
	// EndSet is called when there are no more fields in a set.
	EndSet(key []byte)
	// StartStream is called at the beginning of a stream.
	// Xadd will be called exactly length times before EndStream.
	StartStream(key []byte, cardinality, expiry int64)
	// Xadd is called once for each id in a stream.
	Xadd(key, id, listpack []byte)
	// EndHash is called when there are no more fields in a hash.
	EndStream(key []byte)
	// StartList is called at the beginning of a list.
	// Rpush will be called exactly length times before EndList.
	// If length of the list is not known, then length is -1
	StartList(key []byte, length, expiry int64)
	// Rpush is called once for each value in a list.
	Rpush(key, value []byte)
	// EndList is called when there are no more values in a list.
	EndList(key []byte)
	// StartZSet is called at the beginning of a sorted set.
	// Zadd will be called exactly cardinality times before EndZSet.
	StartZSet(key []byte, cardinality, expiry int64)
	// Zadd is called once for each member of a sorted set.
	Zadd(key []byte, score float64, member []byte)
	// EndZSet is called when there are no more members in a sorted set.
	EndZSet(key []byte)
	// EndDatabase is called at the end of a database.
	EndDatabase(n int)
	// EndRDB is called when parsing of the RDB file is complete.
	EndRDB()
}

// Decode parses a RDB file from r and calls the decode hooks on d.
func Decode(r io.Reader, d Decoder) error {
	decoder := &decode{d, make([]byte, 8), bufio.NewReader(r)}
	return decoder.decode()
}

// DecodeDump a byte slice from the Redis DUMP command. The dump does not contain the
// database, key or expiry, so they must be included in the function call (but
// can be zero values).
func DecodeDump(dump []byte, db int, key []byte, expiry int64, d Decoder) error {
	err := verifyDump(dump)
	if err != nil {
		return err
	}

	decoder := &decode{d, make([]byte, 8), bytes.NewReader(dump[1:])}
	decoder.event.StartRDB()
	decoder.event.StartDatabase(db)

	err = decoder.readObject(key, ValueType(dump[0]), expiry)

	decoder.event.EndDatabase(db)
	decoder.event.EndRDB()
	return err
}

type byteReader interface {
	io.Reader
	io.ByteReader
}

type decode struct {
	event  Decoder
	intBuf []byte
	r      byteReader
}

// ValueType of redis type
type ValueType byte

// type value
const (
	TypeString  ValueType = 0
	TypeList    ValueType = 1
	TypeSet     ValueType = 2
	TypeZSet    ValueType = 3
	TypeHash    ValueType = 4
	TypeZSet2   ValueType = 5
	TypeModule  ValueType = 6
	TypeModule2 ValueType = 7

	TypeHashZipmap      ValueType = 9
	TypeListZiplist     ValueType = 10
	TypeSetIntset       ValueType = 11
	TypeZSetZiplist     ValueType = 12
	TypeHashZiplist     ValueType = 13
	TypeListQuicklist    ValueType = 14
	TypeStreamListPacks  ValueType = 15
	TypeHashListpack     ValueType = 16
	TypeZSetListpack     ValueType = 17
	TypeListQuicklist2   ValueType = 18
	TypeStreamListPacks2 ValueType = 19
	TypeSetListpack      ValueType = 20
	TypeStreamListPacks3 ValueType = 21
)

const (
	quicklistNodeContainerPlain  = 1
	quicklistNodeContainerPacked = 2
)

const (
	rdbVersion  = 11
	rdb6bitLen  = 0
	rdb14bitLen = 1
	rdb32bitLen = 0x80
	rdb64bitLen = 0x81
	rdbEncVal   = 3
	rdbLenErr   = math.MaxUint64

	rdbOpCodeModuleAux = 247
	rdbOpCodeIdle      = 248
	rdbOpCodeFreq      = 249
	rdbOpCodeAux       = 250
	rdbOpCodeResizeDB  = 251
	rdbOpCodeExpiryMS  = 252
	rdbOpCodeExpiry    = 253
	rdbOpCodeSelectDB  = 254
	rdbOpCodeEOF       = 255

	rdbModuleOpCodeEOF    = 0
	rdbModuleOpCodeSint   = 1
	rdbModuleOpCodeUint   = 2
	rdbModuleOpCodeFloat  = 3
	rdbModuleOpCodeDouble = 4
	rdbModuleOpCodeString = 5

	rdbLoadNone  = 0
	rdbLoadEnc   = (1 << 0)
	rdbLoadPlain = (1 << 1)
	rdbLoadSds   = (1 << 2)

	rdbSaveNode        = 0
	rdbSaveAofPreamble = (1 << 0)

	rdbEncInt8  = 0
	rdbEncInt16 = 1
	rdbEncInt32 = 2
	rdbEncLZF   = 3

	rdbZiplist6bitlenString  = 0
	rdbZiplist14bitlenString = 1
	rdbZiplist32bitlenString = 2

	rdbZiplistInt16 = 0xc0
	rdbZiplistInt32 = 0xd0
	rdbZiplistInt64 = 0xe0
	rdbZiplistInt24 = 0xf0
	rdbZiplistInt8  = 0xfe
	rdbZiplistInt4  = 15

	rdbLpHdrSize           = 6
	rdbLpHdrNumeleUnknown  = math.MaxUint16
	rdbLpMaxIntEncodingLen = 0
	rdbLpMaxBacklenSize    = 5
	rdbLpMaxEntryBacklen   = 34359738367
	rdbLpEncodingInt       = 0
	rdbLpEncodingString    = 1

	rdbLpEncoding7BitUint     = 0
	rdbLpEncoding7BitUintMask = 0x80

	rdbLpEncoding6BitStr     = 0x80
	rdbLpEncoding6BitStrMask = 0xC0

	rdbLpEncoding13BitInt     = 0xC0
	rdbLpEncoding13BitIntMask = 0xE0

	rdbLpEncoding12BitStr     = 0xE0
	rdbLpEncoding12BitStrMask = 0xF0

	rdbLpEncoding16BitInt     = 0xF1
	rdbLpEncoding16BitIntMask = 0xFF

	rdbLpEncoding24BitInt     = 0xF2
	rdbLpEncoding24BitIntMask = 0xFF

	rdbLpEncoding32BitInt     = 0xF3
	rdbLpEncoding32BitIntMask = 0xFF

	rdbLpEncoding64BitInt     = 0xF4
	rdbLpEncoding64BitIntMask = 0xFF

	rdbLpEncoding32BitStr     = 0xF0
	rdbLpEncoding32BitStrMask = 0xFF

	rdbLpEOF = 0xFF
)

func (d *decode) decode() error {
	err := d.checkHeader()
	if err != nil {
		return err
	}
	d.event.StartRDB()
	var db uint64
	var expiry int64
	//var lruClock int64
	var lruIdle uint64
	var lfuFreq int
	firstDB := true
	for {
		objType, err := d.r.ReadByte()
		if err != nil {
			return errors.Wrap(err, "readfailed")
		}
		switch objType {
		case rdbOpCodeFreq:
			b, err := d.r.ReadByte()
			lfuFreq = int(b)
			if err != nil {
				return err
			}
		case rdbOpCodeIdle:
			idle, _, err := d.readLength()
			if err != nil {
				return err
			}
			lruIdle = uint64(idle)
		case rdbOpCodeAux:
			auxKey, err := d.readString()
			if err != nil {
				return err
			}
			auxVal, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Aux(auxKey, auxVal)
		case rdbOpCodeResizeDB:
			dbSize, _, err := d.readLength()
			if err != nil {
				return err
			}
			expiresSize, _, err := d.readLength()
			if err != nil {
				return err
			}
			d.event.ResizeDatabase(uint32(dbSize), uint32(expiresSize))
		case rdbOpCodeExpiryMS:
			_, err := io.ReadFull(d.r, d.intBuf)
			if err != nil {
				return err
			}
			expiry = int64(binary.LittleEndian.Uint64(d.intBuf))
		case rdbOpCodeExpiry:
			_, err := io.ReadFull(d.r, d.intBuf[:4])
			if err != nil {
				return err
			}
			expiry = int64(binary.LittleEndian.Uint32(d.intBuf)) * 1000
		case rdbOpCodeSelectDB:
			if !firstDB {
				d.event.EndDatabase(int(db))
			}
			db, _, err = d.readLength()
			if err != nil {
				return err
			}
			d.event.StartDatabase(int(db))
		case rdbOpCodeEOF:
			d.event.EndDatabase(int(db))
			d.event.EndRDB()
			return nil
		case rdbOpCodeModuleAux:

		default:
			key, err := d.readString()
			if err != nil {
				return err
			}
			err = d.readObject(key, ValueType(objType), expiry)
			if err != nil {
				return err
			}
			_, _ = lfuFreq, lruIdle
			expiry = 0
			lfuFreq = 0
			lruIdle = 0
		}
	}

}

func (d *decode) readObject(key []byte, typ ValueType, expiry int64) error {
	switch typ {
	case TypeString:
		value, err := d.readString()
		if err != nil {
			return err
		}
		d.event.Set(key, value, expiry)
	case TypeList:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartList(key, int64(length), expiry)
		for length > 0 {
			length--
			value, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Rpush(key, value)
		}
		d.event.EndList(key)
	case TypeListQuicklist:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartList(key, int64(-1), expiry)
		for length > 0 {
			length--
			d.readZiplist(key, 0, false)
		}
		d.event.EndList(key)
	case TypeSet:
		cardinality, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartSet(key, int64(cardinality), expiry)
		for cardinality > 0 {
			cardinality--
			member, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Sadd(key, member)
		}
		d.event.EndSet(key)
	case TypeZSet2:
		fallthrough
	case TypeZSet:
		cardinality, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartZSet(key, int64(cardinality), expiry)
		for cardinality > 0 {
			cardinality--
			member, err := d.readString()
			if err != nil {
				return err
			}
			var score float64
			if typ == TypeZSet2 {
				score, err = d.readBinaryFloat64()
				if err != nil {
					return err
				}
			} else {
				score, err = d.readFloat64()
				if err != nil {
					return err
				}
			}
			d.event.Zadd(key, score, member)
		}
		d.event.EndZSet(key)
	case TypeHash:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.StartHash(key, int64(length), expiry)
		for length > 0 {
			length--
			field, err := d.readString()
			if err != nil {
				return err
			}
			value, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Hset(key, field, value)
		}
		d.event.EndHash(key)
	case TypeHashZipmap:
		return d.readZipmap(key, expiry)
	case TypeListZiplist:
		return d.readZiplist(key, expiry, true)
	case TypeSetIntset:
		return d.readIntset(key, expiry)
	case TypeZSetZiplist:
		return d.readZiplistZset(key, expiry)
	case TypeHashZiplist:
		return d.readZiplistHash(key, expiry)
	case TypeHashListpack:
		return d.readListpackHash(key, expiry)
	case TypeZSetListpack:
		return d.readListpackZset(key, expiry)
	case TypeListQuicklist2:
		return d.readQuicklist2(key, expiry)
	case TypeSetListpack:
		return d.readListpackSet(key, expiry)
	case TypeStreamListPacks, TypeStreamListPacks2, TypeStreamListPacks3:
		return d.readStream(key, expiry, typ)
	case TypeModule:
		fallthrough
	case TypeModule2:
		return d.readModule(key, expiry)
	default:
		return fmt.Errorf("rdb: unknown object type %d for key %s", typ, key)
	}
	return nil
}

func (d *decode) readModule(key []byte, expiry int64) error {
	moduleid, _, err := d.readLength()
	if err != nil {
		return err
	}
	return fmt.Errorf("Not supported load module %v", moduleid)
}

func (d *decode) readStream(key []byte, expiry int64, typ ValueType) error {
	cardinality, _, err := d.readLength()
	if err != nil {
		return err
	}
	d.event.StartStream(key, int64(cardinality), expiry)
	for cardinality > 0 {
		cardinality--

		streamID, err := d.readString()
		if err != nil {
			return err
		}
		listpack, err := d.readString()
		if err != nil {
			return err
		}
		d.event.Xadd(key, streamID, listpack)
	}
	var length, lastIDms, lastIDseq uint64
	length, _, err = d.readLength()
	if err != nil {
		return err
	}
	lastIDms, _, err = d.readLength()
	if err != nil {
		return err
	}
	lastIDseq, _, err = d.readLength()
	if err != nil {
		return err
	}
	_, _, _ = length, lastIDms, lastIDseq

	if typ == TypeStreamListPacks2 || typ == TypeStreamListPacks3 {
		firstIDms, _, err := d.readLength()
		if err != nil {
			return err
		}
		firstIDseq, _, err := d.readLength()
		if err != nil {
			return err
		}
		maxDeletedIDms, _, err := d.readLength()
		if err != nil {
			return err
		}
		maxDeletedIDseq, _, err := d.readLength()
		if err != nil {
			return err
		}
		entriesAdded, _, err := d.readLength()
		if err != nil {
			return err
		}
		_, _, _, _, _ = firstIDms, firstIDseq, maxDeletedIDms, maxDeletedIDseq, entriesAdded
	}

	//TODO output consumer groups
	var groupsCount uint64
	groupsCount, _, err = d.readLength()
	if err != nil {
		return err
	}
	//fmt.Println(groupsCount)
	for groupsCount > 0 {
		groupsCount--
		name, err := d.readString()
		if err != nil {
			return err
		}
		gIDms, _, _ := d.readLength()
		gIDseq, _, _ := d.readLength()
		_, _, _ = name, gIDms, gIDseq

		if typ == TypeStreamListPacks2 || typ == TypeStreamListPacks3 {
			entriesRead, _, err := d.readLength()
			if err != nil {
				return err
			}
			_ = entriesRead
		}

		pelSize, _, _ := d.readLength()
		for pelSize > 0 {
			pelSize--
			d.readUint64()
			rawid := make([]byte, 16)
			io.ReadFull(d.r, rawid)
			d.readUint64()
			d.readLength()
		}
		consumersNum, _, _ := d.readLength()
		for consumersNum > 0 {
			consumersNum--
			d.readString()
			d.readUint64()
			if typ == TypeStreamListPacks2 || typ == TypeStreamListPacks3 {
				activeTime, err := d.readUint64()
				if err != nil {
					return err
				}
				_ = activeTime
			}
			pelSize, _, _ := d.readLength()
			for pelSize > 0 {
				pelSize--
				d.readUint64()
				rawid := make([]byte, 16)
				io.ReadFull(d.r, rawid)
			}
		}
	}

	d.event.EndStream(key)

	return nil
}

func (d *decode) readZipmap(key []byte, expiry int64) error {
	var length int
	zipmap, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(zipmap)
	lenByte, err := buf.ReadByte()
	if err != nil {
		return err
	}
	if lenByte >= 254 { // we need to count the items manually
		length, err = countZipmapItems(buf)
		length /= 2
		if err != nil {
			return err
		}
	} else {
		length = int(lenByte)
	}
	d.event.StartHash(key, int64(length), expiry)
	for i := 0; i < length; i++ {
		field, err := readZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := readZipmapItem(buf, true)
		if err != nil {
			return err
		}
		d.event.Hset(key, field, value)
	}
	d.event.EndHash(key)
	return nil
}

func readZipmapItem(buf *sliceBuffer, readFree bool) ([]byte, error) {
	length, free, err := readZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func countZipmapItems(buf *sliceBuffer) (int, error) {
	n := 0
	for {
		strLen, free, err := readZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}

func readZipmapItemLength(buf *sliceBuffer, readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, fmt.Errorf("rdb: invalid zipmap item length")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}
	return int(b), int(free), err
}

func lpBacklenSize(l uint64) int {
	if l <= 127 {
		return 1
	} else if l <= 16383 {
		return 2
	} else if l <= 2097151 {
		return 3
	} else if l <= 268435455 {
		return 4
	}
	return 5
}

func readListpackEntry(buf *sliceBuffer) ([]byte, error) {
	b0, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if b0 == byte(rdbLpEOF) {
		return nil, io.EOF
	}

	var entry []byte
	var entryLen uint64

	switch {
	case lpEncodingIs7BitUint(b0):
		val := int64(b0 & 0x7F)
		entry = []byte(strconv.FormatInt(val, 10))
		entryLen = 1
	case lpEncodingIs6BitStr(b0):
		slen := int(lpEncoding6BitStrLen([]byte{b0}))
		s, err := buf.Slice(slen)
		if err != nil {
			return nil, err
		}
		entry = s
		entryLen = uint64(1 + slen)
	case lpEncodingIs13BitInt(b0):
		b1, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		uval := (uint64(b0&0x1F) << 8) | uint64(b1)
		negstart := uint64(1) << 12
		negmax := uint64(8191)
		var val int64
		if uval >= negstart {
			val = -int64(negmax-uval) - 1
		} else {
			val = int64(uval)
		}
		entry = []byte(strconv.FormatInt(val, 10))
		entryLen = 2
	case lpEncodingIs16BitInt(b0):
		bytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		val := int64(int16(binary.LittleEndian.Uint16(bytes)))
		entry = []byte(strconv.FormatInt(val, 10))
		entryLen = 3
	case lpEncodingIs24BitInt(b0):
		bytes, err := buf.Slice(3)
		if err != nil {
			return nil, err
		}
		uval := uint32(bytes[0]) | (uint32(bytes[1]) << 8) | (uint32(bytes[2]) << 16)
		if uval&0x800000 != 0 {
			uval |= 0xFF000000
		}
		val := int64(int32(uval))
		entry = []byte(strconv.FormatInt(val, 10))
		entryLen = 4
	case lpEncodingIs32BitInt(b0):
		bytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		val := int64(int32(binary.LittleEndian.Uint32(bytes)))
		entry = []byte(strconv.FormatInt(val, 10))
		entryLen = 5
	case lpEncodingIs64BitInt(b0):
		bytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		val := int64(binary.LittleEndian.Uint64(bytes))
		entry = []byte(strconv.FormatInt(val, 10))
		entryLen = 9
	case lpEncodingIs12BitStr(b0):
		b1, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		slen := int((uint32(b0&0x0F) << 8) | uint32(b1))
		s, err := buf.Slice(slen)
		if err != nil {
			return nil, err
		}
		entry = s
		entryLen = uint64(2 + slen)
	case lpEncodingIs32BitStr(b0):
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		slen := int(binary.LittleEndian.Uint32(lenBytes))
		s, err := buf.Slice(slen)
		if err != nil {
			return nil, err
		}
		entry = s
		entryLen = uint64(5 + slen)
	default:
		return nil, fmt.Errorf("unknown listpack entry encoding: 0x%02x", b0)
	}

	// Skip backlen
	backlen := lpBacklenSize(entryLen)
	if _, err := buf.Slice(backlen); err != nil {
		return nil, err
	}

	return entry, nil
}

func readAllListpackEntries(buf *sliceBuffer) ([][]byte, error) {
	var entries [][]byte
	for {
		entry, err := readListpackEntry(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (d *decode) readListpackHash(key []byte, expiry int64) error {
	listpack, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(listpack)
	buf.Seek(6, 0)
	entries, err := readAllListpackEntries(buf)
	if err != nil {
		return err
	}
	length := int64(len(entries) / 2)
	d.event.StartHash(key, length, expiry)
	for i := 0; i+1 < len(entries); i += 2 {
		field := entries[i]
		val := entries[i+1]
		d.event.Hset(key, field, val)
	}
	d.event.EndHash(key)
	return nil
}

func (d *decode) readListpackZset(key []byte, expiry int64) error {
	listpack, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(listpack)
	buf.Seek(6, 0)
	entries, err := readAllListpackEntries(buf)
	if err != nil {
		return err
	}
	cardinality := int64(len(entries) / 2)
	d.event.StartZSet(key, cardinality, expiry)
	for i := 0; i+1 < len(entries); i += 2 {
		member := entries[i]
		scoreBytes := entries[i+1]
		score, _ := strconv.ParseFloat(string(scoreBytes), 64)
		d.event.Zadd(key, score, member)
	}
	d.event.EndZSet(key)
	return nil
}

func (d *decode) readListpackSet(key []byte, expiry int64) error {
	listpack, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(listpack)
	buf.Seek(6, 0)
	entries, err := readAllListpackEntries(buf)
	if err != nil {
		return err
	}
	cardinality := int64(len(entries))
	d.event.StartSet(key, cardinality, expiry)
	for _, member := range entries {
		d.event.Sadd(key, member)
	}
	d.event.EndSet(key)
	return nil
}

func (d *decode) readQuicklist2(key []byte, expiry int64) error {
	length, _, err := d.readLength()
	if err != nil {
		return err
	}
	d.event.StartList(key, -1, expiry)
	for length > 0 {
		length--
		container, _, err := d.readLength()
		if err != nil {
			return err
		}
		if container == quicklistNodeContainerPlain {
			entry, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Rpush(key, entry)
		} else if container == quicklistNodeContainerPacked {
			listpack, err := d.readString()
			if err != nil {
				return err
			}
			buf := newSliceBuffer(listpack)
			buf.Seek(6, 0)
			entries, err := readAllListpackEntries(buf)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				d.event.Rpush(key, entry)
			}
		} else {
			return fmt.Errorf("rdb: unknown quicklist2 container %d for key %s", container, key)
		}
	}
	d.event.EndList(key)
	return nil
}

func lpEncodingIs7BitUint(b byte) bool {
	return (((b) & rdbLpEncoding7BitUintMask) == rdbLpEncoding7BitUint)
}
func lpEncodingIs6BitStr(b byte) bool {
	return (((b) & rdbLpEncoding6BitStrMask) == rdbLpEncoding6BitStr)
}
func lpEncodingIs13BitInt(b byte) bool {
	return (((b) & rdbLpEncoding13BitIntMask) == rdbLpEncoding13BitInt)
}
func lpEncodingIs12BitStr(b byte) bool {
	return (((b) & rdbLpEncoding12BitStrMask) == rdbLpEncoding12BitStr)
}
func lpEncodingIs16BitInt(b byte) bool {
	return (((b) & rdbLpEncoding16BitIntMask) == rdbLpEncoding16BitInt)
}
func lpEncodingIs24BitInt(b byte) bool {
	return (((b) & rdbLpEncoding24BitIntMask) == rdbLpEncoding24BitInt)
}
func lpEncodingIs32BitInt(b byte) bool {
	return (((b) & rdbLpEncoding32BitIntMask) == rdbLpEncoding32BitInt)
}
func lpEncodingIs64BitInt(b byte) bool {
	return (((b) & rdbLpEncoding64BitIntMask) == rdbLpEncoding64BitInt)
}
func lpEncodingIs32BitStr(b byte) bool {
	return (((b) & rdbLpEncoding32BitStrMask) == rdbLpEncoding32BitStr)
}
func lpEncoding6BitStrLen(b []byte) uint32 {
	return uint32(b[0] & 0x3F)
}
func lpEncoding12BitStrLen(b []byte) uint32 {
	return (uint32((b)[0]&0xF) << 8) | uint32((b)[1])
}
func lpEncoding32BitStrLen(b []byte) uint32 {
	return (uint32(b[1]) << 0) |
		(uint32(b[2]) << 8) |
		(uint32(b[3]) << 16) |
		(uint32(b[4]) << 24)
}

func (d *decode) readZiplist(key []byte, expiry int64, addListEvents bool) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	if addListEvents {
		d.event.StartList(key, length, expiry)
	}
	for i := int64(0); i < length; i++ {
		entry, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		d.event.Rpush(key, entry)
	}
	if addListEvents {
		d.event.EndList(key)
	}
	return nil
}

func (d *decode) readZiplistZset(key []byte, expiry int64) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	cardinality, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	cardinality /= 2
	d.event.StartZSet(key, cardinality, expiry)
	for i := int64(0); i < cardinality; i++ {
		member, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		scoreBytes, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			return err
		}
		d.event.Zadd(key, score, member)
	}
	d.event.EndZSet(key)
	return nil
}

func (d *decode) readZiplistHash(key []byte, expiry int64) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2
	d.event.StartHash(key, length, expiry)
	for i := int64(0); i < length; i++ {
		field, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		d.event.Hset(key, field, value)
	}
	d.event.EndHash(key)
	return nil
}

func readZiplistLength(buf *sliceBuffer) (int64, error) {
	buf.Seek(8, 0) // skip the zlbytes and zltail
	lenBytes, err := buf.Slice(2)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint16(lenBytes)), nil
}

func readZiplistEntry(buf *sliceBuffer) ([]byte, error) {
	prevLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if prevLen == 254 {
		buf.Seek(4, 1) // skip the 4-byte prevlen
	}

	header, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case header>>6 == rdbZiplist6bitlenString:
		return buf.Slice(int(header & 0x3f))
	case header>>6 == rdbZiplist14bitlenString:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		return buf.Slice((int(header&0x3f) << 8) | int(b))
	case header>>6 == rdbZiplist32bitlenString:
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return buf.Slice(int(binary.BigEndian.Uint32(lenBytes)))
	case header == rdbZiplistInt16:
		intBytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)), nil
	case header == rdbZiplistInt32:
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)), nil
	case header == rdbZiplistInt64:
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)), nil
	case header == rdbZiplistInt24:
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10)), nil
	case header == rdbZiplistInt8:
		b, err := buf.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(b)), 10)), err
	case header>>4 == rdbZiplistInt4:
		return []byte(strconv.FormatInt(int64(header&0x0f)-1, 10)), nil
	}

	return nil, fmt.Errorf("rdb: unknown ziplist header byte: %d", header)
}

func (d *decode) readIntset(key []byte, expiry int64) error {
	intset, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(intset)
	intSizeBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	intSize := binary.LittleEndian.Uint32(intSizeBytes)

	if intSize != 2 && intSize != 4 && intSize != 8 {
		return fmt.Errorf("rdb: unknown intset encoding: %d", intSize)
	}

	lenBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	cardinality := binary.LittleEndian.Uint32(lenBytes)

	d.event.StartSet(key, int64(cardinality), expiry)
	for i := uint32(0); i < cardinality; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return err
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		d.event.Sadd(key, []byte(intString))
	}
	d.event.EndSet(key)
	return nil
}

func (d *decode) checkHeader() error {
	header := make([]byte, 9)
	_, err := io.ReadFull(d.r, header)
	if err != nil {
		return err
	}

	if !bytes.Equal(header[:5], []byte("REDIS")) {
		return fmt.Errorf("rdb: invalid file format")
	}

	version, _ := strconv.ParseInt(string(header[5:]), 10, 64)
	if version < 1 || version > rdbVersion {
		return fmt.Errorf("rdb: invalid RDB version number %d", version)
	}

	return nil
}

func (d *decode) readString() ([]byte, error) {
	length, encoded, err := d.readLength()
	if err != nil {
		return nil, err
	}
	if encoded {
		switch length {
		case rdbEncInt8:
			i, err := d.readUint8()
			return []byte(strconv.FormatInt(int64(int8(i)), 10)), err
		case rdbEncInt16:
			i, err := d.readUint16()
			return []byte(strconv.FormatInt(int64(int16(i)), 10)), err
		case rdbEncInt32:
			i, err := d.readUint32()
			return []byte(strconv.FormatInt(int64(int32(i)), 10)), err
		case rdbEncLZF:
			clen, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			ulen, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			compressed := make([]byte, clen)
			_, err = io.ReadFull(d.r, compressed)
			if err != nil {
				return nil, err
			}
			decompressed := lzfDecompress(compressed, int(ulen))
			if len(decompressed) != int(ulen) {
				return nil, fmt.Errorf("decompressed string length %d didn't match expected length %d", len(decompressed), ulen)
			}
			return decompressed, nil
		}
	}

	str := make([]byte, length)
	_, err = io.ReadFull(d.r, str)
	return str, errors.Wrap(err, "readfailed")
}

func (d *decode) readUint8() (uint8, error) {
	b, err := d.r.ReadByte()
	return uint8(b), errors.Wrap(err, "readfailed")
}

func (d *decode) readUint16() (uint16, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:2])
	if err != nil {
		return 0, errors.Wrap(err, "readfailed")
	}
	return binary.LittleEndian.Uint16(d.intBuf), nil
}

func (d *decode) readUint32() (uint32, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return 0, errors.Wrap(err, "readfailed")
	}
	return binary.LittleEndian.Uint32(d.intBuf), nil
}

func (d *decode) readUint64() (uint64, error) {
	_, err := io.ReadFull(d.r, d.intBuf)
	if err != nil {
		return 0, errors.Wrap(err, "readfailed")
	}
	return binary.LittleEndian.Uint64(d.intBuf), nil
}

func (d *decode) readUint32Big() (uint32, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return 0, errors.Wrap(err, "readfailed")
	}
	return binary.BigEndian.Uint32(d.intBuf), nil
}

func (d *decode) readBinaryFloat64() (float64, error) {
	floatBytes := make([]byte, 8)
	_, err := io.ReadFull(d.r, floatBytes)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(floatBytes)), nil
}

// Doubles are saved as strings prefixed by an unsigned
// 8 bit integer specifying the length of the representation.
// This 8 bit integer has special values in order to specify the following
// conditions:
// 253: not a number
// 254: + inf
// 255: - inf
func (d *decode) readFloat64() (float64, error) {
	length, err := d.readUint8()
	if err != nil {
		return 0, err
	}
	switch length {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(0), nil
	case 255:
		return math.Inf(-1), nil
	default:
		floatBytes := make([]byte, length)
		_, err := io.ReadFull(d.r, floatBytes)
		if err != nil {
			return 0, err
		}
		f, err := strconv.ParseFloat(string(floatBytes), 64)
		return f, err
	}
}

func (d *decode) readLength() (uint64, bool, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, false, errors.Wrap(err, "readfailed")
	}
	// The first two bits of the first byte are used to indicate the length encoding type
	switch (b & 0xc0) >> 6 {
	case rdb6bitLen:
		// When the first two bits are 00, the next 6 bits are the length.
		return uint64(b & 0x3f), false, nil
	case rdb14bitLen:
		// When the first two bits are 01, the next 14 bits are the length.
		bb, err := d.r.ReadByte()
		if err != nil {
			return 0, false, errors.Wrap(err, "readfailed")
		}
		return (uint64(b&0x3f) << 8) | uint64(bb), false, nil
	case rdb32bitLen:
		bb, err := d.readUint32()
		if err != nil {
			return 0, false, err
		}
		return uint64(bb), false, nil
	case rdb64bitLen:
		bb, err := d.readUint64()
		if err != nil {
			return 0, false, err
		}
		return bb, false, nil
	case rdbEncVal:
		// When the first two bits are 11, the next object is encoded.
		// The next 6 bits indicate the encoding type.
		return uint64(b & 0x3f), true, nil
	default:
		// When the first two bits are 10, the next 6 bits are discarded.
		// The next 4 bytes are the length.
		length, err := d.readUint32Big()
		return uint64(length), false, err
	}

}

func verifyDump(d []byte) error {
	if len(d) < 10 {
		return fmt.Errorf("rdb: invalid dump length")
	}
	version := binary.LittleEndian.Uint16(d[len(d)-10:])
	if version > uint16(rdbVersion) {
		return fmt.Errorf("rdb: invalid version %d, expecting %d", version, rdbVersion)
	}

	if binary.LittleEndian.Uint64(d[len(d)-8:]) != crc64.Digest(d[:len(d)-8]) {
		return fmt.Errorf("rdb: invalid CRC checksum")
	}

	return nil
}

func lzfDecompress(in []byte, outlen int) []byte {
	out := make([]byte, outlen)
	for i, o := 0, 0; i < len(in); {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	return out
}
