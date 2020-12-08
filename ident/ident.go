package ident

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"sort"
)

type Identifiable interface {
	Identity() []byte
}

func ContentAddress(entity Identifiable) string {
	return hex.EncodeToString(entity.Identity())
}

type AddressType uint8

const (
	AddressTypeBytes AddressType = iota
	AddressTypeString
	AddressTypeInt64
	AddressTypeStringSlice
	AddressTypeStringMap
	AddressTypeEmbeddedIdentifiable
)

type AddressWriter struct {
	hash.Hash
}

func NewAddressWriter() *AddressWriter {
	return &AddressWriter{sha256.New()}
}

func (b *AddressWriter) marshalType(addressType AddressType) {
	_, _ = b.Write([]byte{byte(addressType)})
}

func (b *AddressWriter) MarshalBytes(v []byte) {
	b.marshalType(AddressTypeBytes)
	b.MarshalInt64(int64(len(v)))
	_, _ = b.Write(v)
}

func (b *AddressWriter) MarshalString(v string) {
	b.marshalType(AddressTypeString)
	b.MarshalInt64(int64(len(v)))
	_, _ = b.Write([]byte(v))
}

func (b *AddressWriter) MarshalInt64(v int64) {
	b.marshalType(AddressTypeInt64)
	_, _ = b.Write([]byte{8})
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(v))
	_, _ = b.Write(bytes)
}

func (b *AddressWriter) MarshalStringSlice(v []string) {
	b.marshalType(AddressTypeStringSlice)
	b.MarshalInt64(int64(len(v)))
	for _, item := range v {
		b.MarshalString(item)
	}
}

func (b *AddressWriter) MarshalStringMap(v map[string]string) {
	b.marshalType(AddressTypeStringMap)
	b.MarshalInt64(int64(len(v)))
	keys := make([]string, len(v))
	i := 0
	for k := range v {
		keys[i] = k
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.MarshalString(k)
		b.MarshalString(v[k])
	}
}

func (b *AddressWriter) MarshalIdentifiable(v Identifiable) {
	b.marshalType(AddressTypeEmbeddedIdentifiable)
	b.MarshalBytes(v.Identity())
}

func (b *AddressWriter) Identity() []byte {
	return b.Sum(nil)
}
