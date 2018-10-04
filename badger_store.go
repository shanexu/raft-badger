package raftbadger

import (
	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"os"
)

var (
	dbLogs         = []byte("logs")
	dbConf         = []byte("conf")
	ErrKeyNotFound = errors.New("not found")
)

type BadgerStore struct {
	conn     *badger.DB
	path     string
}

func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	return New(opts)
}

func New(opts badger.Options) (*BadgerStore, error) {
	db, err := badger.Open(opts)
	if err := os.MkdirAll(opts.Dir, 0700); err != nil {
		return nil, errors.Wrap(err, "create Dir failed")
	}

	if os.MkdirAll(opts.ValueDir, 0700); err != nil {
		return nil, errors.Wrap(err, "create ValueDir failed")
	}
	if err != nil {
		return nil, err
	}
	return &BadgerStore{
		conn:     db,
		path:     opts.Dir,
	}, nil
}

func (b *BadgerStore) FirstIndex() (uint64, error) {
	tx := b.conn.NewTransaction(false)
	defer tx.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := tx.NewIterator(opts)
	defer it.Close()
	prefix := dbLogs
	for it.Seek(append(prefix, MIN_LOG_KEY...)); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		return bytesToUint64(k[len(prefix):]), nil
	}
	return 0, nil
}

func (b *BadgerStore) LastIndex() (uint64, error) {
	tx := b.conn.NewTransaction(false)
	defer tx.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := tx.NewIterator(opts)
	defer it.Close()
	prefix := dbLogs
	for it.Seek(append(prefix[:], MAX_LOG_KEY...)); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		return bytesToUint64(k[len(prefix):]), nil
	}
	return 0, nil
}

func (b *BadgerStore) GetLog(idx uint64, log *raft.Log) error {
	tx := b.conn.NewTransaction(false)
	defer tx.Discard()
	prefix := dbLogs
	item, err := tx.Get(append(prefix[:], uint64ToBytes(idx)...))

	if err == badger.ErrKeyNotFound {
		return raft.ErrLogNotFound
	}

	if err != nil {
		return err
	}

	val, err := item.Value()
	if err != nil {
		return err
	}

	return decodeMsgPack(val, log)
}

func (b *BadgerStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

func (b *BadgerStore) StoreLogs(logs []*raft.Log) error {
	tx := b.conn.NewTransaction(true)
	defer tx.Discard()
	prefix := dbLogs
	for _, log := range logs {
		key := append(prefix[:], uint64ToBytes(log.Index)...)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		if err := tx.Set(key, val.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit(nil)
}

func (b *BadgerStore) DeleteRange(min, max uint64) error {
	prefix := dbLogs
	minKey := append(prefix[:], uint64ToBytes(min)...)

	tx := b.conn.NewTransaction(true)
	defer tx.Discard()

	it := tx.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(minKey); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		if bytesToUint64(k[len(prefix):]) > max {
			break
		}

		if err := tx.Delete(k); err != nil {
			return err
		}
	}

	return tx.Commit(nil)
}

func (b *BadgerStore) Set(k []byte, v []byte) error {
	tx := b.conn.NewTransaction(true)
	defer tx.Discard()
	prefix := dbConf
	if err := tx.Set(append(prefix[:], k...), v); err != nil {
		return err
	}

	return tx.Commit(nil)
}

func (b *BadgerStore) Get(k []byte) ([]byte, error) {
	tx := b.conn.NewTransaction(false)
	defer tx.Discard()
	prefix := dbConf
	item, err := tx.Get(append(prefix[:], k...))

	if err == badger.ErrKeyNotFound {
		return nil, ErrKeyNotFound
	}

	if err != nil {
		return nil, err
	}

	val, err := item.Value()

	if err != nil {
		return nil, err
	}

	return val, nil
}

func (b *BadgerStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

func (b *BadgerStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

func (b *BadgerStore) Close() error {
	return b.conn.Close()
}
