package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

type StandAloneStorageIterator struct {
	cf   string
	iter *badger.Iterator
	txn  *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	options := badger.DefaultOptions
	options.Dir = s.conf.DBPath
	options.ValueDir = s.conf.DBPath
	db, err := badger.Open(options)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &StandAloneStorageReader{
		txn: txn,
	}, nil
}

type DBItem struct {
	badger.Item
	cf string
}

func (item *DBItem) Key() []byte {
	return item.Item.Key()[len(item.cf)+1:]
}

func (item *DBItem) KeyCopy(dst []byte) []byte {
	raw := item.Item.KeyCopy(dst)
	return raw[len(item.cf)+1:]
}

func (i *StandAloneStorageIterator) Item() engine_util.DBItem {
	raw := i.iter.Item()
	return &DBItem{Item: *raw, cf: i.cf}
}

func (i *StandAloneStorageIterator) Valid() bool {
	return i.iter.ValidForPrefix([]byte(i.cf))
}

func (i *StandAloneStorageIterator) Next() {
	i.iter.Next()
}

func (i *StandAloneStorageIterator) Seek(key []byte) {
	i.iter.Seek(engine_util.KeyWithCF(i.cf, key))
}

func (i *StandAloneStorageIterator) Close() {
	i.iter.Close()
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	innerKey := engine_util.KeyWithCF(cf, key)
	item, err := r.txn.Get(innerKey)
	if err != nil {
		if err.Error() == "Key not found" {
			return nil, nil
		}
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	opts := badger.DefaultIteratorOptions
	iter := r.txn.NewIterator(opts)
	return &StandAloneStorageIterator{
		cf:   cf,
		iter: iter,
	}
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if len(batch) > 0 {
		err := s.db.Update(func(txn *badger.Txn) error {
			for _, modify := range batch {
				key := engine_util.KeyWithCF(modify.Cf(), modify.Key())
				var err1 error
				switch modify.Data.(type) {
				case storage.Put:
					err1 = txn.Set(key, modify.Value())
				case storage.Delete:
					err1 = txn.Delete(key)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
