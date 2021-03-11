package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db  *badger.DB
	txn *badger.Txn
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil && err.Error() == "Key not found" {
		return value, nil
	}
	return value, err
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorage) Close() {
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, conf.Raft)
	txn := db.NewTransaction(true)
	return &StandAloneStorage{db, txn}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := s.txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := s.txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
