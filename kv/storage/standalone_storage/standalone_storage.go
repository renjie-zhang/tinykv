package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// init path
	dbPath := filepath.Join(conf.DBPath,"kv")
	raftPath := filepath.Join(conf.DBPath,"raft")
	os.MkdirAll(dbPath,os.ModePerm)
	os.MkdirAll(raftPath,os.ModePerm)

	// check enable raft
	var raftDB *badger.DB
	if conf.Raft{
		raftDB = engine_util.CreateDB(raftPath,true)
	}
	kvDB := engine_util.CreateDB(dbPath,false)
	engine := engine_util.NewEngines(kvDB,raftDB,dbPath,raftPath)

	return &StandAloneStorage{engines: engine,config: conf}
}


func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if err := s.engines.Close(); err != nil{
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandaloneStorageReader{Txn: s.engines.Kv.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engines.Kv, m.Cf(), m.Key(), m.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.engines.Kv, m.Cf(), m.Value())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
