package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandaloneStorageReader struct {
	*badger.Txn
}

func (s *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error){
	txn, err := engine_util.GetCFFromTxn(s.Txn,cf,key)
	if err != nil {
		return nil, err
	}
	return txn,nil
}

func (s *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator{
	return engine_util.NewCFIterator(cf,s.Txn)
}

func (s *StandaloneStorageReader) Close(){
	s.Txn.Discard()
}


