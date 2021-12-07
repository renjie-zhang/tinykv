package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	key := req.GetKey()
	cf := req.GetCf()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	getCF, err := reader.GetCF(cf, key)
	respon := &kvrpcpb.RawGetResponse{
		Value:                nil,
		NotFound:             false,
	}
	if err != nil {
		return respon, err
	}

	if getCF == nil{
		respon.NotFound = true
	}
	respon.Value = getCF
	return respon, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	key := req.GetKey()
	value := req.GetValue()
	cf := req.GetCf()
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data:storage.Put{
				Key:   key,
				Value: value,
				Cf:    cf,
			},
		},

	})
	r := &kvrpcpb.RawPutResponse{}
	if err != nil{
		r.Error = err.Error()
	}
	return r, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be deleted
	key := req.GetKey()
	cf := req.GetCf()
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				key,
				cf,
			},
		},
	})
	r := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		r.Error = err.Error()
	}
	return r, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	startKey := req.GetStartKey()
	limit := req.GetLimit()
	cf := req.GetCf()
	reader, _ := server.storage.Reader(nil)
	iterator := reader.IterCF(cf)

	var kvs []*kvrpcpb.KvPair
	var err error
	iterator.Seek(startKey)
	for i := 0; uint32(i) < limit; i++ {
		if !iterator.Valid() {
			break
		}
		item := iterator.Item()
		key := item.Key()
		var value []byte
		value, err = item.Value()
		if err != nil {
			break
		}
		pair := kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		kvs = append(kvs, &pair)
		iterator.Next()
	}
	response := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}
	if err != nil {
		response.Error = err.Error()
	}
	return response, err



	return nil, nil
}
