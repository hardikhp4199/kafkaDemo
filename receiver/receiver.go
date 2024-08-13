package receiver

import (
	"errors"
	"kafkaDemo/config"
	"kafkaDemo/storage/couchbase"
	"kafkaDemo/storage/logging"

	"github.com/couchbase/gocb/v2"
)

func PutDocInCouchbase(bucket string, key string, cbResult couchbase.CBRawDataResult) (err_set error) {

	if cbResult.Type == couchbase.Json {
		err_set = couchbase.Raw_AddOrUpdateDocument(bucket, key, cbResult.Value)
	} else if cbResult.Type == couchbase.String {
		err_set = couchbase.Raw_AddOrUpdateDocument(bucket, key, string(cbResult.Value))
	}
	return
}

func RemoveDocInCouchbase(bucket string, key string) (err_set error) {

	err_set = couchbase.RemoveDocument(bucket, key)
	if err_set != nil {
		if errors.Is(err_set, gocb.ErrDocumentNotFound) {
			return nil
		} else {
			return err_set
		}
	}
	return nil
}

func InsertAndDeleteDocument(key string, bucketName string, documentMetaData couchbase.CBRawDataResult) (errOut error) {
	insertBucketName := config.GetString("Couchbase.NullDocBucketName")
	err := PutDocInCouchbase(insertBucketName, key, documentMetaData)
	if err != nil {
		errOut = logging.EnrichErrorWithStackTrace(err)
	} else {
		err := RemoveDocInCouchbase(bucketName, key)
		if err != nil {
			errOut = logging.EnrichErrorWithStackTrace(err)
		}
	}
	return
}
