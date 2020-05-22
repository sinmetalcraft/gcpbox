package storage

import "errors"

// PubSubNotifyEventType is Cloud Storage PubSub Notification EventType
// see https://cloud.google.com/storage/docs/pubsub-notifications#events
type PubSubNotifyEventType int

// PubSubNotifyEventType
const (
	ObjectFinalize PubSubNotifyEventType = iota
	ObjectMetaDataUpdate
	ObjectDelete
	ObjectArchive
)

// ErrParseFailure is Parse失敗時のError
var ErrParseFailure = errors.New("parse fail")

// ParseStorageNotifyEventType is 文字列から PubSubNotifyEventType へ変換する
func ParseStorageNotifyEventType(eventType string) (PubSubNotifyEventType, error) {
	switch eventType {
	case "OBJECT_FINALIZE":
		return ObjectFinalize, nil
	case "OBJECT_METADATA_UPDATE":
		return ObjectMetaDataUpdate, nil
	case "OBJECT_DELETE":
		return ObjectDelete, nil
	case "OBJECT_ARCHIVE":
		return ObjectArchive, nil
	default:
		return -1, ErrParseFailure
	}
}
