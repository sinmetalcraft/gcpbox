package storage

// StorageClassType is Cloud Storage Class Type
// see https://cloud.google.com/storage/docs/storage-classes#classes
type StorageClassType int

// StorageClassType
const (
	MultiRegional StorageClassType = iota
	Regional
	Nearline
	Coldline
	Standard
	DurableReducedAvailability
)

// ParseStorageClassType is 文字列からStorageClassType へ変換する
func ParseStorageClassType(storageClassType string) (StorageClassType, error) {
	switch storageClassType {
	case "MULTI_REGIONAL":
		return MultiRegional, nil
	case "REGIONAL":
		return Regional, nil
	case "NEARLINE":
		return Nearline, nil
	case "COLDLINE":
		return Coldline, nil
	case "STANDARD":
		return Standard, nil
	case "DURABLE_REDUCED_AVAILABILITY":
		return DurableReducedAvailability, nil
	default:
		return -1, ErrParseFailure
	}
}
