package storage

import (
	"bytes"
	"testing"
)

var pubsubMessageBodySample = `{"message":{"data":"ew0KICAia2luZCI6ICJzdG9yYWdlI29iamVjdCIsDQogICJpZCI6ICJzdGFnaW5nLnNpbm1ldGFsLXNjaGVkdWxlci1kZXYuYXBwc3BvdC5jb20vR0NQVUctMTIucG5nLzE1MjM5NDgzMzI3NzM1NDkiLA0KICAic2VsZkxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vc3RvcmFnZS92MS9iL3N0YWdpbmcuc2lubWV0YWwtc2NoZWR1bGVyLWRldi5hcHBzcG90LmNvbS9vL0dDUFVHLTEyLnBuZyIsDQogICJuYW1lIjogIkdDUFVHLTEyLnBuZyIsDQogICJidWNrZXQiOiAic3RhZ2luZy5zaW5tZXRhbC1zY2hlZHVsZXItZGV2LmFwcHNwb3QuY29tIiwNCiAgImdlbmVyYXRpb24iOiAiMTUyMzk0ODMzMjc3MzU0OSIsDQogICJtZXRhZ2VuZXJhdGlvbiI6ICIxIiwNCiAgImNvbnRlbnRUeXBlIjogImltYWdlL3BuZyIsDQogICJ0aW1lQ3JlYXRlZCI6ICIyMDE4LTA0LTE3VDA2OjU4OjUyLjc3MFoiLA0KICAidXBkYXRlZCI6ICIyMDE4LTA0LTE3VDA2OjU4OjUyLjc3MFoiLA0KICAic3RvcmFnZUNsYXNzIjogIlNUQU5EQVJEIiwNCiAgInRpbWVTdG9yYWdlQ2xhc3NVcGRhdGVkIjogIjIwMTgtMDQtMTdUMDY6NTg6NTIuNzcwWiIsDQogICJzaXplIjogIjEzODA0MiIsDQogICJtZDVIYXNoIjogIjJFTmFzb3I4V3lodkNXVmlQN2t5WGc9PSIsDQogICJtZWRpYUxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vZG93bmxvYWQvc3RvcmFnZS92MS9iL3N0YWdpbmcuc2lubWV0YWwtc2NoZWR1bGVyLWRldi5hcHBzcG90LmNvbS9vL0dDUFVHLTEyLnBuZz9nZW5lcmF0aW9uPTE1MjM5NDgzMzI3NzM1NDkmYWx0PW1lZGlhIiwNCiAgImNyYzMyYyI6ICJJS3R0K3c9PSIsDQogICJldGFnIjogIkNLM2huL2pkd05vQ0VBRT0iDQp9","attributes":{"bucketId":"staging.sinmetal-scheduler-dev.appspot.com","objectId":"GCPUG-12.png","objectGeneration":"1523948332773549","eventTime":"2018-04-17T06:58:52.770661Z","eventType":"OBJECT_FINALIZE","payloadFormat":"JSON_API_V1","notificationConfig":"projects/_/buckets/staging.sinmetal-scheduler-dev.appspot.com/notificationConfigs/1"},"message_id":"74768261217447","messageId":"74768261217447","publish_time":"2018-04-17T06:58:53.189Z","publishTime":"2018-04-17T06:58:53.189Z"},"subscription":"projects/sinmetal-scheduler-dev/subscriptions/sample"}`

func TestReadPubSubBody(t *testing.T) {

	msg, err := ReadPubSubNotifyBody(bytes.NewBufferString(pubsubMessageBodySample))
	if err != nil {
		t.Fatal(err)
	}

	if e, g := "projects/sinmetal-scheduler-dev/subscriptions/sample", msg.Subscription; e != g {
		t.Fatalf("expected Subscriptions = %s; got %s", e, g)
	}
	if e, g := "74768261217447", msg.Message.MessageID; e != g {
		t.Fatalf("expected Message.MessageID = %s; got %s", e, g)
	}
	if msg.Message.PublishTime.IsZero() {
		t.Fatalf("Message.PuslishTime is Zero")
	}
	if e, g := "staging.sinmetal-scheduler-dev.appspot.com", msg.Message.Attributes.BucketID; e != g {
		t.Fatalf("expected Message.Attributes.BucketID = %s; got %s", e, g)
	}
	if e, g := "GCPUG-12.png", msg.Message.Attributes.ObjectID; e != g {
		t.Fatalf("expected Message.Attributes.Object = %s; got %s", e, g)
	}
	if e, g := "1523948332773549", msg.Message.Attributes.ObjectGeneration; e != g {
		t.Fatalf("expected Message.Attributes.ObjectGeneration = %s; got %s", e, g)
	}
	if msg.Message.Attributes.EventTime.IsZero() {
		t.Fatalf("Message.Attributes.EventTime is Zero")
	}
	if e, g := "ObjectFinalize", msg.Message.Attributes.EventType.String(); e != g {
		t.Fatalf("expected Message.Attributes.EventType = %s; got %s", e, g)
	}
	if e, g := "JSON_API_V1", msg.Message.Attributes.PayloadFormat; e != g {
		t.Fatalf("expected Message.Attributes.PayloadFormat = %s; got %s", e, g)
	}
	if e, g := "projects/_/buckets/staging.sinmetal-scheduler-dev.appspot.com/notificationConfigs/1", msg.Message.Attributes.NotificationConfig; e != g {
		t.Fatalf("expected Message.Attributes.NotificationConfig = %s; got %s", e, g)
	}

	if e, g := "storage#object", msg.Message.Data.Kind; e != g {
		t.Fatalf("expected Message.Data.Kind = %s; got %s", e, g)
	}
	if e, g := "staging.sinmetal-scheduler-dev.appspot.com/GCPUG-12.png/1523948332773549", msg.Message.Data.ID; e != g {
		t.Fatalf("expected Message.Data.ID = %s; got %s", e, g)
	}
	if e, g := "https://www.googleapis.com/storage/v1/b/staging.sinmetal-scheduler-dev.appspot.com/o/GCPUG-12.png", msg.Message.Data.SelfLink; e != g {
		t.Fatalf("expected Message.Data.SelfLink = %s; got %s", e, g)
	}
	if e, g := "GCPUG-12.png", msg.Message.Data.Name; e != g {
		t.Fatalf("expected Message.Data.Name = %s; got %s", e, g)
	}
	if e, g := "staging.sinmetal-scheduler-dev.appspot.com", msg.Message.Data.Bucket; e != g {
		t.Fatalf("expected Message.Data.Bucket = %s; got %s", e, g)
	}
	if e, g := 1523948332773549, msg.Message.Data.Generation; e != g {
		t.Fatalf("expected Message.Data.Generation = %d; got %d", e, g)
	}
	if e, g := 1, msg.Message.Data.Metageneration; e != g {
		t.Fatalf("expected Message.Data.Metageneration = %d; got %d", e, g)
	}
	if e, g := "image/png", msg.Message.Data.ContentType; e != g {
		t.Fatalf("expected Message.Data.ContentType = %s; got %s", e, g)
	}
	if msg.Message.Data.TimeCreated.IsZero() {
		t.Fatalf("Message.Data.TimeCreated is Zero")
	}
	if msg.Message.Data.Updated.IsZero() {
		t.Fatalf("Message.Data.Updated is Zero")
	}
	if e, g := Standard, msg.Message.Data.StorageClass; e != g {
		t.Fatalf("expected Message.Data.StorageClass = %s; got %s", e, g)
	}
	if msg.Message.Data.TimeStorageClassUpdated.IsZero() {
		t.Fatalf("Message.Data.TimeStorageClassUpdated is Zero")
	}
	if e, g := 138042, msg.Message.Data.Size; e != g {
		t.Fatalf("expected Message.Data.Size = %d; got %d", e, g)
	}
	if e, g := "2ENasor8WyhvCWViP7kyXg==", msg.Message.Data.MD5Hash; e != g {
		t.Fatalf("expected Message.Data.MD5Hash = %s; got %s", e, g)
	}
	if e, g := "https://www.googleapis.com/download/storage/v1/b/staging.sinmetal-scheduler-dev.appspot.com/o/GCPUG-12.png?generation=1523948332773549&alt=media", msg.Message.Data.MediaLink; e != g {
		t.Fatalf("expected Message.Data.MediaLink = %s; got %s", e, g)
	}
	if e, g := "IKtt+w==", msg.Message.Data.CRC32C; e != g {
		t.Fatalf("expected Message.Data.CRC32C = %s; got %s", e, g)
	}
	if e, g := "CK3hn/jdwNoCEAE=", msg.Message.Data.Etag; e != g {
		t.Fatalf("expected Message.Data.Etag = %s; got %s", e, g)
	}
}
