package bigquery_test

import (
	"fmt"
	"testing"

	"golang.org/x/xerrors"

	. "github.com/sinmetalcraft/gcpbox/bigquery"
)

func TestStreamingInsertError_Error(t *testing.T) {
	org := &StreamingInsertError{
		InsertID: "sampleInsertID",
		Err:      fmt.Errorf("hello sample error"),
	}
	var err error = org

	var sierr *StreamingInsertError
	if !xerrors.As(err, &sierr) {
		t.Error("err is not StreamingInsertError")
	} else {
		if e, g := org.InsertID, sierr.InsertID; e != g {
			t.Errorf("want InsertID %v but got %v", e, g)
		}
	}
}

func TestStreamingInsertErrors_Error(t *testing.T) {
	org1 := &StreamingInsertError{
		InsertID: "sampleInsertID1",
		Err:      fmt.Errorf("hello sample error 1"),
	}
	org2 := &StreamingInsertError{
		InsertID: "sampleInsertID2",
		Err:      fmt.Errorf("hello sample error 2"),
	}

	org := &StreamingInsertErrors{}
	org.Errors = append(org.Errors, org1, org2)

	var err error = org

	var sierr *StreamingInsertErrors
	if !xerrors.As(err, &sierr) {
		t.Error("err is not StreamingInsertErrors")
	} else {
		if e, g := 2, len(sierr.Errors); e != g {
			t.Errorf("want err.length %v but got %v", e, g)
		}
		errmsg := `InsertID:sampleInsertID1 : hello sample error 1
InsertID:sampleInsertID2 : hello sample error 2
`
		if e, g := errmsg, sierr.Error(); e != g {
			t.Errorf("want Error() %v but got %v", e, g)
		}
	}
}

func TestBigQueryStreamingInsertErrors_Append(t *testing.T) {
	errs := &StreamingInsertErrors{}
	errs.Append(&StreamingInsertError{
		InsertID: "sampleInsertID1",
		Err:      fmt.Errorf("hello sample error 1"),
	})
	if errs.ErrorOrNil() == nil {
		t.Errorf("want err ! but not nil")
	}
}
