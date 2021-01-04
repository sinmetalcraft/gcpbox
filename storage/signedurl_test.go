package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	credentials "cloud.google.com/go/iam/credentials/apiv1"
	"google.golang.org/api/iam/v1"

	. "github.com/sinmetalcraft/gcpbox/storage"
)

const testBucket = "sinmetal-ci-signed-url"

func TestStorageSignedURLService_ObjectURL_CreateAndDownload(t *testing.T) {
	ctx := context.Background()

	s := newStorageSignedURLService(t)
	fileBody, fileInfo := openFile(t)

	const object = "signedurl/TestCreatePutObjectURL"
	putURL, err := s.CreatePutObjectURL(ctx, testBucket, object, fileInfo.ContentType, time.Now().Add(600*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	// Generates *http.Request to request with PUT method to the Signed URL.
	{
		req, err := http.NewRequest("PUT", putURL, bytes.NewReader(fileBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", fileInfo.ContentType)
		req.Header.Set("Content-Length", fmt.Sprintf("%d", fileInfo.Size))
		client := new(http.Client)
		_, err = client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Download Check
	cases := []struct {
		name                   string
		downloadFileName       string
		attachment             bool
		downloadContentType    string
		wantContentType        string
		wantContentDisposition string
	}{
		{"empty", "", false, "", fileInfo.ContentType, ""},
		{"download", "", true, "", fileInfo.ContentType, "attachment;filename*=UTF-8''"},
		{"fileNameを指定してDownload", "hoge", true, "", fileInfo.ContentType, "attachment;filename*=UTF-8''hoge"},
		{"contentTypeを指定する", "", false, "plain/txt", "plain/txt", ""},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			dlURL, err := s.CreateDownloadURL(ctx, testBucket, object, time.Now().Add(10*time.Minute), &CreateDownloadSignedURLParam{
				DownloadFileName:    tt.downloadFileName,
				Attachment:          tt.attachment,
				DownloadContentType: tt.downloadContentType,
			})
			if err != nil {
				t.Fatal(err)
			}
			req, err := http.NewRequest("GET", dlURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			client := new(http.Client)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.wantContentType, resp.Header.Get("content-type"); e != g {
				t.Errorf("content-type want %s but got %s", e, g)
			}
			if e, g := tt.wantContentDisposition, resp.Header.Get("content-disposition"); e != g {
				t.Errorf("content-type want %s but got %s", e, g)
			}
			t.Log(dlURL)
		})
	}
}

func newStorageSignedURLService(t *testing.T) *StorageSignedURLService {
	ctx := context.Background()

	iamService, err := iam.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	iamCredentialsClient, err := credentials.NewIamCredentialsClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	const sa = "signedurl@sinmetal-ci.iam.gserviceaccount.com"
	s, err := NewStorageSignedURLService(ctx, sa, iamService, iamCredentialsClient)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type FileInfo struct {
	Name        string
	ContentType string
	Size        int64
}

func openFile(t *testing.T) ([]byte, *FileInfo) {
	fn := filepath.Join("testdata", "hi.png")
	file, err := os.Open(fn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Only the first 512 bytes are used to sniff the content type.
	buffer := make([]byte, 512)
	_, err = file.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}

	// Reset the read pointer if necessary.
	_, err = file.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}

	var ret FileInfo
	contentType := http.DetectContentType(buffer)
	ret.ContentType = contentType

	fi, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	ret.Name = fi.Name()
	ret.Size = fi.Size()

	body, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	return body, &ret
}
