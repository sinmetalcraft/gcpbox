package metadata_test

import (
	"fmt"
	"testing"

	metadatabox "github.com/sinmetal/gcpbox/metadata"
)

func TestOnGCP(t *testing.T) {
	v := metadatabox.OnGCP()
	fmt.Printf("OnGCP is %v\n", v)
}

func TestGetProjectID(t *testing.T) {
	onGCP := metadatabox.OnGCP()

	p, err := metadatabox.ProjectID()
	if onGCP {
		if err != nil {
			t.Fatal(err)
		}
	} else {
		if err != nil {
			if !metadatabox.Is(err, metadatabox.ErrNotFoundCode) {
				t.Fatal(err)
			}
			fmt.Println("GetProjectID is NotFound")
			return
		}
	}
	fmt.Printf("GetProjectID is %v\n", p)
}

func TestExtractionRegion(t *testing.T) {
	cases := []struct {
		name       string
		metaZone   string
		wantResult string
		wantErr    metadatabox.ErrCode
	}{
		{"normal", "projects/999999999999/zones/asia-northeast1-1", "asia-northeast1", 0},
		{"invalid text pattern 1", "1", "", metadatabox.ErrInvalidArgumentCode},
		{"invalid text pattern 2", "////", "", metadatabox.ErrInvalidArgumentCode},
		{"empty", "", "", metadatabox.ErrInvalidArgumentCode},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := metadatabox.ExtractionRegion(tt.metaZone)
			if err != nil {
				if !metadatabox.Is(err, tt.wantErr) {
					t.Errorf("want error %v but got %v", tt.wantErr, err)
				}
				return
			}
			if got != tt.wantResult {
				t.Errorf("want result %v but got %v", tt.wantResult, got)
			}
		})
	}
}

func TestExtractionZone(t *testing.T) {
	cases := []struct {
		name       string
		metaZone   string
		wantResult string
		wantErr    metadatabox.ErrCode
	}{
		{"normal", "projects/999999999999/zones/asia-northeast1-a", "asia-northeast1-a", 0},
		{"invalid text pattern 1", "1", "1", 0},   // Zone名としてValidかがなんともいい難いので、そのまま返ってきちゃう
		{"invalid text pattern 2", "////", "", 0}, // Zone名としてValidかがなんともいい難いので、そのまま返ってきちゃう
		{"empty", "", "", metadatabox.ErrInvalidArgumentCode},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := metadatabox.ExtractionZone(tt.metaZone)
			if err != nil {
				if !metadatabox.Is(err, tt.wantErr) {
					t.Errorf("want error %v but got %v", tt.wantErr, err)
				}
				return
			}
			if got != tt.wantResult {
				t.Errorf("want result %v but got %v", tt.wantResult, got)
			}
		})
	}
}
