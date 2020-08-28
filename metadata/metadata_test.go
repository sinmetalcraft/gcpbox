package metadata_test

import (
	"fmt"
	"os"
	"testing"

	metadatabox "github.com/sinmetal/gcpbox/metadata"
	"golang.org/x/xerrors"
)

const ProjectID = "sinmetal-ci"
const ServiceAccountEmail = "401580979819@cloudbuild.gserviceaccount.com"

func TestOnGCP(t *testing.T) {
	v := metadatabox.OnGCP()
	fmt.Printf("OnGCP is %v\n", v)
}

func TestGetProjectID(t *testing.T) {
	setTestEnvValue(t)

	p, err := metadatabox.ProjectID()
	if err != nil {
		t.Fatal(err)
	}
	if e, g := ProjectID, p; e != g {
		t.Errorf("want project id %s but got %s", e, g)
	}
}

func TestServiceAccountID(t *testing.T) {
	setTestEnvValue(t)

	saID, err := metadatabox.ServiceAccountID()
	if err != nil {
		t.Fatal(err)
	}
	if e, g := fmt.Sprintf("projects/%s/serviceAccounts/%s", ProjectID, ServiceAccountEmail), saID; e != g {
		t.Errorf("want service account id %s but got %s", e, g)
	}
}

func TestServiceAccountName(t *testing.T) {
	setTestEnvValue(t)

	saName, err := metadatabox.ServiceAccountName()
	if err != nil {
		t.Fatal(err)
	}
	if e, g := "401580979819", saName; e != g {
		t.Errorf("want service account name %s but got %s", e, g)
	}
}

func TestExtractionRegion(t *testing.T) {
	cases := []struct {
		name       string
		metaZone   string
		wantResult string
		wantErr    error
	}{
		{"normal", "projects/999999999999/zones/asia-northeast1-1", "asia-northeast1", nil},
		{"invalid text pattern 1", "1", "", metadatabox.ErrInvalidArgument},
		{"invalid text pattern 2", "////", "", metadatabox.ErrInvalidArgument},
		{"empty", "", "", metadatabox.ErrInvalidArgument},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := metadatabox.ExtractionRegion(tt.metaZone)
			if err != nil {
				if !xerrors.Is(err, tt.wantErr) {
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
		wantErr    error
	}{
		{"normal", "projects/999999999999/zones/asia-northeast1-a", "asia-northeast1-a", nil},
		{"invalid text pattern 1", "1", "1", nil},   // Zone名としてValidかがなんともいい難いので、そのまま返ってきちゃう
		{"invalid text pattern 2", "////", "", nil}, // Zone名としてValidかがなんともいい難いので、そのまま返ってきちゃう
		{"empty", "", "", metadatabox.ErrInvalidArgument},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := metadatabox.ExtractionZone(tt.metaZone)
			if err != nil {
				if !xerrors.Is(err, tt.wantErr) {
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

func setTestEnvValue(t *testing.T) {
	if !metadatabox.OnGCP() {
		if err := os.Setenv("GOOGLE_CLOUD_PROJECT", ProjectID); err != nil {
			t.Fatal(err)
		}
		if err := os.Setenv("GCLOUD_SERVICE_ACCOUNT", ServiceAccountEmail); err != nil {
			t.Fatal(err)
		}
	}
}
