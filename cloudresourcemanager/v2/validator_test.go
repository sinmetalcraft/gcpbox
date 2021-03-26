package cloudresourcemanager

import (
	"testing"
)

func TestValidateProjectIDFormat(t *testing.T) {
	cases := []struct {
		name      string
		projectID string
		want      bool
	}{
		{"empty", "", false},
		{"spaceが含まれている", " gcpbox ", false},
		{"6文字未満", "hoge", false},
		{"30文字を超えている", "a123456789012345678901234567890", false},
		{"利用できない文字が含まれている", "gcpbox_hoge", false},
		{"先頭が英字で始まっていない", "1gcpbox", false},
		{"ハイフンで終わっている", "gcpbox-", false},
		{"正しい", "gcpbox", true},
		{"正しい限界の長さ", "a12345678901234567890123456789", true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := ValidateProjectIDFormat(tt.projectID)
			if got != tt.want {
				t.Errorf("want %v but got %v", tt.want, got)
			}
		})
	}
}
