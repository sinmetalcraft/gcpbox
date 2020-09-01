package spanner

import "testing"

func TestValidateInstanceIDFormat(t *testing.T) {
	cases := []struct {
		name       string
		instanceID string
		want       bool
	}{
		{"empty", "", false},
		{"spaceが含まれている", " gcpbox ", false},
		{"2文字未満", "h", false},
		{"64文字を超えている", "abcde123456789012345678901234567890123456789012345678901234567890", false},
		{"利用できない文字が含まれている", "gcpbox_hoge", false},
		{"先頭が英字で始まっていない", "1gcpbox", false},
		{"ハイフンで終わっている", "gcpbox-", false},
		{"正しい", "gc", true},
		{"正しい限界の長さ", "abcd123456789012345678901234567890123456789012345678901234567890", true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := ValidateInstanceIDFormat(tt.instanceID)
			if got != tt.want {
				t.Errorf("want %v but got %v", tt.want, got)
			}
		})
	}
}

func TestValidateDatabaseIDFormat(t *testing.T) {
	cases := []struct {
		name       string
		databaseID string
		want       bool
	}{
		{"empty", "", false},
		{"spaceが含まれている", " gcpbox ", false},
		{"2文字未満", "h", false},
		{"30文字を超えている", "a123456789012345678901234567890", false},
		{"利用できない文字が含まれている", "gcpbox^hoge", false},
		{"先頭が英字で始まっていない", "1gcpbox", false},
		{"ハイフンで終わっている", "gcpbox-", false},
		{"正しい", "g-c", true},
		{"正しいアンダーバー入り", "g_c", true},
		{"正しい限界の長さ", "a12345678901234567890123456789", true},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := ValidateDatabaseIDFormat(tt.databaseID)
			if got != tt.want {
				t.Errorf("want %v but got %v", tt.want, got)
			}
		})
	}
}
