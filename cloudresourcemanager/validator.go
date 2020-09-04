package cloudresourcemanager

// ValidateProjectIDFormat is ProjectIDのFormatを検証する
//
// ProjectIDは、6〜30文字の小文字、数字、またはハイフンの一意の文字列である必要があります。
// 文字で始める必要があり、末尾にハイフンを付けることはできません。
// https://cloud.google.com/resource-manager/docs/creating-managing-projects?hl=en#before_you_begin
// 仕様上、googleなどの文字を含むものは利用できないが、なんのワードが禁止なのか分からないので、チェックしていない。
func ValidateProjectIDFormat(projectID string) bool {
	var count int
	var lastRune rune
	for _, r := range projectID {
		if count == 0 {
			if !validateProjectIDFirstRune(r) {
				return false
			}
		}

		lastRune = r
		count++
		if count > 30 {
			return false
		}
		if !validateProjectIDRune(r) {
			return false
		}
	}
	if count < 6 {
		return false
	}
	if lastRune == '-' {
		return false
	}
	return true
}

func validateProjectIDFirstRune(v rune) bool {
	const chars = "abcdefghijklmnopqrstuvwxyz"
	for _, c := range chars {
		if c == v {
			return true
		}
	}
	return false
}

func validateProjectIDRune(v rune) bool {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789-"
	for _, c := range chars {
		if c == v {
			return true
		}
	}
	return false
}
