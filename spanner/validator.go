package spanner

// ValidateInstanceIDFormat is InstanceIDのFormatを検証する
//
// Valid identifiers are of the form [a-z][-a-z0-9]*[a-z0-9] and must be between 2 and 64 characters in length.
// https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances/create
func ValidateInstanceIDFormat(projectID string) bool {
	var count int
	var lastRune rune
	for _, r := range projectID {
		if count == 0 {
			if !validateInstanceIDFirstRune(r) {
				return false
			}
		}

		lastRune = r
		count++
		if count > 64 {
			return false
		}
		if !validateInstanceIDRune(r) {
			return false
		}
	}
	if count < 2 {
		return false
	}
	if lastRune == '-' {
		return false
	}
	return true
}

// ValidateDatabaseIDFormat is InstanceIDのFormatを検証する
//
// The database ID must conform to the regular expression [a-z][a-z0-9_\-]*[a-z0-9] and be between 2 and 30 characters in length.
// https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/create
func ValidateDatabaseIDFormat(projectID string) bool {
	var count int
	var lastRune rune
	for _, r := range projectID {
		if count == 0 {
			if !validateDatabaseIDFirstRune(r) {
				return false
			}
		}

		lastRune = r
		count++
		if count > 30 {
			return false
		}
		if !validateDatabaseIDRune(r) {
			return false
		}
	}
	if count < 2 {
		return false
	}
	if lastRune == '-' {
		return false
	}
	return true
}

func validateInstanceIDFirstRune(v rune) bool {
	chars := []rune("abcdefghijklmnopqrstuvwxyz")
	for _, c := range chars {
		if c == v {
			return true
		}
	}
	return false
}

func validateInstanceIDRune(v rune) bool {
	chars := []rune("abcdefghijklmnopqrstuvwxyz0123456789-")
	for _, c := range chars {
		if c == v {
			return true
		}
	}
	return false
}

func validateDatabaseIDFirstRune(v rune) bool {
	chars := []rune("abcdefghijklmnopqrstuvwxyz")
	for _, c := range chars {
		if c == v {
			return true
		}
	}
	return false
}

func validateDatabaseIDRune(v rune) bool {
	chars := []rune("abcdefghijklmnopqrstuvwxyz0123456789_-")
	for _, c := range chars {
		if c == v {
			return true
		}
	}
	return false
}
