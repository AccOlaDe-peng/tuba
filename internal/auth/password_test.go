package auth

import (
	"strings"
	"testing"
)

func TestPasswordDigestAndValidation(t *testing.T) {
	password := "Test-only-password-123!"
	first, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || strings.Contains(first, password) {
		t.Fatal("password digest must use independent salt")
	}
	if !CheckPassword(first, password) || CheckPassword(first, "wrong-password") || CheckPassword("broken", password) {
		t.Fatal("password verification failed")
	}
	for _, p := range []string{"short", strings.Repeat("a", 257)} {
		if _, err := HashPassword(p); err == nil {
			t.Fatal("invalid password accepted")
		}
	}
	for _, name := range []string{"../admin", "ab", "bad user", "admin\nroot"} {
		if _, err := NormalizeUsername(name); err == nil {
			t.Fatalf("unsafe username accepted: %q", name)
		}
	}
	if name, err := NormalizeUsername(" Admin.User "); err != nil || name != "admin.user" {
		t.Fatal("username normalization failed")
	}
}
