package auth

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

var ErrCredentials = errors.New("invalid credentials")
var ErrLocked = errors.New("login temporarily locked")

const passwordIterations = 600000

func ValidatePassword(password string) error {
	if len(password) < 12 || len(password) > 256 {
		return errors.New("password must contain 12..256 bytes")
	}
	return nil
}

// HashPassword stores only a salted PBKDF2-SHA256 digest, never a recoverable password.
func HashPassword(password string) (string, error) {
	if err := ValidatePassword(password); err != nil {
		return "", err
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, passwordIterations, 32)
	if err != nil {
		return "", err
	}
	return "pbkdf2-sha256$" + strconv.Itoa(passwordIterations) + "$" + base64.RawStdEncoding.EncodeToString(salt) + "$" + base64.RawStdEncoding.EncodeToString(key), nil
}

func CheckPassword(encoded, password string) bool {
	if len(password) > 256 {
		return false
	}
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2-sha256" {
		return false
	}
	rounds, err := strconv.Atoi(parts[1])
	if err != nil || rounds != passwordIterations {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil || len(salt) != 16 {
		return false
	}
	expected, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil || len(expected) != 32 {
		return false
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, rounds, 32)
	return err == nil && subtle.ConstantTimeCompare(key, expected) == 1
}

// Equal-cost work prevents unknown usernames taking a cheaper password path.
func DummyPasswordCheck(password string) {
	_, _ = pbkdf2.Key(sha256.New, password, []byte("tuba-dummy-salt!0"), passwordIterations, 32)
}

func NormalizeUsername(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if len(name) < 3 || len(name) > 64 {
		return "", errors.New("username must contain 3..64 characters")
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return "", errors.New("invalid username")
		}
	}
	return name, nil
}
