package indexing

import "tuba/product/internal/event"

type Document struct {
	Event event.Authentication
	Raw   []byte
}
type Result struct {
	Status        int
	Code, Message string
	Retryable     bool
}
