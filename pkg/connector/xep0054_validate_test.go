package connector

import (
	"bytes"
	"errors"
	"testing"
)

var (
	pngHeader  = []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00")
	jpegHeader = []byte("\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00")
)

func TestValidateAvatarBytes(t *testing.T) {
	big := append(append([]byte{}, pngHeader...), bytes.Repeat([]byte{0}, maxAvatarBytes)...)
	tests := []struct {
		name string
		in   []byte
		ok   bool
	}{
		{"png", pngHeader, true},
		{"jpeg", jpegHeader, true},
		{"gif", []byte("GIF89a\x01\x00\x01\x00"), true},
		{"svg", []byte(`<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"/>`), false},
		{"text", []byte("hello world"), false},
		{"oversize", big, false},
	}
	for _, tc := range tests {
		err := validateAvatarBytes(tc.in)
		if tc.ok && err != nil {
			t.Errorf("%s: unexpected error %v", tc.name, err)
		}
		if !tc.ok && !errors.Is(err, errAvatarRejected) {
			t.Errorf("%s: want errAvatarRejected, got %v", tc.name, err)
		}
	}
}

func TestAvatarBase64TooLarge(t *testing.T) {
	if avatarBase64TooLarge(string(bytes.Repeat([]byte("A"), 4*(maxAvatarBytes/3)))) {
		t.Error("just under the limit must not be too large")
	}
	if !avatarBase64TooLarge(string(bytes.Repeat([]byte("A"), 4*(maxAvatarBytes/3)+8))) {
		t.Error("over the limit must be too large")
	}
}
