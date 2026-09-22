package generator

import (
	"reflect"
	"testing"
)

func TestGenerateIsDeterministic(t *testing.T) {
	first, err := Generate("public-demo", 128)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Generate("public-demo", 128)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed and count produced different datasets")
	}
	if first[0].ID == first[1].ID {
		t.Fatal("generated endpoint IDs must be distinct")
	}
}

func TestGenerateRejectsInvalidInput(t *testing.T) {
	if _, err := Generate("", 1); err == nil {
		t.Fatal("empty seed should fail")
	}
	if _, err := Generate("demo", -1); err == nil {
		t.Fatal("negative count should fail")
	}
}
