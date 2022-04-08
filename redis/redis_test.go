package redis

import (
	"fmt"
	"testing"
)

func TestDelete(t *testing.T) {
	fmt.Println("Test Calculate")
	expected := 4
	result := Calculate(2)
	if expected != result {
		t.Error("Failed")
	}
}
