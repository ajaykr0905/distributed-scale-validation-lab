package main

import "testing"

func TestPercentileDoesNotReorderRawSamples(t *testing.T) {
	values := []float64{100, 1, 20, 50, 5}
	if percentile(values, .5) != 20 || percentile(values, .95) != 50 || values[0] != 100 {
		t.Fatal("wrong quantile or mutated raw values")
	}
}
