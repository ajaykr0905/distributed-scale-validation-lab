package main

import (
	"testing"
	"time"
)

func TestPercentileDoesNotReorderRawSamples(t *testing.T) {
	values := []float64{100, 1, 20, 50, 5}
	if percentile(values, .5) != 20 || percentile(values, .95) != 50 || values[0] != 100 {
		t.Fatal("wrong quantile or mutated raw values")
	}
}

func TestResourceSnapshotRequiresEveryLiveProcess(t *testing.T) {
	for _, text := range []string{"1 S 100 2.0", "1 S 100 2.0\n2 Z 90 0", "1 S 100 2.0\n1 S 90 0", "1 S 100 2.0\n3 S 90 0"} {
		if _, _, err := parseResources(text, []string{"1", "2"}); err == nil {
			t.Fatalf("accepted incomplete/dead snapshot: %s", text)
		}
	}
	rss, cpu, err := parseResources("1 S 100 2.0\n2 R 90 1.5", []string{"1", "2"})
	if err != nil || rss != 190 || cpu != 3.5 {
		t.Fatalf("rss=%d cpu=%f err=%v", rss, cpu, err)
	}
}

func TestObservedLatencyIncludesCommitWaitAndFirstObservation(t *testing.T) {
	o := newObservations()
	start := time.Now()
	o.Start("first", start)
	// A SQL update timestamp could precede a delayed commit; no latency exists
	// until a committed result is actually observed by the polling query.
	if _, err := o.Latencies([]string{"first"}); err == nil {
		t.Fatal("unobserved result accepted")
	}
	o.Observe([]string{"first"}, start.Add(250*time.Millisecond))
	o.Observe([]string{"first"}, start.Add(time.Second))
	values, err := o.Latencies([]string{"first"})
	if err != nil || values[0] != 250 {
		t.Fatalf("latencies=%v err=%v", values, err)
	}
}
