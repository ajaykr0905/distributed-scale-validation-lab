package main

import (
	"os"
	"strings"
	"testing"
)

func TestBinaryVerificationRejectsStaleMissingDirtyAndWrongExecutables(t *testing.T) {
	revision := strings.Repeat("a", 40)
	valid := binaryRecord{SHA256: strings.Repeat("f", 64), ModulePath: modulePath, MainPackage: modulePath + "/cmd/durable", VCSRevision: revision, VCSModified: "false"}
	if err := verifyBinary(valid, revision, valid.MainPackage, false); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"stale", "missing revision", "missing modified", "modified", "wrong package", "wrong module", "dirty checkout", "missing digest"} {
		t.Run(kind, func(t *testing.T) {
			value := valid
			dirty := false
			switch kind {
			case "stale":
				value.VCSRevision = strings.Repeat("b", 40)
			case "missing revision":
				value.VCSRevision = ""
			case "missing modified":
				value.VCSModified = ""
			case "modified":
				value.VCSModified = "true"
			case "wrong package":
				value.MainPackage = modulePath + "/cmd/lab"
			case "wrong module":
				value.ModulePath = "another/project"
			case "dirty checkout":
				dirty = true
			case "missing digest":
				value.SHA256 = ""
			}
			if err := verifyBinary(value, revision, valid.MainPackage, dirty); err == nil {
				t.Fatal("accepted unverified source")
			}
		})
	}
}

func TestInspectBinaryHashesActualGoExecutable(t *testing.T) {
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	record, err := inspectBinary(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.SHA256) != 64 || record.GoVersion == "" || record.MainPackage == "" {
		t.Fatalf("missing build record: %+v", record)
	}
	_, err = checkBinary(path, "impossible-revision", modulePath+"/cmd/durable", false, false)
	if err == nil {
		t.Fatal("accepted stale executable without explicit override")
	}
	unverified, err := checkBinary(path, "impossible-revision", modulePath+"/cmd/durable", false, true)
	if err != nil || unverified.Verified || unverified.VerificationError == "" {
		t.Fatalf("override concealed missing verification: %+v %v", unverified, err)
	}
}
