package main

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"runtime/debug"
)

const modulePath = "github.com/ajaykr0905/distributed-scale-validation-lab"

type binaryRecord struct {
	SHA256            string `json:"sha256"`
	MainPackage       string `json:"main_package"`
	ModulePath        string `json:"module_path"`
	GoVersion         string `json:"go_version"`
	VCSRevision       string `json:"vcs_revision"`
	VCSTime           string `json:"vcs_time"`
	VCSModified       string `json:"vcs_modified"`
	Verified          bool   `json:"verified_build_metadata"`
	VerificationError string `json:"verification_error,omitempty"`
}

func inspectBinary(path string) (binaryRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return binaryRecord{}, err
	}
	defer file.Close()
	info, err := buildinfo.Read(file)
	if err != nil {
		return binaryRecord{}, fmt.Errorf("read Go build metadata: %w", err)
	}
	digest := sha256.New()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return binaryRecord{}, err
	}
	if _, err := io.Copy(digest, file); err != nil {
		return binaryRecord{}, err
	}
	result := recordBuildInfo(info)
	result.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return result, nil
}

func recordBuildInfo(info *debug.BuildInfo) binaryRecord {
	result := binaryRecord{MainPackage: info.Path, ModulePath: info.Main.Path, GoVersion: info.GoVersion}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			result.VCSRevision = setting.Value
		case "vcs.time":
			result.VCSTime = setting.Value
		case "vcs.modified":
			result.VCSModified = setting.Value
		}
	}
	return result
}

// Verification uses Go's embedded build declaration; it is not independent
// supply-chain attestation. SHA-256 binds the record to the inspected bytes.
func verifyBinary(record binaryRecord, revision, expectedMain string, dirty bool) error {
	if len(record.SHA256) != 64 {
		return errors.New("missing binary SHA-256")
	}
	if record.ModulePath != modulePath || record.MainPackage != expectedMain {
		return errors.New("binary main/module package differs from expected executable")
	}
	if dirty {
		return errors.New("checkout is dirty; binary source cannot be verified against HEAD")
	}
	if revision == "" || record.VCSRevision == "" || record.VCSModified == "" {
		return errors.New("missing checkout or binary VCS metadata")
	}
	if record.VCSRevision != revision {
		return fmt.Errorf("binary revision %s differs from checkout %s", record.VCSRevision, revision)
	}
	if record.VCSModified != "false" {
		return errors.New("binary was built from a modified checkout")
	}
	return nil
}

func checkBinary(path, revision, expectedMain string, dirty, allowUnverified bool) (binaryRecord, error) {
	record, err := inspectBinary(path)
	if err != nil {
		return binaryRecord{}, err
	}
	if err := verifyBinary(record, revision, expectedMain, dirty); err != nil {
		record.VerificationError = err.Error()
		if !allowUnverified {
			return record, err
		}
		return record, nil
	}
	record.Verified = true
	return record, nil
}
