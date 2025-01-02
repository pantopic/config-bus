package patch

import (
	"encoding/binary"

	"github.com/logbn/icarus/internal/patch/lcs"
)

// Generate returns a patch representing the difference between a and b
func Generate(a, b, patch []byte) []byte {
	diffs := lcs.DiffBytes(a, b)
	patch = binary.AppendUvarint(patch, uint64(len(b)))
	patch = binary.AppendUvarint(patch, uint64(len(diffs)))
	for _, diff := range diffs {
		patch = binary.AppendUvarint(patch, uint64(diff.Start))
		patch = binary.AppendUvarint(patch, uint64(diff.End))
		patch = binary.AppendUvarint(patch, uint64(diff.ReplEnd-diff.ReplStart))
		patch = append(patch, b[diff.ReplStart:diff.ReplEnd]...)
	}
	return patch
}
