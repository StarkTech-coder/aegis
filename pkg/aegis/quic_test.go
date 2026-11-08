package aegis

import (
	"fmt"
	"io"
	"testing"
)

func TestExpectedQUICStreamClosureClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "io EOF",
			err:  io.EOF,
			want: true,
		},
		{
			name: "connection closed wrapping EOF",
			err:  fmt.Errorf("%w: %v", ErrConnectionClosed, io.EOF),
			want: true,
		},
		{
			name: "quic stream closed classification",
			err:  fmt.Errorf("%w: close", ErrQUICStreamClosed),
			want: true,
		},
		{
			name: "invalid packet should not be treated as closure",
			err:  fmt.Errorf("%w: malformed frame", ErrInvalidPacket),
			want: false,
		},
		{
			name: "timeout should not be treated as closure",
			err:  fmt.Errorf("%w: operation timed out", ErrTimeout),
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := isExpectedQUICStreamClosure(tt.err)
			if got != tt.want {
				t.Fatalf("unexpected closure classification for %q: got=%v want=%v err=%v", tt.name, got, tt.want, tt.err)
			}
		})
	}
}
