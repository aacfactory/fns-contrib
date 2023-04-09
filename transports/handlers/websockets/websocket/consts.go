package websocket

import "time"

const (
	CloseNormalClosure           = 1000
	CloseGoingAway               = 1001
	CloseProtocolError           = 1002
	CloseUnsupportedData         = 1003
	CloseNoStatusReceived        = 1005
	CloseAbnormalClosure         = 1006
	CloseInvalidFramePayloadData = 1007
	ClosePolicyViolation         = 1008
	CloseMessageTooBig           = 1009
	CloseMandatoryExtension      = 1010
	CloseInternalServerErr       = 1011
	CloseServiceRestart          = 1012
	CloseTryAgainLater           = 1013
	CloseTLSHandshake            = 1015
)

const (
	finalBit                   = 1 << 7
	rsv1Bit                    = 1 << 6
	rsv2Bit                    = 1 << 5
	rsv3Bit                    = 1 << 4
	maskBit                    = 1 << 7
	maxFrameHeaderSize         = 2 + 8 + 4
	maxControlFramePayloadSize = 125
	writeWait                  = time.Second
	defaultReadBufferSize      = 4096
	defaultWriteBufferSize     = 4096
	continuationFrame          = 0
	noFrame                    = -1
)

const (
	TextMessage   = 1
	BinaryMessage = 2
	CloseMessage  = 8
	PingMessage   = 9
	PongMessage   = 10
)

var validReceivedCloseCodes = map[int]bool{
	CloseNormalClosure:           true,
	CloseGoingAway:               true,
	CloseProtocolError:           true,
	CloseUnsupportedData:         true,
	CloseNoStatusReceived:        false,
	CloseAbnormalClosure:         false,
	CloseInvalidFramePayloadData: true,
	ClosePolicyViolation:         true,
	CloseMessageTooBig:           true,
	CloseMandatoryExtension:      true,
	CloseInternalServerErr:       true,
	CloseServiceRestart:          true,
	CloseTryAgainLater:           true,
	CloseTLSHandshake:            false,
}

func isValidReceivedCloseCode(code int) bool {
	return validReceivedCloseCodes[code] || (code >= 3000 && code <= 4999)
}
