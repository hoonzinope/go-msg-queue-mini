package util

func PreviewStringRuneSafe(s string, maxRunes int) string {
	var payloadStr string = s
	// cut preview length with rune safety
	runes := []rune(payloadStr)
	if len(runes) > maxRunes {
		payloadStr = string(runes[:maxRunes]) + "..."
	}
	return payloadStr
}
