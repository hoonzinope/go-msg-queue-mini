package util

func ChunkSlice(list []any, chunkSize int) [][]any {
	if chunkSize <= 0 {
		return [][]any{list}
	}
	var chunks [][]any
	total := len(list)
	for i := 0; i < total; i += chunkSize {
		end := i + chunkSize
		if end > total {
			end = total
		}
		chunks = append(chunks, list[i:end])
	}
	return chunks
}
