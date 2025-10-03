package util

func ChunkSlice[T any](list []T, chunkSize int) [][]T {
	if chunkSize <= 0 {
		return [][]T{list}
	}
	var chunks [][]T
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
