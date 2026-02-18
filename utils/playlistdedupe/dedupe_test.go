package playlistdedupe

import (
	"encoding/json"
	"main/utils/ampapi"
	"reflect"
	"testing"
)

type albumMeta struct {
	id         string
	name       string
	trackCount int
	isSingle   bool
}

func buildTrack(
	t *testing.T,
	id string,
	trackType string,
	name string,
	artist string,
	isrc string,
	durationMS int,
	contentRating string,
	album *albumMeta,
) ampapi.TrackRespData {
	t.Helper()

	obj := map[string]any{
		"id":   id,
		"type": trackType,
		"attributes": map[string]any{
			"name":             name,
			"artistName":       artist,
			"isrc":             isrc,
			"durationInMillis": durationMS,
			"contentRating":    contentRating,
		},
	}

	if album != nil {
		obj["relationships"] = map[string]any{
			"albums": map[string]any{
				"data": []any{
					map[string]any{
						"id":   album.id,
						"type": "albums",
						"attributes": map[string]any{
							"name":       album.name,
							"trackCount": album.trackCount,
							"isSingle":   album.isSingle,
						},
					},
				},
			},
		}
	}

	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal track failed: %v", err)
	}
	var out ampapi.TrackRespData
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal track failed: %v", err)
	}
	return out
}

func keptIDs(result Result) []string {
	ids := make([]string, 0, len(result.Tracks))
	for _, track := range result.Tracks {
		ids = append(ids, track.ID)
	}
	return ids
}

func TestISRCDuplicateKeepsAlbumOverSingle(t *testing.T) {
	tracks := []ampapi.TrackRespData{
		buildTrack(t, "single", "songs", "Echo", "Artist", "USRC17607839", 200000, "", &albumMeta{
			id:         "s1",
			name:       "Echo (Single)",
			trackCount: 1,
			isSingle:   true,
		}),
		buildTrack(t, "album", "songs", "Echo", "Artist", "USRC17607839", 200500, "", &albumMeta{
			id:         "a1",
			name:       "Echo",
			trackCount: 10,
			isSingle:   false,
		}),
	}

	result := DedupeTracks(tracks, Options{Enabled: true, DurationToleranceMS: 2000})

	if result.RemovedCount != 1 {
		t.Fatalf("expected 1 removed track, got %d", result.RemovedCount)
	}
	if got, want := keptIDs(result), []string{"album"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected kept ids: got %v want %v", got, want)
	}
}

func TestFallbackDuplicateWithinTolerance(t *testing.T) {
	tracks := []ampapi.TrackRespData{
		buildTrack(t, "single", "songs", "Shine", "Artist", "", 200000, "explicit", &albumMeta{
			id:         "s1",
			name:       "Shine (Single)",
			trackCount: 1,
			isSingle:   true,
		}),
		buildTrack(t, "album", "songs", "Shine", "Artist", "", 201500, "explicit", &albumMeta{
			id:         "a1",
			name:       "Shine",
			trackCount: 11,
			isSingle:   false,
		}),
	}

	result := DedupeTracks(tracks, Options{Enabled: true, DurationToleranceMS: 2000})

	if result.RemovedCount != 1 {
		t.Fatalf("expected 1 removed track, got %d", result.RemovedCount)
	}
	if got, want := keptIDs(result), []string{"album"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected kept ids: got %v want %v", got, want)
	}
}

func TestFallbackDuplicateBeyondToleranceNotDeduped(t *testing.T) {
	tracks := []ampapi.TrackRespData{
		buildTrack(t, "first", "songs", "Glow", "Artist", "", 200000, "", &albumMeta{
			id:         "a1",
			name:       "Glow",
			trackCount: 9,
			isSingle:   false,
		}),
		buildTrack(t, "second", "songs", "Glow", "Artist", "", 203500, "", &albumMeta{
			id:         "a2",
			name:       "Glow",
			trackCount: 9,
			isSingle:   false,
		}),
	}

	result := DedupeTracks(tracks, Options{Enabled: true, DurationToleranceMS: 2000})

	if result.RemovedCount != 0 {
		t.Fatalf("expected 0 removed track, got %d", result.RemovedCount)
	}
	if got, want := keptIDs(result), []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected kept ids: got %v want %v", got, want)
	}
}

func TestNoDedupeAcrossDifferentTrackTypes(t *testing.T) {
	tracks := []ampapi.TrackRespData{
		buildTrack(t, "song", "songs", "Pulse", "Artist", "USRC17607839", 200000, "", &albumMeta{
			id:         "a1",
			name:       "Pulse",
			trackCount: 8,
			isSingle:   false,
		}),
		buildTrack(t, "mv", "music-videos", "Pulse", "Artist", "USRC17607839", 200000, "", &albumMeta{
			id:         "a1",
			name:       "Pulse",
			trackCount: 8,
			isSingle:   false,
		}),
	}

	result := DedupeTracks(tracks, Options{Enabled: true, DurationToleranceMS: 2000})

	if result.RemovedCount != 0 {
		t.Fatalf("expected 0 removed track, got %d", result.RemovedCount)
	}
	if got, want := keptIDs(result), []string{"song", "mv"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected kept ids: got %v want %v", got, want)
	}
}

func TestTieKeepsEarliestOccurrence(t *testing.T) {
	tracks := []ampapi.TrackRespData{
		buildTrack(t, "first", "songs", "Nova", "Artist", "USRC17607839", 200000, "", &albumMeta{
			id:         "a1",
			name:       "Nova",
			trackCount: 9,
			isSingle:   false,
		}),
		buildTrack(t, "second", "songs", "Nova", "Artist", "USRC17607839", 200000, "", &albumMeta{
			id:         "a2",
			name:       "Nova",
			trackCount: 9,
			isSingle:   false,
		}),
	}

	result := DedupeTracks(tracks, Options{Enabled: true, DurationToleranceMS: 2000})

	if result.RemovedCount != 1 {
		t.Fatalf("expected 1 removed track, got %d", result.RemovedCount)
	}
	if got, want := keptIDs(result), []string{"first"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected kept ids: got %v want %v", got, want)
	}
}

func TestOptOutReturnsUnchangedTracks(t *testing.T) {
	tracks := []ampapi.TrackRespData{
		buildTrack(t, "first", "songs", "Nova", "Artist", "USRC17607839", 200000, "", &albumMeta{
			id:         "a1",
			name:       "Nova",
			trackCount: 9,
			isSingle:   false,
		}),
		buildTrack(t, "second", "songs", "Nova", "Artist", "USRC17607839", 200000, "", &albumMeta{
			id:         "a2",
			name:       "Nova",
			trackCount: 9,
			isSingle:   false,
		}),
	}

	result := DedupeTracks(tracks, Options{Enabled: false, DurationToleranceMS: 2000})

	if result.RemovedCount != 0 {
		t.Fatalf("expected 0 removed track, got %d", result.RemovedCount)
	}
	if got, want := keptIDs(result), []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected kept ids: got %v want %v", got, want)
	}
	if got, want := result.KeptIndexes, []int{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected kept indexes: got %v want %v", got, want)
	}
}
