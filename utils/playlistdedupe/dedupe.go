package playlistdedupe

import (
	"main/utils/ampapi"
	"regexp"
	"sort"
	"strings"
)

const (
	defaultDurationToleranceMS = 2000
	releaseRankUnknown         = 0
	releaseRankSingle          = 1
	releaseRankEP              = 2
	releaseRankAlbum           = 3
)

var nonAlnum = regexp.MustCompile(`[^A-Za-z0-9]+`)

type Options struct {
	Enabled             bool
	DurationToleranceMS int
}

type Result struct {
	Tracks        []ampapi.TrackRespData
	KeptIndexes   []int
	RemovedCount  int
	DroppedToKept map[int]int
}

type fallbackKey struct {
	trackType     string
	contentRating string
	artist        string
	title         string
}

type durationEntry struct {
	idx      int
	duration int
}

// DedupeTracks deduplicates playlist tracks using:
//  1. normalized ISRC (with track type isolation)
//  2. fallback normalized title+artist+type+content_rating with duration tolerance.
//
// Winners are selected by release priority (Album > EP > Single > Unknown),
// then by earliest playlist position.
func DedupeTracks(tracks []ampapi.TrackRespData, opts Options) Result {
	if len(tracks) == 0 {
		return Result{
			Tracks:        []ampapi.TrackRespData{},
			KeptIndexes:   []int{},
			RemovedCount:  0,
			DroppedToKept: map[int]int{},
		}
	}

	keepAll := func() Result {
		kept := make([]int, len(tracks))
		out := make([]ampapi.TrackRespData, len(tracks))
		for i := range tracks {
			kept[i] = i
			out[i] = tracks[i]
		}
		return Result{
			Tracks:        out,
			KeptIndexes:   kept,
			RemovedCount:  0,
			DroppedToKept: map[int]int{},
		}
	}

	if !opts.Enabled || len(tracks) == 1 {
		return keepAll()
	}

	tolerance := opts.DurationToleranceMS
	if tolerance <= 0 {
		tolerance = defaultDurationToleranceMS
	}

	releaseRanks := make([]int, len(tracks))
	isrcByIndex := make([]string, len(tracks))
	for i := range tracks {
		releaseRanks[i] = releaseRankForTrack(&tracks[i])
		isrcByIndex[i] = normalizeISRC(tracks[i].Attributes.Isrc)
	}

	winnerByIndex := make([]int, len(tracks))
	for i := range winnerByIndex {
		winnerByIndex[i] = i
	}
	droppedToKept := make(map[int]int)

	// Stage 1: ISRC grouping.
	isrcGroups := make(map[string][]int)
	for i := range tracks {
		isrc := isrcByIndex[i]
		if isrc == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(tracks[i].Type)) + "|" + isrc
		isrcGroups[key] = append(isrcGroups[key], i)
	}
	for _, members := range isrcGroups {
		if len(members) < 2 {
			continue
		}
		winner := members[0]
		for _, idx := range members[1:] {
			if betterCandidate(idx, winner, releaseRanks) {
				winner = idx
			}
		}
		for _, idx := range members {
			if idx == winner {
				continue
			}
			winnerByIndex[idx] = winner
			droppedToKept[idx] = winner
		}
	}

	// Stage 2: fallback matching for tracks that do not have ISRC.
	fallbackGroups := make(map[fallbackKey][]durationEntry)
	for i := range tracks {
		if isrcByIndex[i] != "" {
			continue
		}
		duration := tracks[i].Attributes.DurationInMillis
		if duration <= 0 {
			continue
		}
		artist := normalizeText(tracks[i].Attributes.ArtistName)
		title := normalizeText(tracks[i].Attributes.Name)
		if artist == "" || title == "" {
			continue
		}
		key := fallbackKey{
			trackType:     strings.ToLower(strings.TrimSpace(tracks[i].Type)),
			contentRating: strings.ToLower(strings.TrimSpace(tracks[i].Attributes.ContentRating)),
			artist:        artist,
			title:         title,
		}
		fallbackGroups[key] = append(fallbackGroups[key], durationEntry{
			idx:      i,
			duration: duration,
		})
	}

	for _, entries := range fallbackGroups {
		if len(entries) < 2 {
			continue
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].duration == entries[j].duration {
				return entries[i].idx < entries[j].idx
			}
			return entries[i].duration < entries[j].duration
		})

		cluster := make([]durationEntry, 0, len(entries))
		flushCluster := func() {
			if len(cluster) < 2 {
				cluster = cluster[:0]
				return
			}
			winner := cluster[0].idx
			for _, entry := range cluster[1:] {
				if betterCandidate(entry.idx, winner, releaseRanks) {
					winner = entry.idx
				}
			}
			for _, entry := range cluster {
				if entry.idx == winner {
					continue
				}
				// Keep ISRC winner decisions authoritative.
				if _, already := droppedToKept[entry.idx]; already {
					continue
				}
				winnerByIndex[entry.idx] = winner
				droppedToKept[entry.idx] = winner
			}
			cluster = cluster[:0]
		}

		for _, entry := range entries {
			if len(cluster) == 0 {
				cluster = append(cluster, entry)
				continue
			}
			last := cluster[len(cluster)-1]
			if abs(entry.duration-last.duration) <= tolerance {
				cluster = append(cluster, entry)
				continue
			}
			flushCluster()
			cluster = append(cluster, entry)
		}
		flushCluster()
	}

	keptIndexes := make([]int, 0, len(tracks))
	for i := range tracks {
		if winnerByIndex[i] == i {
			keptIndexes = append(keptIndexes, i)
		}
	}
	sort.Ints(keptIndexes)

	out := make([]ampapi.TrackRespData, 0, len(keptIndexes))
	for _, idx := range keptIndexes {
		out = append(out, tracks[idx])
	}

	return Result{
		Tracks:        out,
		KeptIndexes:   keptIndexes,
		RemovedCount:  len(tracks) - len(keptIndexes),
		DroppedToKept: droppedToKept,
	}
}

func betterCandidate(left, right int, releaseRanks []int) bool {
	if releaseRanks[left] != releaseRanks[right] {
		return releaseRanks[left] > releaseRanks[right]
	}
	return left < right
}

func releaseRankForTrack(track *ampapi.TrackRespData) int {
	if track == nil || len(track.Relationships.Albums.Data) == 0 {
		return releaseRankUnknown
	}
	album := track.Relationships.Albums.Data[0].Attributes
	name := strings.ToLower(strings.TrimSpace(album.Name))
	if album.IsSingle || strings.Contains(name, "single") {
		return releaseRankSingle
	}
	if looksLikeEPName(name) {
		return releaseRankEP
	}
	if album.TrackCount > 0 {
		if album.TrackCount <= 3 {
			return releaseRankSingle
		}
		if album.TrackCount <= 6 {
			return releaseRankEP
		}
		return releaseRankAlbum
	}
	return releaseRankUnknown
}

func looksLikeEPName(lowerName string) bool {
	return strings.Contains(lowerName, " ep") ||
		strings.HasSuffix(lowerName, " ep") ||
		strings.Contains(lowerName, "- ep") ||
		strings.Contains(lowerName, "(ep)") ||
		strings.Contains(lowerName, "[ep]")
}

func normalizeISRC(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.ToUpper(nonAlnum.ReplaceAllString(value, ""))
}

func normalizeText(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return ""
	}
	return strings.Join(strings.Fields(trimmed), " ")
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
