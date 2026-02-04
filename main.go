package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"main/utils/ampapi"
	"main/utils/lyrics"
	"main/utils/runv2"
	"main/utils/runv3"
	"main/utils/structs"
	"main/utils/task"

	"github.com/AlecAivazis/survey/v2"
	"github.com/fatih/color"
	"github.com/grafov/m3u8"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/pflag"
	"github.com/zhaarey/go-mp4tag"
	"gopkg.in/yaml.v2"
)

var (
	forbiddenNames = regexp.MustCompile(`[/\\<>:"|?*]`)
	prefetchKeyURI = "skd://itunes.apple.com/P000000000/s1/e1"
	dl_atmos       bool
	dl_aac         bool
	dl_select      bool
	dl_song        bool
	dl_preview     bool
	dl_lyrics_only bool
	dl_covers_only bool
	artist_select  bool
	debug_mode     bool
	select_tracks  string
	abortRetries   bool
	alac_max       *int
	atmos_max      *int
	mv_max         *int
	mv_audio_type  *string
	aac_type       *string
	Config         structs.ConfigSet
	counter        structs.Counter
	okDict         = make(map[string][]int)
)

func loadConfig() error {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &Config)
	if err != nil {
		return err
	}
	if len(Config.Storefront) != 2 {
		Config.Storefront = "us"
	}
	return nil
}

func LimitString(s string) string {
	if len([]rune(s)) > Config.LimitMax {
		return string([]rune(s)[:Config.LimitMax])
	}
	return s
}

func isInArray(arr []int, target int) bool {
	for _, num := range arr {
		if num == target {
			return true
		}
	}
	return false
}

func mediaPlaylistHasPrefetchKey(mediaURL string) (bool, error) {
	resp, err := http.Get(mediaURL)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, errors.New(resp.Status)
	}
	playlist, listType, err := m3u8.DecodeFrom(resp.Body, true)
	if err != nil {
		return false, err
	}
	if listType != m3u8.MEDIA {
		return false, errors.New("m3u8 not of media type")
	}
	media := playlist.(*m3u8.MediaPlaylist)
	for _, segment := range media.Segments {
		if segment == nil || segment.Key == nil {
			continue
		}
		if segment.Key.URI == prefetchKeyURI {
			return true, nil
		}
	}
	return false, nil
}

func fileExists(path string) (bool, error) {
	f, err := os.Stat(path)
	if err == nil {
		return !f.IsDir(), nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func checkUrl(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/album|\/album\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlMv(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music)\.apple\.com\/(\w{2})(?:\/music-video|\/music-video\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlSong(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/song|\/song\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlPlaylist(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/playlist|\/playlist\/.+))\/(?:id)?(pl\.[\w-]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}

func checkUrlStation(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music)\.apple\.com\/(\w{2})(?:\/station|\/station\/.+))\/(?:id)?(ra\.[\w-]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}

func checkUrlArtist(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/artist|\/artist\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func getUrlSong(songUrl string, token string) (string, error) {
	storefront, songId := checkUrlSong(songUrl)
	manifest, err := ampapi.GetSongResp(storefront, songId, Config.Language, token)
	if err != nil {
		fmt.Println("\u26A0 Failed to get manifest:", err)
		counter.NotSong++
		return "", err
	}
	albumId := manifest.Data[0].Relationships.Albums.Data[0].ID
	songAlbumUrl := fmt.Sprintf("https://music.apple.com/%s/album/1/%s?i=%s", storefront, albumId, songId)
	return songAlbumUrl, nil
}
func getUrlArtistName(artistUrl string, token string) (string, string, error) {
	storefront, artistId := checkUrlArtist(artistUrl)
	req, err := http.NewRequest("GET", fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s", storefront, artistId), nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Origin", "https://music.apple.com")
	query := url.Values{}
	query.Set("l", Config.Language)
	req.URL.RawQuery = query.Encode()
	do, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer do.Body.Close()
	if do.StatusCode != http.StatusOK {
		return "", "", errors.New(do.Status)
	}
	obj := new(structs.AutoGeneratedArtist)
	err = json.NewDecoder(do.Body).Decode(&obj)
	if err != nil {
		return "", "", err
	}
	return obj.Data[0].Attributes.Name, obj.Data[0].ID, nil
}

func checkArtist(artistUrl string, token string, relationship string) ([]string, error) {
	storefront, artistId := checkUrlArtist(artistUrl)
	Num := 0
	//id := 1
	var args []string
	var urls []string
	var options [][]string
	for {
		req, err := http.NewRequest("GET", fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s/%s?limit=100&offset=%d&l=%s", storefront, artistId, relationship, Num, Config.Language), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
		req.Header.Set("Origin", "https://music.apple.com")
		do, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer do.Body.Close()
		if do.StatusCode != http.StatusOK {
			return nil, errors.New(do.Status)
		}
		obj := new(structs.AutoGeneratedArtist)
		err = json.NewDecoder(do.Body).Decode(&obj)
		if err != nil {
			return nil, err
		}
		for _, album := range obj.Data {
			options = append(options, []string{album.Attributes.Name, album.Attributes.ReleaseDate, album.ID, album.Attributes.URL})
		}
		Num = Num + 100
		if len(obj.Next) == 0 {
			break
		}
	}
	sort.Slice(options, func(i, j int) bool {
		// 将日期字符串解析为 time.Time 类型进行比较
		dateI, _ := time.Parse("2006-01-02", options[i][1])
		dateJ, _ := time.Parse("2006-01-02", options[j][1])
		return dateI.Before(dateJ) // 返回 true 表示 i 在 j 前面
	})

	table := tablewriter.NewWriter(os.Stdout)
	if relationship == "albums" {
		table.SetHeader([]string{"", "Album Name", "Date", "Album ID"})
	} else if relationship == "music-videos" {
		table.SetHeader([]string{"", "MV Name", "Date", "MV ID"})
	}
	table.SetRowLine(false)
	table.SetHeaderColor(tablewriter.Colors{},
		tablewriter.Colors{tablewriter.FgRedColor, tablewriter.Bold},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor})

	table.SetColumnColor(tablewriter.Colors{tablewriter.FgCyanColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgRedColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor})
	for i, v := range options {
		urls = append(urls, v[3])
		options[i] = append([]string{fmt.Sprint(i + 1)}, v[:3]...)
		table.Append(options[i])
	}
	table.Render()
	if artist_select {
		fmt.Println("You have selected all options:")
		return urls, nil
	}
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Please select from the " + relationship + " options above (multiple options separated by commas, ranges supported, or type 'all' to select all)")
	cyanColor := color.New(color.FgCyan)
	cyanColor.Print("Enter your choice: ")
	input, _ := reader.ReadString('\n')

	input = strings.TrimSpace(input)
	if input == "all" {
		fmt.Println("You have selected all options:")
		return urls, nil
	}

	selectedOptions := [][]string{}
	parts := strings.Split(input, ",")
	for _, part := range parts {
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			selectedOptions = append(selectedOptions, rangeParts)
		} else {
			selectedOptions = append(selectedOptions, []string{part})
		}
	}

	fmt.Println("You have selected the following options:")
	for _, opt := range selectedOptions {
		if len(opt) == 1 {
			num, err := strconv.Atoi(opt[0])
			if err != nil {
				fmt.Println("Invalid option:", opt[0])
				continue
			}
			if num > 0 && num <= len(options) {
				fmt.Println(options[num-1])
				args = append(args, urls[num-1])
			} else {
				fmt.Println("Option out of range:", opt[0])
			}
		} else if len(opt) == 2 {
			start, err1 := strconv.Atoi(opt[0])
			end, err2 := strconv.Atoi(opt[1])
			if err1 != nil || err2 != nil {
				fmt.Println("Invalid range:", opt)
				continue
			}
			if start < 1 || end > len(options) || start > end {
				fmt.Println("Range out of range:", opt)
				continue
			}
			for i := start; i <= end; i++ {
				fmt.Println(options[i-1])
				args = append(args, urls[i-1])
			}
		} else {
			fmt.Println("Invalid option:", opt)
		}
	}
	return args, nil
}

func writeCover(sanAlbumFolder, name string, url string) (string, error) {
	originalUrl := url
	var ext string
	var covPath string
	if Config.CoverFormat == "original" {
		ext = strings.Split(url, "/")[len(strings.Split(url, "/"))-2]
		ext = ext[strings.LastIndex(ext, ".")+1:]
		covPath = filepath.Join(sanAlbumFolder, name+"."+ext)
	} else {
		covPath = filepath.Join(sanAlbumFolder, name+"."+Config.CoverFormat)
	}
	exists, err := fileExists(covPath)
	if err != nil {
		fmt.Println("Failed to check if cover exists.")
		return "", err
	}
	if exists {
		_ = os.Remove(covPath)
	}
	if Config.CoverFormat == "png" {
		re := regexp.MustCompile(`\{w\}x\{h\}`)
		parts := re.Split(url, 2)
		url = parts[0] + "{w}x{h}" + strings.Replace(parts[1], ".jpg", ".png", 1)
	}
	url = strings.Replace(url, "{w}x{h}", Config.CoverSize, 1)
	if Config.CoverFormat == "original" {
		url = strings.Replace(url, "is1-ssl.mzstatic.com/image/thumb", "a5.mzstatic.com/us/r1000/0", 1)
		url = url[:strings.LastIndex(url, "/")]
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	do, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer do.Body.Close()
	if do.StatusCode != http.StatusOK {
		if Config.CoverFormat == "original" {
			fmt.Println("Failed to get cover, falling back to " + ext + " url.")
			splitByDot := strings.Split(originalUrl, ".")
			last := splitByDot[len(splitByDot)-1]
			fallback := originalUrl[:len(originalUrl)-len(last)] + ext
			fallback = strings.Replace(fallback, "{w}x{h}", Config.CoverSize, 1)
			fmt.Println("Fallback URL:", fallback)
			req, err = http.NewRequest("GET", fallback, nil)
			if err != nil {
				fmt.Println("Failed to create request for fallback url.")
				return "", err
			}
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
			do, err = http.DefaultClient.Do(req)
			if err != nil {
				fmt.Println("Failed to get cover from fallback url.")
				return "", err
			}
			defer do.Body.Close()
			if do.StatusCode != http.StatusOK {
				fmt.Println(fallback)
				return "", errors.New(do.Status)
			}
		} else {
			return "", errors.New(do.Status)
		}
	}
	f, err := os.Create(covPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = io.Copy(f, do.Body)
	if err != nil {
		return "", err
	}
	return covPath, nil
}

func writeLyrics(sanAlbumFolder, filename string, lrc string) error {
	lyricspath := filepath.Join(sanAlbumFolder, filename)
	f, err := os.Create(lyricspath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(lrc)
	if err != nil {
		return err
	}
	return nil
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func isInteractive() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func parseTrackSelection(input string, max int) ([]int, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, nil
	}
	if strings.EqualFold(trimmed, "all") {
		out := make([]int, max)
		for i := 0; i < max; i++ {
			out[i] = i + 1
		}
		return out, nil
	}

	selection := make(map[int]struct{})
	parts := strings.Split(trimmed, ",")
	for _, raw := range parts {
		part := strings.TrimSpace(raw)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			rangeParts := strings.SplitN(part, "-", 2)
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range: %s", part)
			}
			start, err1 := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			end, err2 := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("invalid range: %s", part)
			}
			if start < 1 || end > max || start > end {
				return nil, fmt.Errorf("range out of bounds: %s", part)
			}
			for i := start; i <= end; i++ {
				selection[i] = struct{}{}
			}
			continue
		}

		num, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid selection: %s", part)
		}
		if num < 1 || num > max {
			return nil, fmt.Errorf("selection out of bounds: %s", part)
		}
		selection[num] = struct{}{}
	}

	out := make([]int, 0, len(selection))
	for key := range selection {
		out = append(out, key)
	}
	sort.Ints(out)
	return out, nil
}

func detectReleaseType(name string, trackCount int, isSingle bool) string {
	lower := strings.ToLower(name)
	if isSingle || strings.Contains(lower, "single") {
		return "Singles"
	}
	if strings.Contains(lower, " ep") ||
		strings.HasSuffix(lower, " ep") ||
		strings.Contains(lower, "- ep") ||
		strings.Contains(lower, "(ep)") ||
		strings.Contains(lower, "[ep]") {
		return "EPs"
	}
	if trackCount > 0 {
		if trackCount <= 3 {
			return "Singles"
		}
		if trackCount <= 6 {
			return "EPs"
		}
	}
	return "Albums"
}

func releaseFolderLabel(releaseType string) string {
	switch strings.ToLower(strings.TrimSpace(releaseType)) {
	case "ep", "eps":
		return "EPs"
	case "single", "singles":
		return "Singles"
	default:
		return "Albums"
	}
}

func sanitizeFolderName(name string) string {
	name = strings.TrimSpace(name)
	if strings.HasSuffix(name, ".") {
		name = strings.ReplaceAll(name, ".", "")
	}
	return strings.TrimSpace(name)
}

func currentRootFolder() string {
	if dl_atmos {
		return Config.AtmosSaveFolder
	}
	if dl_aac {
		return Config.AacSaveFolder
	}
	return Config.AlacSaveFolder
}

func buildArtistFolderName(artistName, artistID string) string {
	if Config.ArtistFolderFormat == "" {
		return ""
	}
	folder := strings.NewReplacer(
		"{UrlArtistName}", LimitString(artistName),
		"{ArtistName}", LimitString(artistName),
		"{ArtistId}", artistID,
	).Replace(Config.ArtistFolderFormat)
	return sanitizeFolderName(folder)
}

func stopSignalPath() string {
	return "stop.signal"
}

func clearStopSignal() {
	if _, err := os.Stat(stopSignalPath()); err == nil {
		_ = os.Remove(stopSignalPath())
	}
}

func stopRequested() bool {
	_, err := os.Stat(stopSignalPath())
	return err == nil
}

func checkStopAndWarn() bool {
	if stopRequested() {
		fmt.Println("🛑 Stop signal detected, exiting gracefully.")
		return true
	}
	return false
}

func isConnectionRefused(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "connection refused")
}

func markAbortRetries(err error) {
	if isConnectionRefused(err) {
		abortRetries = true
	}
}

func shouldRetryWrapper(err error) bool {
	if err == nil {
		return false
	}
	if isConnectionRefused(err) {
		return true
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "decryptfragment") && strings.Contains(lower, "eof")
}

func waitForWrapperReady(addr string, attempts int, delay time.Duration) bool {
	for i := 0; i < attempts; i++ {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(delay)
	}
	return false
}

func coverFilePath(folder, name, url string) string {
	if Config.CoverFormat == "original" {
		ext := strings.Split(url, "/")[len(strings.Split(url, "/"))-2]
		ext = ext[strings.LastIndex(ext, ".")+1:]
		return filepath.Join(folder, name+"."+ext)
	}
	return filepath.Join(folder, name+"."+Config.CoverFormat)
}

func relativeToRoot(dir, root string) (string, bool) {
	if root == "" {
		return "", false
	}
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return "", false
	}
	if rel == "." {
		return "", true
	}
	if strings.HasPrefix(rel, "..") {
		return "", false
	}
	return rel, true
}

func siblingDirsForPath(dir string) []string {
	roots := []string{Config.AlacSaveFolder, Config.AtmosSaveFolder, Config.AacSaveFolder}
	var rel string
	var base string
	for _, root := range roots {
		if root == "" {
			continue
		}
		if candidate, ok := relativeToRoot(dir, root); ok {
			rel = candidate
			base = root
			break
		}
	}
	if base == "" {
		return nil
	}
	out := []string{}
	for _, root := range roots {
		if root == "" || root == base {
			continue
		}
		if rel == "" {
			out = append(out, root)
		} else {
			out = append(out, filepath.Join(root, rel))
		}
	}
	return out
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return nil
}

func findExistingSiblingFile(dir, filename string) (string, bool) {
	target := filepath.Join(dir, filename)
	exists, err := fileExists(target)
	if err == nil && exists {
		return target, true
	}
	for _, sibling := range siblingDirsForPath(dir) {
		candidate := filepath.Join(sibling, filename)
		exists, err := fileExists(candidate)
		if err == nil && exists {
			return candidate, true
		}
	}
	return "", false
}

func ensureCoverFile(dir, name, url string) (string, error) {
	target := coverFilePath(dir, name, url)
	exists, err := fileExists(target)
	if err == nil && exists {
		return target, nil
	}
	for _, sibling := range siblingDirsForPath(dir) {
		candidate := coverFilePath(sibling, name, url)
		if ok, err := fileExists(candidate); err == nil && ok {
			if err := copyFile(candidate, target); err == nil {
				return target, nil
			}
		}
	}
	return writeCover(dir, name, url)
}

func downloadAnimatedArtworkSquare(folder string, videoURL string) {
	if videoURL == "" {
		return
	}
	motionvideoUrlSquare, err := extractVideo(videoURL)
	if err != nil {
		fmt.Println("no motion video square.\n", err)
		return
	}
	exists, err := fileExists(filepath.Join(folder, "square_animated_artwork.mp4"))
	if err != nil {
		fmt.Println("Failed to check if animated artwork square exists.")
		return
	}
	if exists {
		fmt.Println("Animated artwork square already exists locally.")
		return
	}
	fmt.Println("Animation Artwork Square Downloading...")
	cmd := exec.Command("ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlSquare, "-c", "copy", filepath.Join(folder, "square_animated_artwork.mp4"))
	if err := cmd.Run(); err != nil {
		fmt.Printf("animated artwork square dl err: %v\n", err)
		return
	}
	fmt.Println("Animation Artwork Square Downloaded")

	if Config.EmbyAnimatedArtwork {
		cmd3 := exec.Command("ffmpeg", "-i", filepath.Join(folder, "square_animated_artwork.mp4"), "-vf", "scale=440:-1", "-r", "24", "-f", "gif", filepath.Join(folder, "folder.jpg"))
		if err := cmd3.Run(); err != nil {
			fmt.Printf("animated artwork square to gif err: %v\n", err)
		}
	}
}

func handleCoversOnlyAlbum(albumFolderPath string, artistFolder string, coverURL string, artistCoverURL string, animatedSquareURL string) {
	if Config.SaveArtistCover && artistCoverURL != "" {
		if _, err := ensureCoverFile(artistFolder, "folder", artistCoverURL); err != nil {
			fmt.Println("Failed to write artist cover.")
		}
	}
	if _, err := ensureCoverFile(albumFolderPath, "cover", coverURL); err != nil {
		fmt.Println("Failed to write cover.")
	}
	if Config.SaveAnimatedArtwork && dl_atmos {
		downloadAnimatedArtworkSquare(albumFolderPath, animatedSquareURL)
	}
}

func hasAtmosVariant(m3u8Url string) (bool, error) {
	resp, err := http.Get(m3u8Url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, errors.New(resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	from, listType, err := m3u8.DecodeFrom(strings.NewReader(string(body)), true)
	if err != nil || listType != m3u8.MASTER {
		return false, errors.New("m3u8 not of master type")
	}
	master := from.(*m3u8.MasterPlaylist)
	for _, variant := range master.Variants {
		if variant.Codecs == "ec-3" && strings.Contains(strings.ToLower(variant.Audio), "atmos") {
			return true, nil
		}
	}
	return false, nil
}

func defaultConvertFormats() []string {
	return []string{"lossless", "hires", "aac"}
}

func isHiResQuality(quality string) bool {
	parts := strings.Split(quality, "-")
	if len(parts) < 2 {
		return false
	}
	last := parts[len(parts)-1]
	last = strings.TrimSpace(strings.TrimSuffix(last, "kHz"))
	if last == "" {
		return false
	}
	value, err := strconv.ParseFloat(last, 64)
	if err != nil {
		return false
	}
	return value > 48
}

func formatKeyForTrack(track *task.Track) string {
	codec := strings.ToUpper(track.Codec)
	switch codec {
	case "ATMOS":
		return "atmos"
	case "AAC":
		return "aac"
	}
	if codec == "ALAC" {
		if isHiResQuality(track.Quality) {
			return "hires"
		}
		return "lossless"
	}
	return "lossless"
}

func shouldConvertTrack(track *task.Track) bool {
	formats := Config.ConvertFormats
	if len(formats) == 0 {
		formats = defaultConvertFormats()
	}
	formatKey := formatKeyForTrack(track)
	for _, entry := range formats {
		if strings.EqualFold(entry, formatKey) {
			return true
		}
	}
	return false
}

func shouldEmitHistory() bool {
	return !dl_lyrics_only && !dl_covers_only
}

func albumIDForTrack(track *task.Track) string {
	if track.PreType == "albums" && track.PreID != "" {
		return track.PreID
	}
	if len(track.Resp.Relationships.Albums.Data) > 0 {
		return track.Resp.Relationships.Albums.Data[0].ID
	}
	return ""
}

func albumNameForTrack(track *task.Track) string {
	if track.AlbumData.ID != "" {
		return track.AlbumData.Attributes.Name
	}
	if track.Resp.Attributes.AlbumName != "" {
		return track.Resp.Attributes.AlbumName
	}
	if len(track.Resp.Relationships.Albums.Data) > 0 {
		return track.Resp.Relationships.Albums.Data[0].Attributes.Name
	}
	return "Unknown Album"
}

func albumArtistForTrack(track *task.Track) string {
	if track.AlbumData.ID != "" {
		return track.AlbumData.Attributes.ArtistName
	}
	if len(track.Resp.Relationships.Albums.Data) > 0 {
		return track.Resp.Relationships.Albums.Data[0].Attributes.ArtistName
	}
	if track.Resp.Attributes.ArtistName != "" {
		return track.Resp.Attributes.ArtistName
	}
	return "Unknown Artist"
}

func releaseTypeForTrack(track *task.Track) string {
	if track.AlbumData.ID != "" {
		return detectReleaseType(track.AlbumData.Attributes.Name, track.AlbumData.Attributes.TrackCount, track.AlbumData.Attributes.IsSingle)
	}
	if len(track.Resp.Relationships.Albums.Data) > 0 {
		album := track.Resp.Relationships.Albums.Data[0].Attributes
		return detectReleaseType(album.Name, album.TrackCount, album.IsSingle)
	}
	return "Albums"
}

func emitHistoryEntry(track *task.Track) {
	if !shouldEmitHistory() {
		return
	}
	entry := map[string]any{
		"_history_entry": "download",
		"artist":         albumArtistForTrack(track),
		"album":          albumNameForTrack(track),
		"release_type":   releaseTypeForTrack(track),
		"album_id":       albumIDForTrack(track),
		"track_num":      track.Resp.Attributes.TrackNumber,
		"track_name":     track.Resp.Attributes.Name,
		"storefront":     track.Storefront,
	}
	if track.Resp.Attributes.TrackNumber == 0 {
		entry["track_num"] = track.TaskNum
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		fmt.Println("Failed to emit history:", err)
		return
	}
	fmt.Printf("HISTORY:%s\n", string(payload))
}

func getLyricsWithFallback(track *task.Track, token string, mediaUserToken string) (string, error) {
	lrcStr, err := lyrics.Get(track.Storefront, track.ID, Config.LrcType, Config.Language, Config.LrcFormat, token, mediaUserToken)
	if err == nil && lrcStr != "" {
		return lrcStr, nil
	}
	if Config.Language != "" {
		fallback, fallbackErr := lyrics.Get(track.Storefront, track.ID, Config.LrcType, "", Config.LrcFormat, token, mediaUserToken)
		if fallbackErr == nil && fallback != "" {
			return fallback, nil
		}
		if fallbackErr != nil {
			if err != nil {
				return "", fmt.Errorf("%v; fallback failed: %w", err, fallbackErr)
			}
			return "", fallbackErr
		}
	}
	if err != nil {
		return "", err
	}
	return "", errors.New("no lyrics found")
}

func resolveAlbumQuality(storefront, trackID, language, token string, codec string, audioTraits []string) (string, string) {
	quality := ""
	resolvedCodec := codec
	if !strings.Contains(Config.AlbumFolderFormat, "Quality") {
		return quality, resolvedCodec
	}
	if trackID == "" {
		return quality, resolvedCodec
	}

	if dl_atmos {
		return fmt.Sprintf("%dKbps", Config.AtmosMax-2000), resolvedCodec
	}
	if dl_aac && Config.AacType == "aac-lc" {
		return "256Kbps", resolvedCodec
	}

	manifest1, err := ampapi.GetSongResp(storefront, trackID, language, token)
	if err != nil {
		fmt.Println("Failed to get manifest.\n", err)
		return quality, resolvedCodec
	}
	if manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls == "" {
		resolvedCodec = "AAC"
		return "256Kbps", resolvedCodec
	}

	needCheck := false
	if Config.GetM3u8Mode == "all" {
		needCheck = true
	} else if Config.GetM3u8Mode == "hires" && contains(audioTraits, "hi-res-lossless") {
		needCheck = true
	}
	if needCheck {
		EnhancedHls_m3u8, err := checkM3u8(trackID, "album")
		if err != nil {
			markAbortRetries(err)
		}
		if strings.HasSuffix(EnhancedHls_m3u8, ".m3u8") {
			manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls = EnhancedHls_m3u8
		}
	}
	_, quality, err = extractMedia(manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls, true)
	if err != nil {
		fmt.Println("Failed to extract quality from manifest.\n", err)
	}
	return quality, resolvedCodec
}

// START: New functions for search functionality

// SearchResultItem is a unified struct to hold search results for display.
type SearchResultItem struct {
	Type   string
	Name   string
	Detail string
	URL    string
	ID     string
}

// QualityOption holds information about a downloadable quality.
type QualityOption struct {
	ID          string
	Description string
}

type PreviewTrack struct {
	Num    int    `json:"num"`
	Name   string `json:"name"`
	Artist string `json:"artist,omitempty"`
	Album  string `json:"album,omitempty"`
}

type PreviewPayload struct {
	Kind        string         `json:"kind,omitempty"`
	Artist      string         `json:"artist,omitempty"`
	Title       string         `json:"title,omitempty"`
	ReleaseType string         `json:"release_type,omitempty"`
	TrackCount  int            `json:"track_count,omitempty"`
	Tracks      []PreviewTrack `json:"tracks,omitempty"`
	Preselected []int          `json:"preselected,omitempty"`
}

// setDlFlags configures the global download flags based on the user's quality selection.
func setDlFlags(quality string) {
	dl_atmos = false
	dl_aac = false

	switch quality {
	case "atmos":
		dl_atmos = true
		fmt.Println("Quality set to: Dolby Atmos")
	case "aac":
		dl_aac = true
		*aac_type = "aac"
		fmt.Println("Quality set to: High-Quality (AAC)")
	case "alac":
		fmt.Println("Quality set to: Lossless (ALAC)")
	}
}

// promptForQuality asks the user to select a download quality for the chosen media.
func promptForQuality(item SearchResultItem, token string) (string, error) {
	if item.Type == "Artist" {
		fmt.Println("Artist selected. Proceeding to list all albums/videos.")
		return "default", nil
	}

	fmt.Printf("\nFetching available qualities for: %s\n", item.Name)

	qualities := []QualityOption{
		{ID: "alac", Description: "Lossless (ALAC)"},
		{ID: "aac", Description: "High-Quality (AAC)"},
		{ID: "atmos", Description: "Dolby Atmos"},
	}
	qualityOptions := []string{}
	for _, q := range qualities {
		qualityOptions = append(qualityOptions, q.Description)
	}

	prompt := &survey.Select{
		Message:  "Select a quality to download:",
		Options:  qualityOptions,
		PageSize: 5,
	}

	selectedIndex := 0
	err := survey.AskOne(prompt, &selectedIndex)
	if err != nil {
		// This can happen if the user presses Ctrl+C
		return "", nil
	}

	return qualities[selectedIndex].ID, nil
}

// handleSearch manages the entire interactive search process.
func handleSearch(searchType string, queryParts []string, token string) (string, error) {
	query := strings.Join(queryParts, " ")
	validTypes := map[string]bool{"album": true, "song": true, "artist": true}
	if !validTypes[searchType] {
		return "", fmt.Errorf("invalid search type: %s. Use 'album', 'song', or 'artist'", searchType)
	}

	fmt.Printf("Searching for %ss: \"%s\" in storefront \"%s\"\n", searchType, query, Config.Storefront)

	offset := 0
	limit := 15 // Increased limit for better navigation

	apiSearchType := searchType + "s"

	for {
		searchResp, err := ampapi.Search(Config.Storefront, query, apiSearchType, Config.Language, token, limit, offset)
		if err != nil {
			return "", fmt.Errorf("error fetching search results: %w", err)
		}

		var items []SearchResultItem
		var displayOptions []string
		hasNext := false

		// Special options for navigation
		const prevPageOpt = "⬅️  Previous Page"
		const nextPageOpt = "➡️  Next Page"

		// Add previous page option if applicable
		if offset > 0 {
			displayOptions = append(displayOptions, prevPageOpt)
		}

		switch searchType {
		case "album":
			if searchResp.Results.Albums != nil {
				for _, item := range searchResp.Results.Albums.Data {
					year := ""
					if len(item.Attributes.ReleaseDate) >= 4 {
						year = item.Attributes.ReleaseDate[:4]
					}
					trackInfo := fmt.Sprintf("%d tracks", item.Attributes.TrackCount)
					detail := fmt.Sprintf("%s (%s, %s)", item.Attributes.ArtistName, year, trackInfo)
					displayOptions = append(displayOptions, fmt.Sprintf("%s - %s", item.Attributes.Name, detail))
					items = append(items, SearchResultItem{Type: "Album", URL: item.Attributes.URL, ID: item.ID})
				}
				hasNext = searchResp.Results.Albums.Next != ""
			}
		case "song":
			if searchResp.Results.Songs != nil {
				for _, item := range searchResp.Results.Songs.Data {
					detail := fmt.Sprintf("%s (%s)", item.Attributes.ArtistName, item.Attributes.AlbumName)
					displayOptions = append(displayOptions, fmt.Sprintf("%s - %s", item.Attributes.Name, detail))
					items = append(items, SearchResultItem{Type: "Song", URL: item.Attributes.URL, ID: item.ID})
				}
				hasNext = searchResp.Results.Songs.Next != ""
			}
		case "artist":
			if searchResp.Results.Artists != nil {
				for _, item := range searchResp.Results.Artists.Data {
					detail := ""
					if len(item.Attributes.GenreNames) > 0 {
						detail = strings.Join(item.Attributes.GenreNames, ", ")
					}
					displayOptions = append(displayOptions, fmt.Sprintf("%s (%s)", item.Attributes.Name, detail))
					items = append(items, SearchResultItem{Type: "Artist", URL: item.Attributes.URL, ID: item.ID})
				}
				hasNext = searchResp.Results.Artists.Next != ""
			}
		}

		if len(items) == 0 && offset == 0 {
			fmt.Println("No results found.")
			return "", nil
		}

		// Add next page option if applicable
		if hasNext {
			displayOptions = append(displayOptions, nextPageOpt)
		}

		prompt := &survey.Select{
			Message:  "Use arrow keys to navigate, Enter to select:",
			Options:  displayOptions,
			PageSize: limit, // Show a full page of results
		}

		selectedIndex := 0
		err = survey.AskOne(prompt, &selectedIndex)
		if err != nil {
			// User pressed Ctrl+C
			return "", nil
		}

		selectedOption := displayOptions[selectedIndex]

		// Handle pagination
		if selectedOption == nextPageOpt {
			offset += limit
			continue
		}
		if selectedOption == prevPageOpt {
			offset -= limit
			continue
		}

		// Adjust index to match the `items` slice if "Previous Page" was an option
		itemIndex := selectedIndex
		if offset > 0 {
			itemIndex--
		}

		selectedItem := items[itemIndex]

		// Automatically set single song download flag
		if selectedItem.Type == "Song" {
			dl_song = true
		}

		quality, err := promptForQuality(selectedItem, token)
		if err != nil {
			return "", fmt.Errorf("could not process quality selection: %w", err)
		}
		if quality == "" { // User cancelled quality selection
			fmt.Println("Selection cancelled.")
			return "", nil
		}

		if quality != "default" {
			setDlFlags(quality)
		}

		return selectedItem.URL, nil
	}
}

func buildPreviewPayload(rawUrl string, token string) (*PreviewPayload, error) {
	parsed, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}
	preselectID := parsed.Query().Get("i")

	if storefront, albumId := checkUrl(rawUrl); albumId != "" {
		album, err := ampapi.GetAlbumResp(storefront, albumId, Config.Language, token)
		if err != nil {
			return nil, err
		}
		meta := album.Data[0]
		tracks := make([]PreviewTrack, 0, len(meta.Relationships.Tracks.Data))
		preselected := []int{}
		for i, track := range meta.Relationships.Tracks.Data {
			tracks = append(tracks, PreviewTrack{
				Num:    i + 1,
				Name:   track.Attributes.Name,
				Artist: track.Attributes.ArtistName,
				Album:  meta.Attributes.Name,
			})
			if preselectID != "" && track.ID == preselectID {
				preselected = []int{i + 1}
			}
		}
		return &PreviewPayload{
			Kind:        "Album",
			Artist:      meta.Attributes.ArtistName,
			Title:       meta.Attributes.Name,
			ReleaseType: detectReleaseType(meta.Attributes.Name, meta.Attributes.TrackCount, meta.Attributes.IsSingle),
			TrackCount:  meta.Attributes.TrackCount,
			Tracks:      tracks,
			Preselected: preselected,
		}, nil
	}

	if storefront, playlistId := checkUrlPlaylist(rawUrl); playlistId != "" {
		playlist, err := ampapi.GetPlaylistResp(storefront, playlistId, Config.Language, token)
		if err != nil {
			return nil, err
		}
		meta := playlist.Data[0]
		tracks := make([]PreviewTrack, 0, len(meta.Relationships.Tracks.Data))
		for i, track := range meta.Relationships.Tracks.Data {
			tracks = append(tracks, PreviewTrack{
				Num:    i + 1,
				Name:   track.Attributes.Name,
				Artist: track.Attributes.ArtistName,
				Album:  track.Attributes.AlbumName,
			})
		}
		artistName := meta.Attributes.ArtistName
		if artistName == "" {
			artistName = "Apple Music"
		}
		return &PreviewPayload{
			Kind:        "Playlist",
			Artist:      artistName,
			Title:       meta.Attributes.Name,
			ReleaseType: "Playlists",
			TrackCount:  len(tracks),
			Tracks:      tracks,
		}, nil
	}

	if storefront, songId := checkUrlSong(rawUrl); songId != "" {
		song, err := ampapi.GetSongResp(storefront, songId, Config.Language, token)
		if err != nil {
			return nil, err
		}
		data := song.Data[0]
		releaseType := "Singles"
		if len(data.Relationships.Albums.Data) > 0 {
			album := data.Relationships.Albums.Data[0].Attributes
			releaseType = detectReleaseType(album.Name, album.TrackCount, album.IsSingle)
		}
		return &PreviewPayload{
			Kind:        "Song",
			Artist:      data.Attributes.ArtistName,
			Title:       data.Attributes.Name,
			ReleaseType: releaseType,
			TrackCount:  1,
			Tracks: []PreviewTrack{
				{
					Num:    1,
					Name:   data.Attributes.Name,
					Artist: data.Attributes.ArtistName,
					Album:  data.Attributes.AlbumName,
				},
			},
			Preselected: []int{1},
		}, nil
	}

	return nil, errors.New("unsupported url")
}

// END: New functions for search functionality

// CONVERSION FEATURE: Determine if source codec is lossy (rough heuristic by extension/codec name).
func isLossySource(ext string, codec string) bool {
	ext = strings.ToLower(ext)
	if ext == ".m4a" && (codec == "AAC" || strings.Contains(codec, "AAC") || strings.Contains(codec, "ATMOS")) {
		return true
	}
	if ext == ".mp3" || ext == ".opus" || ext == ".ogg" {
		return true
	}
	return false
}

// CONVERSION FEATURE: Build ffmpeg arguments for desired target.
func buildFFmpegArgs(ffmpegPath, inPath, outPath, targetFmt, extraArgs string) ([]string, error) {
	args := []string{"-y", "-i", inPath, "-vn"}
	switch targetFmt {
	case "flac":
		args = append(args, "-c:a", "flac")
	case "mp3":
		// VBR quality 2 ~ high quality
		args = append(args, "-c:a", "libmp3lame", "-qscale:a", "2")
	case "opus":
		// Medium/high quality
		args = append(args, "-c:a", "libopus", "-b:a", "192k", "-vbr", "on")
	case "wav":
		args = append(args, "-c:a", "pcm_s16le")
	case "copy":
		// Just container copy (probably pointless for same container)
		args = append(args, "-c", "copy")
	default:
		return nil, fmt.Errorf("unsupported convert-format: %s", targetFmt)
	}
	if extraArgs != "" {
		// naive split; for complex quoting you could enhance
		args = append(args, strings.Fields(extraArgs)...)
	}
	args = append(args, outPath)
	return args, nil
}

// CONVERSION FEATURE: Perform conversion if enabled.
func convertIfNeeded(track *task.Track) {
	if !Config.ConvertAfterDownload {
		return
	}
	if Config.ConvertFormat == "" {
		return
	}
	if !shouldConvertTrack(track) {
		fmt.Printf("Conversion skipped (format %s not selected)\n", formatKeyForTrack(track))
		return
	}
	srcPath := track.SavePath
	if srcPath == "" {
		return
	}
	ext := strings.ToLower(filepath.Ext(srcPath))
	targetFmt := strings.ToLower(Config.ConvertFormat)

	// Map extension for output
	if targetFmt == "copy" {
		fmt.Println("Convert (copy) requested; skipping because it produces no new format.")
		return
	}

	if Config.ConvertSkipIfSourceMatch {
		if ext == "."+targetFmt {
			fmt.Printf("Conversion skipped (already %s)\n", targetFmt)
			return
		}
	}

	outBase := strings.TrimSuffix(srcPath, ext)
	outPath := outBase + "." + targetFmt

	// Handle lossy -> lossless cases: optionally skip or warn
	if (targetFmt == "flac" || targetFmt == "wav") && isLossySource(ext, track.Codec) {
		if Config.ConvertSkipLossyToLossless {
			fmt.Println("Skipping conversion: source appears lossy and target is lossless; configured to skip.")
			return
		}
		if Config.ConvertWarnLossyToLossless {
			fmt.Println("Warning: Converting lossy source to lossless container will not improve quality.")
		}
	}

	if _, err := exec.LookPath(Config.FFmpegPath); err != nil {
		fmt.Printf("ffmpeg not found at '%s'; skipping conversion.\n", Config.FFmpegPath)
		return
	}

	args, err := buildFFmpegArgs(Config.FFmpegPath, srcPath, outPath, targetFmt, Config.ConvertExtraArgs)
	if err != nil {
		fmt.Println("Conversion config error:", err)
		return
	}

	fmt.Printf("Converting -> %s ...\n", targetFmt)
	cmd := exec.Command(Config.FFmpegPath, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	start := time.Now()
	if err := cmd.Run(); err != nil {
		fmt.Println("Conversion failed:", err)
		// leave original
		return
	}
	fmt.Printf("Conversion completed in %s: %s\n", time.Since(start).Truncate(time.Millisecond), filepath.Base(outPath))

	if !Config.ConvertKeepOriginal {
		if err := os.Remove(srcPath); err != nil {
			fmt.Println("Failed to remove original after conversion:", err)
		} else {
			track.SavePath = outPath
			track.SaveName = filepath.Base(outPath)
			fmt.Println("Original removed.")
		}
	} else {
		// Keep both but point track to new file (optional decision)
		track.SavePath = outPath
		track.SaveName = filepath.Base(outPath)
	}
}

func buildSongName(track *task.Track, quality string) string {
	stringsToJoin := []string{}
	if track.Resp.Attributes.IsAppleDigitalMaster {
		if Config.AppleMasterChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.AppleMasterChoice)
		}
	}
	if track.Resp.Attributes.ContentRating == "explicit" {
		if Config.ExplicitChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.ExplicitChoice)
		}
	}
	if track.Resp.Attributes.ContentRating == "clean" {
		if Config.CleanChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.CleanChoice)
		}
	}
	tagString := strings.Join(stringsToJoin, " ")
	trackNumber := track.Resp.Attributes.TrackNumber
	if trackNumber == 0 {
		trackNumber = track.TaskNum
	}
	return strings.NewReplacer(
		"{SongId}", track.ID,
		"{SongNumer}", fmt.Sprintf("%02d", trackNumber),
		"{SongName}", LimitString(track.Resp.Attributes.Name),
		"{DiscNumber}", fmt.Sprintf("%0d", track.Resp.Attributes.DiscNumber),
		"{TrackNumber}", fmt.Sprintf("%0d", trackNumber),
		"{Quality}", quality,
		"{Tag}", tagString,
		"{Codec}", track.Codec,
	).Replace(Config.SongFileFormat)
}

func normalizedArtistNames(names []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func formatArtistList(names []string) string {
	normalized := normalizedArtistNames(names)
	if len(normalized) == 0 {
		return ""
	}
	return strings.Join(normalized, ", ")
}

func primaryArtist(names []string) string {
	normalized := normalizedArtistNames(names)
	if len(normalized) == 0 {
		return ""
	}
	return normalized[0]
}

func artistNamesFromTrack(track *task.Track) []string {
	names := []string{}
	if track != nil {
		for _, artist := range track.Resp.Relationships.Artists.Data {
			names = append(names, artist.Attributes.Name)
		}
		if len(names) > 0 {
			return normalizedArtistNames(names)
		}
		if track.Resp.Attributes.ArtistName != "" {
			return []string{track.Resp.Attributes.ArtistName}
		}
	}
	return nil
}

func artistNamesFromAlbumData(album *ampapi.AlbumRespData) []string {
	if album == nil {
		return nil
	}
	names := []string{}
	for _, artist := range album.Relationships.Artists.Data {
		names = append(names, artist.Attributes.Name)
	}
	if len(names) > 0 {
		return normalizedArtistNames(names)
	}
	if album.Attributes.ArtistName != "" {
		return []string{album.Attributes.ArtistName}
	}
	return nil
}

func artistNamesFromMusicVideoData(video *ampapi.MusicVideoRespData) []string {
	if video == nil {
		return nil
	}
	names := []string{}
	for _, artist := range video.Relationships.Artists.Data {
		names = append(names, artist.Attributes.Name)
	}
	if len(names) > 0 {
		return normalizedArtistNames(names)
	}
	if video.Attributes.ArtistName != "" {
		return []string{video.Attributes.ArtistName}
	}
	return nil
}

func albumArtistNamesFromTrack(track *task.Track) []string {
	if track == nil {
		return nil
	}
	if track.AlbumData.ID != "" {
		if names := artistNamesFromAlbumData(&track.AlbumData); len(names) > 0 {
			return names
		}
	}
	if len(track.Resp.Relationships.Albums.Data) > 0 {
		albumRel := track.Resp.Relationships.Albums.Data[0]
		if albumRel.Attributes.ArtistName != "" {
			return []string{albumRel.Attributes.ArtistName}
		}
	}
	if track.Resp.Attributes.ArtistName != "" {
		return []string{track.Resp.Attributes.ArtistName}
	}
	return nil
}

func trackSupportsCurrentFormat(track *task.Track) bool {
	if track == nil {
		return false
	}
	if dl_atmos {
		if track.WebM3u8 == "" {
			return false
		}
		available, err := hasAtmosVariant(track.WebM3u8)
		if err != nil {
			fmt.Println("Atmos availability check failed:", err)
			return false
		}
		return available
	}
	if dl_aac {
		return true
	}
	traits := track.Resp.Attributes.AudioTraits
	if Config.GetM3u8Mode == "hires" {
		return contains(traits, "hi-res-lossless")
	}
	if len(traits) == 0 {
		return true
	}
	return contains(traits, "lossless") || contains(traits, "hi-res-lossless")
}

func anySelectedTrackSupportsFormat(tracks []task.Track, selected []int) bool {
	if len(tracks) == 0 || len(selected) == 0 {
		return false
	}
	for _, idx := range selected {
		if idx <= 0 || idx > len(tracks) {
			continue
		}
		if trackSupportsCurrentFormat(&tracks[idx-1]) {
			return true
		}
	}
	return false
}

func ripTrack(track *task.Track, token string, mediaUserToken string) bool {
	if checkStopAndWarn() {
		return false
	}
	var err error
	counter.Total++
	fmt.Printf("Track %d of %d: %s\n", track.TaskNum, track.TaskTotal, track.Type)

	//提前获取到的播放列表下track所在的专辑信息
	if track.PreType == "playlists" && Config.UseSongInfoForPlaylist {
		if track.AlbumData.ID == "" {
			track.GetAlbumData(token)
		}
	}

	//mv dl dev
	if track.Type == "music-videos" {
		if len(mediaUserToken) <= 50 {
			fmt.Println("meida-user-token is not set, skip MV dl")
			counter.Success++
			return true
		}
		if _, err := exec.LookPath("mp4decrypt"); err != nil {
			fmt.Println("mp4decrypt is not found, skip MV dl")
			counter.Success++
			return true
		}
		err := mvDownloader(track.ID, track.SaveDir, token, track.Storefront, mediaUserToken, track)
		if err != nil {
			fmt.Println("\u26A0 Failed to dl MV:", err)
			counter.Error++
			return false
		}
		counter.Success++
		return true
	}

	if dl_atmos {
		if track.WebM3u8 == "" {
			fmt.Println("Atmos not available for this track.")
			counter.Unavailable++
			return false
		}
		available, err := hasAtmosVariant(track.WebM3u8)
		if err != nil {
			fmt.Println("Atmos availability check failed:", err)
			counter.Unavailable++
			markAbortRetries(err)
			return false
		}
		if !available {
			fmt.Println("Atmos not available for this track.")
			counter.Unavailable++
			return false
		}
	}

	needDlAacLc := false
	if dl_aac && Config.AacType == "aac-lc" {
		needDlAacLc = true
	}
	if track.WebM3u8 == "" && !needDlAacLc {
		if dl_atmos {
			fmt.Println("Unavailable")
			counter.Unavailable++
			return false
		}
		fmt.Println("Unavailable, trying to dl aac-lc")
		needDlAacLc = true
	}

	needCheck := false
	if Config.GetM3u8Mode == "all" {
		needCheck = true
	} else if Config.GetM3u8Mode == "hires" && contains(track.Resp.Attributes.AudioTraits, "hi-res-lossless") {
		needCheck = true
	}
	if needCheck && !needDlAacLc {
		EnhancedHls_m3u8, err := checkM3u8(track.ID, "song")
		if err != nil {
			markAbortRetries(err)
		}
		if strings.HasSuffix(EnhancedHls_m3u8, ".m3u8") {
			track.DeviceM3u8 = EnhancedHls_m3u8
			track.M3u8 = EnhancedHls_m3u8
		}
	}

	var Quality string
	if strings.Contains(Config.SongFileFormat, "Quality") {
		if dl_atmos {
			Quality = fmt.Sprintf("%dKbps", Config.AtmosMax-2000)
		} else if needDlAacLc {
			Quality = "256Kbps"
		} else {
			_, Quality, err = extractMedia(track.M3u8, true)
			if err != nil {
				fmt.Println("Failed to extract quality from manifest.\n", err)
				counter.Error++
				return false
			}
		}
	}
	track.Quality = Quality

	songName := buildSongName(track, Quality)
	fmt.Println(songName)
	filename := fmt.Sprintf("%s.m4a", forbiddenNames.ReplaceAllString(songName, "_"))
	track.SaveName = filename
	trackPath := filepath.Join(track.SaveDir, track.SaveName)
	lrcFilename := fmt.Sprintf("%s.%s", forbiddenNames.ReplaceAllString(songName, "_"), Config.LrcFormat)

	// Determine possible post-conversion target file (so we can skip re-download)
	var convertedPath string
	considerConverted := false
	if Config.ConvertAfterDownload &&
		Config.ConvertFormat != "" &&
		strings.ToLower(Config.ConvertFormat) != "copy" &&
		!Config.ConvertKeepOriginal {
		convertedPath = strings.TrimSuffix(trackPath, filepath.Ext(trackPath)) + "." + strings.ToLower(Config.ConvertFormat)
		considerConverted = true
	}

	// Existence check now considers converted output (if original was deleted)
	existsOriginal, err := fileExists(trackPath)
	if err != nil {
		fmt.Println("Failed to check if track exists.")
	}
	if existsOriginal {
		fmt.Println("Track already exists locally.")
		counter.Success++
		okDict[track.PreID] = append(okDict[track.PreID], track.TaskNum)
		emitHistoryEntry(track)
		return true
	}
	if considerConverted {
		existsConverted, err2 := fileExists(convertedPath)
		if err2 == nil && existsConverted {
			fmt.Println("Converted track already exists locally.")
			counter.Success++
			okDict[track.PreID] = append(okDict[track.PreID], track.TaskNum)
			emitHistoryEntry(track)
			return true
		}
	}

	if needDlAacLc {
		if len(mediaUserToken) <= 50 {
			fmt.Println("Invalid media-user-token")
			counter.Error++
			return false
		}
		_, err := runv3.Run(track.ID, trackPath, token, mediaUserToken, false, "")
		if err != nil {
			fmt.Println("Failed to dl aac-lc:", err)
			if err.Error() == "Unavailable" {
				counter.Unavailable++
				return false
			}
			counter.Error++
			return false
		}
	} else {
		trackM3u8Url, _, err := extractMedia(track.M3u8, false)
		if err != nil {
			fmt.Println("\u26A0 Failed to extract info from manifest:", err)
			counter.Unavailable++
			return false
		}
		if Config.GetM3u8FromDevice {
			hasPrefetch, err := mediaPlaylistHasPrefetchKey(trackM3u8Url)
			if err != nil {
				fmt.Println("⚠️ Failed to inspect media playlist for prefetch key:", err)
			} else if hasPrefetch {
				fmt.Println("⚠️ Prefetch-only key detected; requesting device m3u8...")
				deviceM3u8, _ := checkM3u8(track.ID, "song")
				if strings.HasSuffix(deviceM3u8, ".m3u8") {
					track.DeviceM3u8 = deviceM3u8
					track.M3u8 = deviceM3u8
					trackM3u8Url, _, err = extractMedia(track.M3u8, false)
					if err != nil {
						fmt.Println("\u26A0 Failed to extract info from device manifest:", err)
						counter.Unavailable++
						return false
					}
				} else {
					fmt.Println("⚠️ Device m3u8 unavailable; continuing with web playlist.")
				}
			}
		}
		//边下载边解密
		err = runv2.Run(track.ID, trackM3u8Url, trackPath, Config)
		if err != nil && shouldRetryWrapper(err) {
			fmt.Println("Decryptor connection dropped; waiting for wrapper to restart...")
			if waitForWrapperReady(Config.DecryptM3u8Port, 5, time.Second) {
				err = runv2.Run(track.ID, trackM3u8Url, trackPath, Config)
			}
		}
		if err != nil {
			fmt.Println("Failed to run v2:", err)
			markAbortRetries(err)
			counter.Error++
			return false
		}
	}

	// Lyrics after audio (reuse from siblings when possible)
	var lrc string
	if Config.EmbedLrc || Config.SaveLrcFile {
		existingPath, ok := findExistingSiblingFile(track.SaveDir, lrcFilename)
		if ok {
			content, err := os.ReadFile(existingPath)
			if err == nil {
				targetPath := filepath.Join(track.SaveDir, lrcFilename)
				if Config.SaveLrcFile && existingPath != targetPath {
					if err := copyFile(existingPath, targetPath); err != nil {
						fmt.Println("Failed to copy lyrics:", err)
					}
				}
				if Config.EmbedLrc {
					lrc = string(content)
				}
			}
		} else {
			lrcStr, err := getLyricsWithFallback(track, token, mediaUserToken)
			if err != nil {
				fmt.Println(err)
			} else {
				if Config.SaveLrcFile {
					err := writeLyrics(track.SaveDir, lrcFilename, lrcStr)
					if err != nil {
						fmt.Printf("Failed to write lyrics")
					}
				}
				if Config.EmbedLrc {
					lrc = lrcStr
				}
			}
		}
	}

	// Embed cover after lyrics to keep order: audio -> lyrics -> covers
	tags := []string{
		"tool=",
		"artist=AppleMusic",
	}
	if Config.EmbedCover {
		if track.CoverPath == "" {
			coverPath, err := ensureCoverFile(track.SaveDir, "cover", track.Resp.Attributes.Artwork.URL)
			if err != nil {
				fmt.Println("Failed to write cover.")
			} else {
				track.CoverPath = coverPath
			}
		}
		if track.CoverPath != "" {
			tags = append(tags, fmt.Sprintf("cover=%s", track.CoverPath))
		}
	}
	tagsString := strings.Join(tags, ":")
	cmd := exec.Command("MP4Box", "-itags", tagsString, trackPath)
	if err := cmd.Run(); err != nil {
		fmt.Printf("Embed failed: %v\n", err)
		counter.Error++
		return false
	}

	track.SavePath = trackPath
	err = writeMP4Tags(track, lrc)
	if err != nil {
		fmt.Println("\u26A0 Failed to write tags in media:", err)
		counter.Unavailable++
		return false
	}

	// CONVERSION FEATURE hook
	convertIfNeeded(track)

	counter.Success++
	okDict[track.PreID] = append(okDict[track.PreID], track.TaskNum)
	emitHistoryEntry(track)
	return true
}

func ripLyricsTrack(track *task.Track, token string, mediaUserToken string) bool {
	if checkStopAndWarn() {
		return false
	}
	counter.Total++
	fmt.Printf("Track %d of %d: %s\n", track.TaskNum, track.TaskTotal, track.Type)

	if !trackSupportsCurrentFormat(track) {
		fmt.Println("Format not available for this track; skipping lyrics.")
		counter.Unavailable++
		return false
	}

	if track.Type == "music-videos" {
		fmt.Println("Skipping music video for lyrics-only.")
		counter.Success++
		return true
	}

	if !Config.SaveLrcFile {
		fmt.Println("save-lrc-file is disabled; nothing to write in lyrics-only mode.")
		counter.Success++
		return true
	}

	songName := buildSongName(track, "")
	fmt.Println(songName)
	lrcFilename := fmt.Sprintf("%s.%s", forbiddenNames.ReplaceAllString(songName, "_"), Config.LrcFormat)
	targetPath := filepath.Join(track.SaveDir, lrcFilename)
	exists, err := fileExists(targetPath)
	if err == nil && exists {
		fmt.Println("Lyrics already exist locally.")
		counter.Success++
		return true
	}

	if existingPath, ok := findExistingSiblingFile(track.SaveDir, lrcFilename); ok {
		if err := copyFile(existingPath, targetPath); err != nil {
			fmt.Println("Failed to copy lyrics:", err)
			counter.Error++
			return false
		}
		fmt.Println("Lyrics copied from sibling format.")
		counter.Success++
		return true
	}

	lrcStr, err := getLyricsWithFallback(track, token, mediaUserToken)
	if err != nil {
		fmt.Println(err)
		counter.Unavailable++
		return false
	}
	if err := writeLyrics(track.SaveDir, lrcFilename, lrcStr); err != nil {
		fmt.Println("Failed to write lyrics.")
		counter.Error++
		return false
	}
	counter.Success++
	return true
}

func ripStation(albumId string, token string, storefront string, mediaUserToken string) error {
	station := task.NewStation(storefront, albumId)
	err := station.GetResp(mediaUserToken, token, Config.Language)
	if err != nil {
		return err
	}
	fmt.Println(" -", station.Type)
	meta := station.Resp

	var Codec string
	if dl_atmos {
		Codec = "ATMOS"
	} else if dl_aac {
		Codec = "AAC"
	} else {
		Codec = "ALAC"
	}
	station.Codec = Codec
	var singerFoldername string
	if Config.ArtistFolderFormat != "" {
		singerFoldername = strings.NewReplacer(
			"{ArtistName}", "Apple Music Station",
			"{ArtistId}", "",
			"{UrlArtistName}", "Apple Music Station",
		).Replace(Config.ArtistFolderFormat)
		if strings.HasSuffix(singerFoldername, ".") {
			singerFoldername = strings.ReplaceAll(singerFoldername, ".", "")
		}
		singerFoldername = strings.TrimSpace(singerFoldername)
		fmt.Println(singerFoldername)
	}
	singerFolder := filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	if dl_atmos {
		singerFolder = filepath.Join(Config.AtmosSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	if dl_aac {
		singerFolder = filepath.Join(Config.AacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	os.MkdirAll(singerFolder, os.ModePerm)
	station.SaveDir = singerFolder

	playlistFolder := strings.NewReplacer(
		"{ArtistName}", "Apple Music Station",
		"{PlaylistName}", LimitString(station.Name),
		"{PlaylistId}", station.ID,
		"{Quality}", "",
		"{Codec}", Codec,
		"{Tag}", "",
	).Replace(Config.PlaylistFolderFormat)
	if strings.HasSuffix(playlistFolder, ".") {
		playlistFolder = strings.ReplaceAll(playlistFolder, ".", "")
	}
	playlistFolder = strings.TrimSpace(playlistFolder)
	playlistFolderPath := filepath.Join(singerFolder, forbiddenNames.ReplaceAllString(playlistFolder, "_"))
	os.MkdirAll(playlistFolderPath, os.ModePerm)
	station.SaveName = playlistFolder
	fmt.Println(playlistFolder)

	if Config.SaveCoverFile || Config.EmbedCover {
		covPath, err := ensureCoverFile(playlistFolderPath, "cover", meta.Data[0].Attributes.Artwork.URL)
		if err != nil {
			fmt.Println("Failed to write cover.")
		}
		station.CoverPath = covPath
	}

	if Config.SaveAnimatedArtwork && dl_atmos && meta.Data[0].Attributes.EditorialVideo.MotionSquare.Video != "" {
		fmt.Println("Found Animation Artwork.")

		motionvideoUrlSquare, err := extractVideo(meta.Data[0].Attributes.EditorialVideo.MotionSquare.Video)
		if err != nil {
			fmt.Println("no motion video square.\n", err)
		} else {
			exists, err := fileExists(filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
			if err != nil {
				fmt.Println("Failed to check if animated artwork square exists.")
			}
			if exists {
				fmt.Println("Animated artwork square already exists locally.")
			} else {
				fmt.Println("Animation Artwork Square Downloading...")
				cmd := exec.Command("ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlSquare, "-c", "copy", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork square dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Square Downloaded")
				}
			}
		}

		if Config.EmbyAnimatedArtwork {
			cmd3 := exec.Command("ffmpeg", "-i", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"), "-vf", "scale=440:-1", "-r", "24", "-f", "gif", filepath.Join(playlistFolderPath, "folder.jpg"))
			if err := cmd3.Run(); err != nil {
				fmt.Printf("animated artwork square to gif err: %v\n", err)
			}
		}
	}
	if station.Type == "stream" {
		counter.Total++
		if isInArray(okDict[station.ID], 1) {
			counter.Success++
			return nil
		}
		songName := strings.NewReplacer(
			"{SongId}", station.ID,
			"{SongNumer}", "01",
			"{SongName}", LimitString(station.Name),
			"{DiscNumber}", "1",
			"{TrackNumber}", "1",
			"{Quality}", "256Kbps",
			"{Tag}", "",
			"{Codec}", "AAC",
		).Replace(Config.SongFileFormat)
		fmt.Println(songName)
		trackPath := filepath.Join(playlistFolderPath, fmt.Sprintf("%s.m4a", forbiddenNames.ReplaceAllString(songName, "_")))
		exists, _ := fileExists(trackPath)
		if exists {
			counter.Success++
			okDict[station.ID] = append(okDict[station.ID], 1)

			fmt.Println("Radio already exists locally.")
			return nil
		}
		assetsUrl, serverUrl, err := ampapi.GetStationAssetsUrlAndServerUrl(station.ID, mediaUserToken, token)
		if err != nil {
			fmt.Println("Failed to get station assets url.", err)
			counter.Error++
			return err
		}
		trackM3U8 := strings.ReplaceAll(assetsUrl, "index.m3u8", "256/prog_index.m3u8")
		keyAndUrls, _ := runv3.Run(station.ID, trackM3U8, token, mediaUserToken, true, serverUrl)
		err = runv3.ExtMvData(keyAndUrls, trackPath)
		if err != nil {
			fmt.Println("Failed to download station stream.", err)
			counter.Error++
			return err
		}
		tags := []string{
			"tool=",
			"disk=1/1",
			"track=1",
			"tracknum=1/1",
			fmt.Sprintf("artist=%s", "Apple Music Station"),
			fmt.Sprintf("performer=%s", "Apple Music Station"),
			fmt.Sprintf("album_artist=%s", "Apple Music Station"),
			fmt.Sprintf("album=%s", station.Name),
			fmt.Sprintf("title=%s", station.Name),
		}
		if Config.EmbedCover {
			tags = append(tags, fmt.Sprintf("cover=%s", station.CoverPath))
		}
		tagsString := strings.Join(tags, ":")
		cmd := exec.Command("MP4Box", "-itags", tagsString, trackPath)
		if err := cmd.Run(); err != nil {
			fmt.Printf("Embed failed: %v\n", err)
		}
		counter.Success++
		okDict[station.ID] = append(okDict[station.ID], 1)
		return nil
	}

	for i := range station.Tracks {
		station.Tracks[i].CoverPath = station.CoverPath
		station.Tracks[i].SaveDir = playlistFolderPath
		station.Tracks[i].Codec = Codec
	}

	trackTotal := len(station.Tracks)
	arr := make([]int, trackTotal)
	for i := 0; i < trackTotal; i++ {
		arr[i] = i + 1
	}
	var selected []int

	if true {
		selected = arr
	}
	for i := range station.Tracks {
		i++
		if isInArray(selected, i) {
			ripTrack(&station.Tracks[i-1], token, mediaUserToken)
		}
	}
	return nil
}

func ripAlbum(albumId string, token string, storefront string, mediaUserToken string, urlArg_i string) error {
	if checkStopAndWarn() {
		return nil
	}
	album := task.NewAlbum(storefront, albumId)
	err := album.GetResp(token, Config.Language)
	if err != nil {
		fmt.Println("Failed to get album response.")
		return err
	}
	meta := album.Resp
	if debug_mode {
		fmt.Println(meta.Data[0].Attributes.ArtistName)
		fmt.Println(meta.Data[0].Attributes.Name)

		for trackNum, track := range meta.Data[0].Relationships.Tracks.Data {
			trackNum++
			fmt.Printf("\nTrack %d of %d:\n", trackNum, len(meta.Data[0].Relationships.Tracks.Data))
			fmt.Printf("%02d. %s\n", trackNum, track.Attributes.Name)

			manifest, err := ampapi.GetSongResp(storefront, track.ID, album.Language, token)
			if err != nil {
				fmt.Printf("Failed to get manifest for track %d: %v\n", trackNum, err)
				continue
			}

			var m3u8Url string
			if manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls != "" {
				m3u8Url = manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls
			}
			needCheck := false
			if Config.GetM3u8Mode == "all" {
				needCheck = true
			} else if Config.GetM3u8Mode == "hires" && contains(track.Attributes.AudioTraits, "hi-res-lossless") {
				needCheck = true
			}
			if needCheck {
				fullM3u8Url, err := checkM3u8(track.ID, "song")
				if err != nil {
					markAbortRetries(err)
				}
				if err == nil && strings.HasSuffix(fullM3u8Url, ".m3u8") {
					m3u8Url = fullM3u8Url
				} else {
					fmt.Println("Failed to get best quality m3u8 from device m3u8 port, will use m3u8 from Web API")
				}
			}

			_, _, err = extractMedia(m3u8Url, true)
			if err != nil {
				fmt.Printf("Failed to extract quality info for track %d: %v\n", trackNum, err)
				continue
			}
		}
		return nil
	}

	codec := "ALAC"
	if dl_atmos {
		codec = "ATMOS"
	} else if dl_aac {
		codec = "AAC"
	}
	album.Codec = codec

	trackTotal := len(meta.Data[0].Relationships.Tracks.Data)
	arr := make([]int, trackTotal)
	for i := 0; i < trackTotal; i++ {
		arr[i] = i + 1
	}

	var selected []int
	if select_tracks != "" {
		selected, err = parseTrackSelection(select_tracks, trackTotal)
		if err != nil {
			fmt.Println("Invalid --select-tracks:", err)
			return err
		}
	} else if !dl_select {
		selected = arr
	} else {
		selected = album.ShowSelect()
	}

	if (dl_covers_only || dl_lyrics_only) && !anySelectedTrackSupportsFormat(album.Tracks, selected) {
		fmt.Println("No selected tracks available for this format; skipping.")
		return nil
	}

	primaryAlbumArtist := primaryArtist(artistNamesFromAlbumData(&meta.Data[0]))
	if primaryAlbumArtist == "" {
		primaryAlbumArtist = meta.Data[0].Attributes.ArtistName
	}
	artistID := ""
	if len(meta.Data[0].Relationships.Artists.Data) > 0 {
		artistID = meta.Data[0].Relationships.Artists.Data[0].ID
	}
	singerFolderName := buildArtistFolderName(primaryAlbumArtist, artistID)
	rootFolder := currentRootFolder()
	singerFolder := rootFolder
	if singerFolderName != "" {
		singerFolder = filepath.Join(rootFolder, forbiddenNames.ReplaceAllString(singerFolderName, "_"))
	}
	os.MkdirAll(singerFolder, os.ModePerm)
	album.SaveDir = singerFolder

	releaseType := detectReleaseType(meta.Data[0].Attributes.Name, meta.Data[0].Attributes.TrackCount, meta.Data[0].Attributes.IsSingle)
	releaseFolder := releaseFolderLabel(releaseType)

	quality, resolvedCodec := resolveAlbumQuality(
		storefront,
		meta.Data[0].Relationships.Tracks.Data[0].ID,
		album.Language,
		token,
		codec,
		meta.Data[0].Relationships.Tracks.Data[0].Attributes.AudioTraits,
	)
	codec = resolvedCodec
	album.Codec = codec

	stringsToJoin := []string{}
	if meta.Data[0].Attributes.IsAppleDigitalMaster || meta.Data[0].Attributes.IsMasteredForItunes {
		if Config.AppleMasterChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.AppleMasterChoice)
		}
	}
	if meta.Data[0].Attributes.ContentRating == "explicit" {
		if Config.ExplicitChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.ExplicitChoice)
		}
	}
	if meta.Data[0].Attributes.ContentRating == "clean" {
		if Config.CleanChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.CleanChoice)
		}
	}
	tagString := strings.Join(stringsToJoin, " ")

	releaseDate := meta.Data[0].Attributes.ReleaseDate
	releaseYear := ""
	if len(releaseDate) >= 4 {
		releaseYear = releaseDate[:4]
	}
	albumFolderName := strings.NewReplacer(
		"{ReleaseDate}", releaseDate,
		"{ReleaseYear}", releaseYear,
		"{ArtistName}", LimitString(primaryAlbumArtist),
		"{AlbumName}", LimitString(meta.Data[0].Attributes.Name),
		"{UPC}", meta.Data[0].Attributes.Upc,
		"{RecordLabel}", meta.Data[0].Attributes.RecordLabel,
		"{Copyright}", meta.Data[0].Attributes.Copyright,
		"{AlbumId}", albumId,
		"{Quality}", quality,
		"{Codec}", codec,
		"{Tag}", tagString,
	).Replace(Config.AlbumFolderFormat)
	albumFolderName = sanitizeFolderName(albumFolderName)
	if dl_atmos && !strings.Contains(strings.ToLower(albumFolderName), "dolby atmos") {
		albumFolderName = fmt.Sprintf("%s (Dolby Atmos)", albumFolderName)
	}
	albumFolderPath := filepath.Join(singerFolder, releaseFolder, forbiddenNames.ReplaceAllString(albumFolderName, "_"))
	os.MkdirAll(albumFolderPath, os.ModePerm)
	album.SaveName = albumFolderName
	fmt.Println(albumFolderName)

	artistCoverURL := ""
	if len(meta.Data[0].Relationships.Artists.Data) > 0 {
		artistCoverURL = meta.Data[0].Relationships.Artists.Data[0].Attributes.Artwork.Url
	}
	squareVideo := meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video
	if squareVideo == "" {
		squareVideo = meta.Data[0].Attributes.EditorialVideo.MotionSquare.Video
	}

	if dl_covers_only {
		handleCoversOnlyAlbum(albumFolderPath, singerFolder, meta.Data[0].Attributes.Artwork.URL, artistCoverURL, squareVideo)
		return nil
	}

	for i := range album.Tracks {
		album.Tracks[i].SaveDir = albumFolderPath
		album.Tracks[i].Codec = codec
	}

	if dl_song {
		if urlArg_i != "" {
			for i := range album.Tracks {
				if urlArg_i == album.Tracks[i].ID {
					if dl_lyrics_only {
						ripLyricsTrack(&album.Tracks[i], token, mediaUserToken)
					} else {
						ripTrack(&album.Tracks[i], token, mediaUserToken)
					}
					return nil
				}
			}
		}
		return nil
	}

	anySuccess := false
	for i := range album.Tracks {
		if checkStopAndWarn() {
			return nil
		}
		index := i + 1
		if isInArray(okDict[albumId], index) {
			counter.Total++
			counter.Success++
			continue
		}
		if !isInArray(selected, index) {
			continue
		}
		var success bool
		if dl_lyrics_only {
			success = ripLyricsTrack(&album.Tracks[i], token, mediaUserToken)
		} else {
			success = ripTrack(&album.Tracks[i], token, mediaUserToken)
		}
		if success {
			anySuccess = true
		}
	}

	if anySuccess && !dl_lyrics_only {
		if Config.SaveCoverFile {
			if _, err := ensureCoverFile(albumFolderPath, "cover", meta.Data[0].Attributes.Artwork.URL); err != nil {
				fmt.Println("Failed to write cover.")
			}
		}
		if Config.SaveArtistCover && artistCoverURL != "" {
			if _, err := ensureCoverFile(singerFolder, "folder", artistCoverURL); err != nil {
				fmt.Println("Failed to write artist cover.")
			}
		}
		if Config.SaveAnimatedArtwork && dl_atmos {
			downloadAnimatedArtworkSquare(albumFolderPath, squareVideo)
		}
	}

	return nil

}
func ripPlaylist(playlistId string, token string, storefront string, mediaUserToken string) error {
	if checkStopAndWarn() {
		return nil
	}
	playlist := task.NewPlaylist(storefront, playlistId)
	err := playlist.GetResp(token, Config.Language)
	if err != nil {
		fmt.Println("Failed to get playlist response.")
		return err
	}
	meta := playlist.Resp
	if debug_mode {
		fmt.Println(meta.Data[0].Attributes.ArtistName)
		fmt.Println(meta.Data[0].Attributes.Name)

		for trackNum, track := range meta.Data[0].Relationships.Tracks.Data {
			trackNum++
			fmt.Printf("\nTrack %d of %d:\n", trackNum, len(meta.Data[0].Relationships.Tracks.Data))
			fmt.Printf("%02d. %s\n", trackNum, track.Attributes.Name)

			manifest, err := ampapi.GetSongResp(storefront, track.ID, playlist.Language, token)
			if err != nil {
				fmt.Printf("Failed to get manifest for track %d: %v\n", trackNum, err)
				continue
			}

			var m3u8Url string
			if manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls != "" {
				m3u8Url = manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls
			}
			needCheck := false
			if Config.GetM3u8Mode == "all" {
				needCheck = true
			} else if Config.GetM3u8Mode == "hires" && contains(track.Attributes.AudioTraits, "hi-res-lossless") {
				needCheck = true
			}
			if needCheck {
				fullM3u8Url, err := checkM3u8(track.ID, "song")
				if err != nil {
					markAbortRetries(err)
				}
				if err == nil && strings.HasSuffix(fullM3u8Url, ".m3u8") {
					m3u8Url = fullM3u8Url
				} else {
					fmt.Println("Failed to get best quality m3u8 from device m3u8 port, will use m3u8 from Web API")
				}
			}

			_, _, err = extractMedia(m3u8Url, true)
			if err != nil {
				fmt.Printf("Failed to extract quality info for track %d: %v\n", trackNum, err)
				continue
			}
		}
		return nil
	}

	codec := "ALAC"
	if dl_atmos {
		codec = "ATMOS"
	} else if dl_aac {
		codec = "AAC"
	}
	playlist.Codec = codec

	trackTotal := len(meta.Data[0].Relationships.Tracks.Data)
	arr := make([]int, trackTotal)
	for i := 0; i < trackTotal; i++ {
		arr[i] = i + 1
	}

	var selected []int
	if select_tracks != "" {
		selected, err = parseTrackSelection(select_tracks, trackTotal)
		if err != nil {
			fmt.Println("Invalid --select-tracks:", err)
			return err
		}
	} else if !dl_select {
		selected = arr
	} else {
		selected = playlist.ShowSelect()
	}

	type albumGroup struct {
		albumID      string
		albumName    string
		artistName   string
		artistID     string
		releaseDate  string
		upc          string
		recordLabel  string
		artistCover  string
		trackCount   int
		isSingle     bool
		audioTraits  []string
		coverURL     string
		sampleTrack  string
		tagString    string
		quality      string
		codec        string
		folderPath   string
		artistFolder string
		trackIndexes []int
		skip         bool
	}

	groups := make(map[string]*albumGroup)
	albumCache := make(map[string]*ampapi.AlbumRespData)
	albumTrackNumbers := make(map[string]map[string][2]int)
	artistCoverCache := make(map[string]string)
	rootFolder := currentRootFolder()

	for idx := range playlist.Tracks {
		order := idx + 1
		if !isInArray(selected, order) {
			continue
		}

		track := &playlist.Tracks[idx]
		albumID := ""
		albumName := track.Resp.Attributes.AlbumName
		releaseDate := ""
		upc := ""
		recordLabel := ""
		artistCover := ""
		trackCount := 0
		isSingle := false
		audioTraits := track.Resp.Attributes.AudioTraits

		if len(track.Resp.Relationships.Albums.Data) > 0 {
			albumRel := track.Resp.Relationships.Albums.Data[0]
			albumID = albumRel.ID
			if albumRel.Attributes.Name != "" {
				albumName = albumRel.Attributes.Name
			}
			releaseDate = albumRel.Attributes.ReleaseDate
			trackCount = albumRel.Attributes.TrackCount
			isSingle = albumRel.Attributes.IsSingle
			upc = albumRel.Attributes.Upc
		}

		if albumID == "" {
			albumID = fmt.Sprintf("unknown-%s", albumName)
		}

		artistID := ""
		if len(track.Resp.Relationships.Artists.Data) > 0 {
			artistID = track.Resp.Relationships.Artists.Data[0].ID
		}

		var albumData *ampapi.AlbumRespData
		shouldFetchAlbum := !strings.HasPrefix(albumID, "unknown-")
		if shouldFetchAlbum {
			if cached, ok := albumCache[albumID]; ok {
				albumData = cached
			} else {
				resp, err := ampapi.GetAlbumResp(storefront, albumID, playlist.Language, token)
				if err != nil {
					fmt.Println("Failed to fetch album data for playlist item:", err)
				} else if len(resp.Data) > 0 {
					albumData = &resp.Data[0]
					albumCache[albumID] = albumData
				}
			}
		}

		if albumData != nil {
			if albumData.Attributes.Name != "" {
				albumName = albumData.Attributes.Name
			}
			if albumData.Attributes.ReleaseDate != "" {
				releaseDate = albumData.Attributes.ReleaseDate
			}
			if albumData.Attributes.Upc != "" {
				upc = albumData.Attributes.Upc
			}
			if albumData.Attributes.RecordLabel != "" {
				recordLabel = albumData.Attributes.RecordLabel
			}
			if albumData.Attributes.TrackCount > 0 {
				trackCount = albumData.Attributes.TrackCount
			}
			isSingle = albumData.Attributes.IsSingle
			if len(albumData.Relationships.Artists.Data) > 0 {
				artistCover = albumData.Relationships.Artists.Data[0].Attributes.Artwork.Url
			}
			if len(albumData.Relationships.Artists.Data) > 0 {
				artistID = albumData.Relationships.Artists.Data[0].ID
			}
			track.AlbumData = *albumData
			if len(albumData.Relationships.Tracks.Data) > 0 {
				track.DiscTotal = albumData.Relationships.Tracks.Data[len(albumData.Relationships.Tracks.Data)-1].Attributes.DiscNumber
			}
			if _, ok := albumTrackNumbers[albumID]; !ok {
				trackMap := make(map[string][2]int)
				for _, albumTrack := range albumData.Relationships.Tracks.Data {
					trackMap[albumTrack.ID] = [2]int{
						albumTrack.Attributes.TrackNumber,
						albumTrack.Attributes.DiscNumber,
					}
				}
				albumTrackNumbers[albumID] = trackMap
			}
		}
		if releaseDate == "" && track.Resp.Attributes.ReleaseDate != "" {
			releaseDate = track.Resp.Attributes.ReleaseDate
		}
		if artistCover == "" && artistID != "" {
			if cached, ok := artistCoverCache[artistID]; ok {
				artistCover = cached
			} else {
				resp, err := ampapi.GetArtistResp(storefront, artistID, playlist.Language, token)
				if err == nil && len(resp.Data) > 0 {
					artistCover = resp.Data[0].Attributes.Artwork.Url
				}
				artistCoverCache[artistID] = artistCover
			}
		}

		primaryAlbumArtist := ""
		if albumData != nil {
			primaryAlbumArtist = primaryArtist(artistNamesFromAlbumData(albumData))
		}
		if primaryAlbumArtist == "" {
			primaryAlbumArtist = primaryArtist(artistNamesFromTrack(track))
		}
		if primaryAlbumArtist == "" {
			primaryAlbumArtist = track.Resp.Attributes.ArtistName
		}
		if trackMap, ok := albumTrackNumbers[albumID]; ok {
			if nums, ok := trackMap[track.ID]; ok {
				if nums[0] > 0 {
					track.Resp.Attributes.TrackNumber = nums[0]
				}
				if nums[1] > 0 {
					track.Resp.Attributes.DiscNumber = nums[1]
				}
			}
		}

		stringsToJoin := []string{}
		if track.Resp.Attributes.IsAppleDigitalMaster {
			if Config.AppleMasterChoice != "" {
				stringsToJoin = append(stringsToJoin, Config.AppleMasterChoice)
			}
		}
		if track.Resp.Attributes.ContentRating == "explicit" {
			if Config.ExplicitChoice != "" {
				stringsToJoin = append(stringsToJoin, Config.ExplicitChoice)
			}
		}
		if track.Resp.Attributes.ContentRating == "clean" {
			if Config.CleanChoice != "" {
				stringsToJoin = append(stringsToJoin, Config.CleanChoice)
			}
		}
		tagString := strings.Join(stringsToJoin, " ")

		group, ok := groups[albumID]
		if !ok {
			group = &albumGroup{
				albumID:      albumID,
				albumName:    albumName,
				artistName:   primaryAlbumArtist,
				artistID:     artistID,
				releaseDate:  releaseDate,
				upc:          upc,
				recordLabel:  recordLabel,
				artistCover:  artistCover,
				trackCount:   trackCount,
				isSingle:     isSingle,
				audioTraits:  audioTraits,
				coverURL:     track.Resp.Attributes.Artwork.URL,
				sampleTrack:  track.ID,
				tagString:    tagString,
				codec:        codec,
				trackIndexes: []int{idx},
			}
			groups[albumID] = group
		} else {
			group.trackIndexes = append(group.trackIndexes, idx)
			if group.albumName == "" && albumName != "" {
				group.albumName = albumName
			}
			if group.artistName == "" && primaryAlbumArtist != "" {
				group.artistName = primaryAlbumArtist
			}
			if group.releaseDate == "" && releaseDate != "" {
				group.releaseDate = releaseDate
			}
			if group.upc == "" && upc != "" {
				group.upc = upc
			}
			if group.recordLabel == "" && recordLabel != "" {
				group.recordLabel = recordLabel
			}
			if group.artistCover == "" && artistCover != "" {
				group.artistCover = artistCover
			}
			if group.trackCount == 0 && trackCount > 0 {
				group.trackCount = trackCount
			}
			if group.artistID == "" && artistID != "" {
				group.artistID = artistID
			}
		}
	}

	for _, group := range groups {
		if dl_atmos || dl_covers_only || dl_lyrics_only {
			hasSupported := false
			for _, idx := range group.trackIndexes {
				if idx >= 0 && idx < len(playlist.Tracks) {
					if trackSupportsCurrentFormat(&playlist.Tracks[idx]) {
						hasSupported = true
						break
					}
				}
			}
			if !hasSupported {
				fmt.Printf("No selected tracks available for this format; skipping %s.\n", group.albumName)
				group.skip = true
				continue
			}
		}

		artistFolderName := buildArtistFolderName(group.artistName, group.artistID)
		artistFolder := rootFolder
		if artistFolderName != "" {
			artistFolder = filepath.Join(rootFolder, forbiddenNames.ReplaceAllString(artistFolderName, "_"))
		}
		group.artistFolder = artistFolder
		releaseType := detectReleaseType(group.albumName, group.trackCount, group.isSingle)
		releaseFolder := releaseFolderLabel(releaseType)

		quality, resolvedCodec := resolveAlbumQuality(
			storefront,
			group.sampleTrack,
			playlist.Language,
			token,
			group.codec,
			group.audioTraits,
		)
		group.codec = resolvedCodec
		group.quality = quality

		releaseYear := ""
		if len(group.releaseDate) >= 4 {
			releaseYear = group.releaseDate[:4]
		}
		albumFolderName := strings.NewReplacer(
			"{ReleaseDate}", group.releaseDate,
			"{ReleaseYear}", releaseYear,
			"{ArtistName}", LimitString(group.artistName),
			"{AlbumName}", LimitString(group.albumName),
			"{UPC}", group.upc,
			"{RecordLabel}", group.recordLabel,
			"{Copyright}", "",
			"{AlbumId}", group.albumID,
			"{Quality}", group.quality,
			"{Codec}", group.codec,
			"{Tag}", group.tagString,
		).Replace(Config.AlbumFolderFormat)
		albumFolderName = sanitizeFolderName(albumFolderName)
		if dl_atmos && !strings.Contains(strings.ToLower(albumFolderName), "dolby atmos") {
			albumFolderName = fmt.Sprintf("%s (Dolby Atmos)", albumFolderName)
		}
		group.folderPath = filepath.Join(artistFolder, releaseFolder, forbiddenNames.ReplaceAllString(albumFolderName, "_"))
		os.MkdirAll(group.folderPath, os.ModePerm)

		if Config.SaveCoverFile && !dl_covers_only {
			if _, err := ensureCoverFile(group.folderPath, "cover", group.coverURL); err != nil {
				fmt.Println("Failed to write cover.")
			}
		}

		if dl_covers_only {
			handleCoversOnlyAlbum(group.folderPath, artistFolder, group.coverURL, group.artistCover, "")
		}
	}

	if dl_covers_only {
		return nil
	}

	groupSuccess := make(map[string]bool)

	for idx := range playlist.Tracks {
		if checkStopAndWarn() {
			return nil
		}
		order := idx + 1
		if !isInArray(selected, order) {
			continue
		}
		track := &playlist.Tracks[idx]
		albumID := ""
		if len(track.Resp.Relationships.Albums.Data) > 0 {
			albumID = track.Resp.Relationships.Albums.Data[0].ID
		}
		if albumID == "" {
			albumID = fmt.Sprintf("unknown-%s", track.Resp.Attributes.AlbumName)
		}
		group, ok := groups[albumID]
		if ok {
			if group.skip {
				continue
			}
			track.SaveDir = group.folderPath
			track.Codec = group.codec
		} else {
			if dl_covers_only || dl_lyrics_only {
				continue
			}
			track.SaveDir = rootFolder
			track.Codec = codec
		}

		if isInArray(okDict[playlistId], order) {
			counter.Total++
			counter.Success++
			continue
		}

		if dl_lyrics_only {
			if ripLyricsTrack(track, token, mediaUserToken) {
				groupSuccess[albumID] = true
			}
		} else {
			if ripTrack(track, token, mediaUserToken) {
				groupSuccess[albumID] = true
			}
		}
	}

	if !dl_lyrics_only && Config.SaveArtistCover {
		for albumID, success := range groupSuccess {
			if !success {
				continue
			}
			group, ok := groups[albumID]
			if !ok || group.artistCover == "" || group.artistFolder == "" {
				continue
			}
			if _, err := ensureCoverFile(group.artistFolder, "folder", group.artistCover); err != nil {
				fmt.Println("Failed to write artist cover.")
			}
		}
	}
	return nil
}

func writeMP4Tags(track *task.Track, lrc string) error {
	trackArtistList := formatArtistList(artistNamesFromTrack(track))
	if trackArtistList == "" {
		trackArtistList = track.Resp.Attributes.ArtistName
	}
	albumArtistName := primaryArtist(albumArtistNamesFromTrack(track))
	if albumArtistName == "" {
		albumArtistName = track.Resp.Attributes.ArtistName
	}
	trackNumber := track.Resp.Attributes.TrackNumber
	if trackNumber == 0 {
		trackNumber = track.TaskNum
	}

	t := &mp4tag.MP4Tags{
		Title:      track.Resp.Attributes.Name,
		TitleSort:  track.Resp.Attributes.Name,
		Artist:     trackArtistList,
		ArtistSort: trackArtistList,
		Custom: map[string]string{
			"PERFORMER":   trackArtistList,
			"RELEASETIME": track.Resp.Attributes.ReleaseDate,
			"ISRC":        track.Resp.Attributes.Isrc,
			"LABEL":       "",
			"UPC":         "",
		},
		Composer:     track.Resp.Attributes.ComposerName,
		ComposerSort: track.Resp.Attributes.ComposerName,
		CustomGenre:  track.Resp.Attributes.GenreNames[0],
		Lyrics:       lrc,
		TrackNumber:  int16(trackNumber),
		DiscNumber:   int16(track.Resp.Attributes.DiscNumber),
		Album:        track.Resp.Attributes.AlbumName,
		AlbumSort:    track.Resp.Attributes.AlbumName,
	}
	if strings.EqualFold(track.Codec, "ATMOS") {
		t.Custom["ALBUMVERSION"] = "Dolby Atmos"
	}

	if track.PreType == "albums" {
		albumID, err := strconv.ParseUint(track.PreID, 10, 32)
		if err != nil {
			return err
		}
		t.ItunesAlbumID = int32(albumID)
	}

	if len(track.Resp.Relationships.Artists.Data) > 0 {
		artistID, err := strconv.ParseUint(track.Resp.Relationships.Artists.Data[0].ID, 10, 32)
		if err != nil {
			return err
		}
		t.ItunesArtistID = int32(artistID)
	}

	if (track.PreType == "playlists" || track.PreType == "stations") && !Config.UseSongInfoForPlaylist {
		t.DiscNumber = 1
		t.DiscTotal = 1
		t.TrackNumber = int16(trackNumber)
		t.TrackTotal = int16(track.TaskTotal)
		t.Album = track.PlaylistData.Attributes.Name
		t.AlbumSort = track.PlaylistData.Attributes.Name
		t.AlbumArtist = albumArtistName
		t.AlbumArtistSort = albumArtistName
	} else if (track.PreType == "playlists" || track.PreType == "stations") && Config.UseSongInfoForPlaylist {
		t.DiscTotal = int16(track.DiscTotal)
		t.TrackTotal = int16(track.AlbumData.Attributes.TrackCount)
		t.AlbumArtist = albumArtistName
		t.AlbumArtistSort = albumArtistName
		t.Custom["UPC"] = track.AlbumData.Attributes.Upc
		t.Custom["LABEL"] = track.AlbumData.Attributes.RecordLabel
		t.Date = track.AlbumData.Attributes.ReleaseDate
		t.Copyright = track.AlbumData.Attributes.Copyright
		t.Publisher = track.AlbumData.Attributes.RecordLabel
	} else {
		t.DiscTotal = int16(track.DiscTotal)
		t.TrackTotal = int16(track.AlbumData.Attributes.TrackCount)
		t.AlbumArtist = albumArtistName
		t.AlbumArtistSort = albumArtistName
		t.Custom["UPC"] = track.AlbumData.Attributes.Upc
		t.Date = track.AlbumData.Attributes.ReleaseDate
		t.Copyright = track.AlbumData.Attributes.Copyright
		t.Publisher = track.AlbumData.Attributes.RecordLabel
	}

	if track.Resp.Attributes.ContentRating == "explicit" {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryExplicit
	} else if track.Resp.Attributes.ContentRating == "clean" {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryClean
	} else {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryNone
	}

	mp4, err := mp4tag.Open(track.SavePath)
	if err != nil {
		return err
	}
	defer mp4.Close()
	err = mp4.Write(t, []string{})
	if err != nil {
		return err
	}
	return nil
}

func main() {
	err := loadConfig()
	if err != nil {
		fmt.Printf("load Config failed: %v", err)
		return
	}
	token, err := ampapi.GetToken()
	if err != nil {
		if Config.AuthorizationToken != "" && Config.AuthorizationToken != "your-authorization-token" {
			token = strings.Replace(Config.AuthorizationToken, "Bearer ", "", -1)
		} else {
			fmt.Println("Failed to get token.")
			return
		}
	}
	var search_type string
	pflag.StringVar(&search_type, "search", "", "Search for 'album', 'song', or 'artist'. Provide query after flags.")
	pflag.BoolVar(&dl_preview, "preview", false, "Output JSON preview metadata and exit")
	pflag.BoolVar(&dl_atmos, "atmos", false, "Enable atmos download mode")
	pflag.BoolVar(&dl_aac, "aac", false, "Enable adm-aac download mode")
	pflag.BoolVar(&dl_select, "select", false, "Enable selective download")
	pflag.StringVar(&select_tracks, "select-tracks", "", "Select tracks by list/range (e.g., 1,2,5-7)")
	pflag.BoolVar(&dl_song, "song", false, "Enable single song download mode")
	pflag.BoolVar(&dl_lyrics_only, "lyrics-only", false, "Download lyrics only (no audio)")
	pflag.BoolVar(&dl_covers_only, "covers-only", false, "Download covers only (no audio)")
	pflag.BoolVar(&artist_select, "all-album", false, "Download all artist albums")
	pflag.BoolVar(&debug_mode, "debug", false, "Enable debug mode to show audio quality information")
	alac_max = pflag.Int("alac-max", Config.AlacMax, "Specify the max quality for download alac")
	atmos_max = pflag.Int("atmos-max", Config.AtmosMax, "Specify the max quality for download atmos")
	aac_type = pflag.String("aac-type", Config.AacType, "Select AAC type, aac aac-binaural aac-downmix")
	mv_audio_type = pflag.String("mv-audio-type", Config.MVAudioType, "Select MV audio type, atmos ac3 aac")
	mv_max = pflag.Int("mv-max", Config.MVMax, "Specify the max quality for download MV")

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [url1 url2 ...]\n", "[main | main.exe | go run main.go]")
		fmt.Fprintf(os.Stderr, "Search Usage: %s --search [album|song|artist] [query]\n", "[main | main.exe | go run main.go]")
		fmt.Println("\nOptions:")
		pflag.PrintDefaults()
	}

	pflag.Parse()
	Config.AlacMax = *alac_max
	Config.AtmosMax = *atmos_max
	Config.AacType = *aac_type
	Config.MVAudioType = *mv_audio_type
	Config.MVMax = *mv_max
	clearStopSignal()

	if dl_lyrics_only && dl_covers_only {
		fmt.Println("Error: --lyrics-only and --covers-only cannot be used together.")
		return
	}
	if select_tracks != "" {
		dl_select = true
	}
	if !isInteractive() && dl_select && select_tracks == "" {
		fmt.Println("Error: selective downloads require --select-tracks when running non-interactively.")
		return
	}

	args := pflag.Args()

	if search_type != "" {
		if len(args) == 0 {
			fmt.Println("Error: --search flag requires a query.")
			pflag.Usage()
			return
		}
		selectedUrl, err := handleSearch(search_type, args, token)
		if err != nil {
			fmt.Printf("\nSearch process failed: %v\n", err)
			return
		}
		if selectedUrl == "" {
			fmt.Println("\nExiting.")
			return
		}
		os.Args = []string{selectedUrl}
	} else {
		if len(args) == 0 {
			fmt.Println("No URLs provided. Please provide at least one URL.")
			pflag.Usage()
			return
		}
		os.Args = args
	}

	if dl_preview {
		if len(os.Args) == 0 {
			fmt.Println("No URL provided for preview.")
			return
		}
		preview, err := buildPreviewPayload(os.Args[0], token)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Preview failed: %v\n", err)
			os.Exit(1)
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(preview); err != nil {
			fmt.Fprintf(os.Stderr, "Preview output failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if strings.Contains(os.Args[0], "/artist/") {
		urlArtistName, urlArtistID, err := getUrlArtistName(os.Args[0], token)
		if err != nil {
			fmt.Println("Failed to get artistname.")
			return
		}
		Config.ArtistFolderFormat = strings.NewReplacer(
			"{UrlArtistName}", LimitString(urlArtistName),
			"{ArtistId}", urlArtistID,
		).Replace(Config.ArtistFolderFormat)
		albumArgs, err := checkArtist(os.Args[0], token, "albums")
		if err != nil {
			fmt.Println("Failed to get artist albums.")
			return
		}
		mvArgs, err := checkArtist(os.Args[0], token, "music-videos")
		if err != nil {
			fmt.Println("Failed to get artist music-videos.")
		}
		os.Args = append(albumArgs, mvArgs...)
	}
	albumTotal := len(os.Args)
	for {
		for albumNum, urlRaw := range os.Args {
			if checkStopAndWarn() {
				return
			}
			fmt.Printf("Queue %d of %d: ", albumNum+1, albumTotal)
			var storefront, albumId string

			if strings.Contains(urlRaw, "/music-video/") {
				fmt.Println("Music Video")
				if dl_lyrics_only || dl_covers_only {
					fmt.Println("Skipping music videos in lyrics/covers-only mode.")
					continue
				}
				if debug_mode {
					continue
				}
				counter.Total++
				if len(Config.MediaUserToken) <= 50 {
					fmt.Println(": meida-user-token is not set, skip MV dl")
					counter.Success++
					continue
				}
				if _, err := exec.LookPath("mp4decrypt"); err != nil {
					fmt.Println(": mp4decrypt is not found, skip MV dl")
					counter.Success++
					continue
				}
				mvSaveDir := strings.NewReplacer(
					"{ArtistName}", "",
					"{UrlArtistName}", "",
					"{ArtistId}", "",
				).Replace(Config.ArtistFolderFormat)
				if mvSaveDir != "" {
					mvSaveDir = filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(mvSaveDir, "_"))
				} else {
					mvSaveDir = Config.AlacSaveFolder
				}
				storefront, albumId = checkUrlMv(urlRaw)
				err := mvDownloader(albumId, mvSaveDir, token, storefront, Config.MediaUserToken, nil)
				if err != nil {
					fmt.Println("\u26A0 Failed to dl MV:", err)
					counter.Error++
					continue
				}
				counter.Success++
				continue
			}
			if strings.Contains(urlRaw, "/song/") {
				fmt.Printf("Song->")
				storefront, songId := checkUrlSong(urlRaw)
				if storefront == "" || songId == "" {
					fmt.Println("Invalid song URL format.")
					continue
				}
				err := ripSong(songId, token, storefront, Config.MediaUserToken)
				if err != nil {
					fmt.Println("Failed to rip song:", err)
				}
				continue
			}
			parse, err := url.Parse(urlRaw)
			if err != nil {
				log.Fatalf("Invalid URL: %v", err)
			}
			var urlArg_i = parse.Query().Get("i")

			if strings.Contains(urlRaw, "/album/") {
				fmt.Println("Album")
				storefront, albumId = checkUrl(urlRaw)
				err := ripAlbum(albumId, token, storefront, Config.MediaUserToken, urlArg_i)
				if err != nil {
					fmt.Println("Failed to rip album:", err)
				}
			} else if strings.Contains(urlRaw, "/playlist/") {
				fmt.Println("Playlist")
				storefront, albumId = checkUrlPlaylist(urlRaw)
				err := ripPlaylist(albumId, token, storefront, Config.MediaUserToken)
				if err != nil {
					fmt.Println("Failed to rip playlist:", err)
				}
			} else if strings.Contains(urlRaw, "/station/") {
				fmt.Printf("Station")
				if dl_lyrics_only || dl_covers_only {
					fmt.Println(": skipping stations in lyrics/covers-only mode")
					continue
				}
				storefront, albumId = checkUrlStation(urlRaw)
				if len(Config.MediaUserToken) <= 50 {
					fmt.Println(": meida-user-token is not set, skip station dl")
					continue
				}
				err := ripStation(albumId, token, storefront, Config.MediaUserToken)
				if err != nil {
					fmt.Println("Failed to rip station:", err)
				}
			} else {
				fmt.Println("Invalid type")
			}
		}
		fmt.Printf("=======  [\u2714 ] Completed: %d/%d  |  [\u26A0 ] Warnings: %d  |  [\u2716 ] Errors: %d  =======\n", counter.Success, counter.Total, counter.Unavailable+counter.NotSong, counter.Error)
		if counter.Error == 0 {
			break
		}
		if !isInteractive() || abortRetries {
			fmt.Println("Error detected; aborting retries in non-interactive mode.")
			break
		}
		fmt.Println("Error detected, press Enter to try again...")
		fmt.Scanln()
		fmt.Println("Start trying again...")
		counter = structs.Counter{}
	}
}

func mvDownloader(adamID string, saveDir string, token string, storefront string, mediaUserToken string, track *task.Track) error {
	MVInfo, err := ampapi.GetMusicVideoResp(storefront, adamID, Config.Language, token)
	if err != nil {
		fmt.Println("\u26A0 Failed to get MV manifest:", err)
		return nil
	}

	if strings.HasSuffix(saveDir, ".") {
		saveDir = strings.ReplaceAll(saveDir, ".", "")
	}
	saveDir = strings.TrimSpace(saveDir)

	vidPath := filepath.Join(saveDir, fmt.Sprintf("%s_vid.mp4", adamID))
	audPath := filepath.Join(saveDir, fmt.Sprintf("%s_aud.mp4", adamID))
	mvSaveName := fmt.Sprintf("%s (%s)", MVInfo.Data[0].Attributes.Name, adamID)
	if track != nil {
		mvSaveName = fmt.Sprintf("%02d. %s", track.TaskNum, MVInfo.Data[0].Attributes.Name)
	}

	mvOutPath := filepath.Join(saveDir, fmt.Sprintf("%s.mp4", forbiddenNames.ReplaceAllString(mvSaveName, "_")))

	fmt.Println(MVInfo.Data[0].Attributes.Name)

	exists, _ := fileExists(mvOutPath)
	if exists {
		fmt.Println("MV already exists locally.")
		return nil
	}

	mvm3u8url, _, _, _ := runv3.GetWebplayback(adamID, token, mediaUserToken, true)
	if mvm3u8url == "" {
		return errors.New("media-user-token may wrong or expired")
	}

	os.MkdirAll(saveDir, os.ModePerm)
	videom3u8url, _ := extractVideo(mvm3u8url)
	videokeyAndUrls, _ := runv3.Run(adamID, videom3u8url, token, mediaUserToken, true, "")
	_ = runv3.ExtMvData(videokeyAndUrls, vidPath)
	defer os.Remove(vidPath)
	audiom3u8url, _ := extractMvAudio(mvm3u8url)
	audiokeyAndUrls, _ := runv3.Run(adamID, audiom3u8url, token, mediaUserToken, true, "")
	_ = runv3.ExtMvData(audiokeyAndUrls, audPath)
	defer os.Remove(audPath)

	tags := []string{
		"tool=",
		fmt.Sprintf("artist=%s", func() string {
			list := formatArtistList(artistNamesFromMusicVideoData(&MVInfo.Data[0]))
			if list == "" {
				return MVInfo.Data[0].Attributes.ArtistName
			}
			return list
		}()),
		fmt.Sprintf("title=%s", MVInfo.Data[0].Attributes.Name),
		fmt.Sprintf("genre=%s", MVInfo.Data[0].Attributes.GenreNames[0]),
		fmt.Sprintf("created=%s", MVInfo.Data[0].Attributes.ReleaseDate),
		fmt.Sprintf("ISRC=%s", MVInfo.Data[0].Attributes.Isrc),
	}
	mvPrimaryArtist := primaryArtist(artistNamesFromMusicVideoData(&MVInfo.Data[0]))
	if mvPrimaryArtist == "" {
		mvPrimaryArtist = MVInfo.Data[0].Attributes.ArtistName
	}

	if MVInfo.Data[0].Attributes.ContentRating == "explicit" {
		tags = append(tags, "rating=1")
	} else if MVInfo.Data[0].Attributes.ContentRating == "clean" {
		tags = append(tags, "rating=2")
	} else {
		tags = append(tags, "rating=0")
	}

	if track != nil {
		if track.PreType == "playlists" && !Config.UseSongInfoForPlaylist {
			tags = append(tags, "disk=1/1")
			tags = append(tags, fmt.Sprintf("album=%s", track.PlaylistData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("track=%d", track.TaskNum))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.TaskNum, track.TaskTotal))
			tags = append(tags, fmt.Sprintf("album_artist=%s", mvPrimaryArtist))
			tags = append(tags, fmt.Sprintf("performer=%s", mvPrimaryArtist))
		} else if track.PreType == "playlists" && Config.UseSongInfoForPlaylist {
			tags = append(tags, fmt.Sprintf("album=%s", track.AlbumData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("disk=%d/%d", track.Resp.Attributes.DiscNumber, track.DiscTotal))
			tags = append(tags, fmt.Sprintf("track=%d", track.Resp.Attributes.TrackNumber))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.Resp.Attributes.TrackNumber, track.AlbumData.Attributes.TrackCount))
			tags = append(tags, fmt.Sprintf("album_artist=%s", mvPrimaryArtist))
			tags = append(tags, fmt.Sprintf("performer=%s", mvPrimaryArtist))
			tags = append(tags, fmt.Sprintf("copyright=%s", track.AlbumData.Attributes.Copyright))
			tags = append(tags, fmt.Sprintf("UPC=%s", track.AlbumData.Attributes.Upc))
		} else {
			tags = append(tags, fmt.Sprintf("album=%s", track.AlbumData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("disk=%d/%d", track.Resp.Attributes.DiscNumber, track.DiscTotal))
			tags = append(tags, fmt.Sprintf("track=%d", track.Resp.Attributes.TrackNumber))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.Resp.Attributes.TrackNumber, track.AlbumData.Attributes.TrackCount))
			tags = append(tags, fmt.Sprintf("album_artist=%s", mvPrimaryArtist))
			tags = append(tags, fmt.Sprintf("performer=%s", mvPrimaryArtist))
			tags = append(tags, fmt.Sprintf("copyright=%s", track.AlbumData.Attributes.Copyright))
			tags = append(tags, fmt.Sprintf("UPC=%s", track.AlbumData.Attributes.Upc))
		}
	} else {
		tags = append(tags, fmt.Sprintf("album=%s", MVInfo.Data[0].Attributes.AlbumName))
		tags = append(tags, fmt.Sprintf("disk=%d", MVInfo.Data[0].Attributes.DiscNumber))
		tags = append(tags, fmt.Sprintf("track=%d", MVInfo.Data[0].Attributes.TrackNumber))
		tags = append(tags, fmt.Sprintf("tracknum=%d", MVInfo.Data[0].Attributes.TrackNumber))
		tags = append(tags, fmt.Sprintf("performer=%s", mvPrimaryArtist))
	}

	var covPath string
	if true {
		thumbURL := MVInfo.Data[0].Attributes.Artwork.URL
		baseThumbName := forbiddenNames.ReplaceAllString(mvSaveName, "_") + "_thumbnail"
		covPath, err = writeCover(saveDir, baseThumbName, thumbURL)
		if err != nil {
			fmt.Println("Failed to save MV thumbnail:", err)
		} else {
			tags = append(tags, fmt.Sprintf("cover=%s", covPath))
		}
	}
	defer os.Remove(covPath)

	tagsString := strings.Join(tags, ":")
	muxCmd := exec.Command("MP4Box", "-itags", tagsString, "-quiet", "-add", vidPath, "-add", audPath, "-keep-utc", "-new", mvOutPath)
	fmt.Printf("MV Remuxing...")
	if err := muxCmd.Run(); err != nil {
		fmt.Printf("MV mux failed: %v\n", err)
		return err
	}
	fmt.Printf("\rMV Remuxed.   \n")
	return nil
}

func extractMvAudio(c string) (string, error) {
	MediaUrl, err := url.Parse(c)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(c)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	audioString := string(body)
	from, listType, err := m3u8.DecodeFrom(strings.NewReader(audioString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", errors.New("m3u8 not of media type")
	}

	audio := from.(*m3u8.MasterPlaylist)

	var audioPriority = []string{"audio-atmos", "audio-ac3", "audio-stereo-256"}
	if Config.MVAudioType == "ac3" {
		audioPriority = []string{"audio-ac3", "audio-stereo-256"}
	} else if Config.MVAudioType == "aac" {
		audioPriority = []string{"audio-stereo-256"}
	}

	re := regexp.MustCompile(`_gr(\d+)_`)

	type AudioStream struct {
		URL     string
		Rank    int
		GroupID string
	}
	var audioStreams []AudioStream

	for _, variant := range audio.Variants {
		for _, audiov := range variant.Alternatives {
			if audiov.URI != "" {
				for _, priority := range audioPriority {
					if audiov.GroupId == priority {
						matches := re.FindStringSubmatch(audiov.URI)
						if len(matches) == 2 {
							var rank int
							fmt.Sscanf(matches[1], "%d", &rank)
							streamUrl, _ := MediaUrl.Parse(audiov.URI)
							audioStreams = append(audioStreams, AudioStream{
								URL:     streamUrl.String(),
								Rank:    rank,
								GroupID: audiov.GroupId,
							})
						}
					}
				}
			}
		}
	}

	if len(audioStreams) == 0 {
		return "", errors.New("no suitable audio stream found")
	}

	sort.Slice(audioStreams, func(i, j int) bool {
		return audioStreams[i].Rank > audioStreams[j].Rank
	})
	fmt.Println("Audio: " + audioStreams[0].GroupID)
	return audioStreams[0].URL, nil
}

func checkM3u8(b string, f string) (string, error) {
	var EnhancedHls string
	if Config.GetM3u8FromDevice {
		adamID := b
		conn, err := net.Dial("tcp", Config.GetM3u8Port)
		if err != nil {
			fmt.Println("Error connecting to device:", err)
			markAbortRetries(err)
			return "none", err
		}
		defer conn.Close()
		if f == "song" {
			fmt.Println("Connected to device")
		}

		adamIDBuffer := []byte(adamID)
		lengthBuffer := []byte{byte(len(adamIDBuffer))}

		_, err = conn.Write(lengthBuffer)
		if err != nil {
			fmt.Println("Error writing length to device:", err)
			return "none", err
		}

		_, err = conn.Write(adamIDBuffer)
		if err != nil {
			fmt.Println("Error writing adamID to device:", err)
			return "none", err
		}

		response, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			fmt.Println("Error reading response from device:", err)
			return "none", err
		}

		response = bytes.TrimSpace(response)
		if len(response) > 0 {
			if f == "song" {
				fmt.Println("Received URL:", string(response))
			}
			EnhancedHls = string(response)
		} else {
			fmt.Println("Received an empty response")
		}
	}
	return EnhancedHls, nil
}

func formatAvailability(available bool, quality string) string {
	if !available {
		return "Not Available"
	}
	return quality
}

func extractMedia(b string, more_mode bool) (string, string, error) {
	masterUrl, err := url.Parse(b)
	if err != nil {
		return "", "", err
	}
	resp, err := http.Get(b)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", errors.New(resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	masterString := string(body)
	from, listType, err := m3u8.DecodeFrom(strings.NewReader(masterString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", "", errors.New("m3u8 not of master type")
	}
	master := from.(*m3u8.MasterPlaylist)
	var streamUrl *url.URL
	sort.Slice(master.Variants, func(i, j int) bool {
		return master.Variants[i].AverageBandwidth > master.Variants[j].AverageBandwidth
	})
	if debug_mode && more_mode {
		fmt.Println("\nDebug: All Available Variants:")
		var data [][]string
		for _, variant := range master.Variants {
			data = append(data, []string{variant.Codecs, variant.Audio, fmt.Sprint(variant.Bandwidth)})
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Codec", "Audio", "Bandwidth"})
		table.SetAutoMergeCells(true)
		table.SetRowLine(true)
		table.AppendBulk(data)
		table.Render()

		var hasAAC, hasLossless, hasHiRes, hasAtmos, hasDolbyAudio bool
		var aacQuality, losslessQuality, hiResQuality, atmosQuality, dolbyAudioQuality string

		for _, variant := range master.Variants {
			if variant.Codecs == "mp4a.40.2" { // AAC
				hasAAC = true
				split := strings.Split(variant.Audio, "-")
				if len(split) >= 3 {
					bitrate, _ := strconv.Atoi(split[2])
					currentBitrate := 0
					if aacQuality != "" {
						current := strings.Split(aacQuality, " | ")[2]
						current = strings.Split(current, " ")[0]
						currentBitrate, _ = strconv.Atoi(current)
					}
					if bitrate > currentBitrate {
						aacQuality = fmt.Sprintf("AAC | 2 Channel | %d Kbps", bitrate)
					}
				}
			} else if variant.Codecs == "ec-3" && strings.Contains(variant.Audio, "atmos") { // Dolby Atmos
				hasAtmos = true
				split := strings.Split(variant.Audio, "-")
				if len(split) > 0 {
					bitrateStr := split[len(split)-1]
					if len(bitrateStr) == 4 && bitrateStr[0] == '2' {
						bitrateStr = bitrateStr[1:]
					}
					bitrate, _ := strconv.Atoi(bitrateStr)
					currentBitrate := 0
					if atmosQuality != "" {
						current := strings.Split(strings.Split(atmosQuality, " | ")[2], " ")[0]
						currentBitrate, _ = strconv.Atoi(current)
					}
					if bitrate > currentBitrate {
						atmosQuality = fmt.Sprintf("E-AC-3 | 16 Channel | %d Kbps", bitrate)
					}
				}
			} else if variant.Codecs == "alac" { // ALAC (Lossless or Hi-Res)
				split := strings.Split(variant.Audio, "-")
				if len(split) >= 3 {
					bitDepth := split[len(split)-1]
					sampleRate := split[len(split)-2]
					sampleRateInt, _ := strconv.Atoi(sampleRate)
					if sampleRateInt > 48000 { // Hi-Res
						hasHiRes = true
						hiResQuality = fmt.Sprintf("ALAC | 2 Channel | %s-bit/%d kHz", bitDepth, sampleRateInt/1000)
					} else { // Standard Lossless
						hasLossless = true
						losslessQuality = fmt.Sprintf("ALAC | 2 Channel | %s-bit/%d kHz", bitDepth, sampleRateInt/1000)
					}
				}
			} else if variant.Codecs == "ac-3" { // Dolby Audio
				hasDolbyAudio = true
				split := strings.Split(variant.Audio, "-")
				if len(split) > 0 {
					bitrate, _ := strconv.Atoi(split[len(split)-1])
					dolbyAudioQuality = fmt.Sprintf("AC-3 |  16 Channel | %d Kbps", bitrate)
				}
			}
		}

		fmt.Println("Available Audio Formats:")
		fmt.Println("------------------------")
		fmt.Printf("AAC             : %s\n", formatAvailability(hasAAC, aacQuality))
		fmt.Printf("Lossless        : %s\n", formatAvailability(hasLossless, losslessQuality))
		fmt.Printf("Hi-Res Lossless : %s\n", formatAvailability(hasHiRes, hiResQuality))
		fmt.Printf("Dolby Atmos     : %s\n", formatAvailability(hasAtmos, atmosQuality))
		fmt.Printf("Dolby Audio     : %s\n", formatAvailability(hasDolbyAudio, dolbyAudioQuality))
		fmt.Println("------------------------")

		return "", "", nil
	}
	var Quality string
	for _, variant := range master.Variants {
		if dl_atmos {
			if variant.Codecs == "ec-3" && strings.Contains(variant.Audio, "atmos") {
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found Dolby Atmos variant - %s (Bitrate: %d Kbps)\n",
						variant.Audio, variant.Bandwidth/1000)
				}
				split := strings.Split(variant.Audio, "-")
				length := len(split)
				length_int, err := strconv.Atoi(split[length-1])
				if err != nil {
					return "", "", err
				}
				if length_int <= Config.AtmosMax {
					if !debug_mode && !more_mode {
						fmt.Printf("%s\n", variant.Audio)
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						return "", "", err
					}
					streamUrl = streamUrlTemp
					Quality = fmt.Sprintf("%s Kbps", split[len(split)-1])
					break
				}
			} else if variant.Codecs == "ac-3" { // Add Dolby Audio support
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found Dolby Audio variant - %s (Bitrate: %d Kbps)\n",
						variant.Audio, variant.Bandwidth/1000)
				}
				streamUrlTemp, err := masterUrl.Parse(variant.URI)
				if err != nil {
					return "", "", err
				}
				streamUrl = streamUrlTemp
				split := strings.Split(variant.Audio, "-")
				Quality = fmt.Sprintf("%s Kbps", split[len(split)-1])
				break
			}
		} else if dl_aac {
			if variant.Codecs == "mp4a.40.2" {
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found AAC variant - %s (Bitrate: %d)\n", variant.Audio, variant.Bandwidth)
				}
				aacregex := regexp.MustCompile(`audio-stereo-\d+`)
				replaced := aacregex.ReplaceAllString(variant.Audio, "aac")
				if replaced == Config.AacType {
					if !debug_mode && !more_mode {
						fmt.Printf("%s\n", variant.Audio)
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						panic(err)
					}
					streamUrl = streamUrlTemp
					split := strings.Split(variant.Audio, "-")
					Quality = fmt.Sprintf("%s Kbps", split[2])
					break
				}
			}
		} else {
			if variant.Codecs == "alac" {
				split := strings.Split(variant.Audio, "-")
				length := len(split)
				length_int, err := strconv.Atoi(split[length-2])
				if err != nil {
					return "", "", err
				}
				if length_int <= Config.AlacMax {
					if !debug_mode && !more_mode {
						fmt.Printf("%s-bit / %s Hz\n", split[length-1], split[length-2])
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						panic(err)
					}
					streamUrl = streamUrlTemp
					KHZ := float64(length_int) / 1000.0
					Quality = fmt.Sprintf("%sB-%.1fkHz", split[length-1], KHZ)
					break
				}
			}
		}
	}
	if streamUrl == nil {
		return "", "", errors.New("no codec found")
	}
	return streamUrl.String(), Quality, nil
}
func extractVideo(c string) (string, error) {
	MediaUrl, err := url.Parse(c)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(c)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	videoString := string(body)

	from, listType, err := m3u8.DecodeFrom(strings.NewReader(videoString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", errors.New("m3u8 not of media type")
	}

	video := from.(*m3u8.MasterPlaylist)

	re := regexp.MustCompile(`_(\d+)x(\d+)`)

	var streamUrl *url.URL
	sort.Slice(video.Variants, func(i, j int) bool {
		return video.Variants[i].AverageBandwidth > video.Variants[j].AverageBandwidth
	})

	maxHeight := Config.MVMax

	for _, variant := range video.Variants {
		matches := re.FindStringSubmatch(variant.URI)
		if len(matches) == 3 {
			height := matches[2]
			var h int
			_, err := fmt.Sscanf(height, "%d", &h)
			if err != nil {
				continue
			}
			if h <= maxHeight {
				streamUrl, err = MediaUrl.Parse(variant.URI)
				if err != nil {
					return "", err
				}
				fmt.Println("Video: " + variant.Resolution + "-" + variant.VideoRange)
				break
			}
		}
	}

	if streamUrl == nil {
		return "", errors.New("no suitable video stream found")
	}

	return streamUrl.String(), nil
}

func ripSong(songId string, token string, storefront string, mediaUserToken string) error {
	// Get song info to find album ID
	manifest, err := ampapi.GetSongResp(storefront, songId, Config.Language, token)
	if err != nil {
		fmt.Println("Failed to get song response.")
		return err
	}

	songData := manifest.Data[0]
	albumId := songData.Relationships.Albums.Data[0].ID

	// Use album approach but only download the specific song
	dl_song = true
	err = ripAlbum(albumId, token, storefront, mediaUserToken, songId)
	if err != nil {
		fmt.Println("Failed to rip song:", err)
		return err
	}

	return nil
}
