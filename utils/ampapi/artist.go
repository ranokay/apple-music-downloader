package ampapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

func GetArtistResp(storefront string, id string, language string, token string) (*ArtistResp, error) {
	var err error
	if token == "" {
		token, err = GetToken()
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s", storefront, id), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Origin", "https://music.apple.com")
	query := url.Values{}
	query.Set("l", language)
	query.Set("fields[artists]", "name,artwork")
	req.URL.RawQuery = query.Encode()
	do, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer do.Body.Close()
	if do.StatusCode != http.StatusOK {
		return nil, errors.New(do.Status)
	}
	obj := new(ArtistResp)
	if err := json.NewDecoder(do.Body).Decode(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

type ArtistResp struct {
	Href string           `json:"href"`
	Next string           `json:"next"`
	Data []ArtistRespData `json:"data"`
}

type ArtistRespData struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	Href       string `json:"href"`
	Attributes struct {
		Name    string `json:"name"`
		Artwork struct {
			Url string `json:"url"`
		} `json:"artwork"`
	} `json:"attributes"`
}
