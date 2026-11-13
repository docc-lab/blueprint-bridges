package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/MediaService/MediaHandler.h.

import (
	"context"
	"errors"
)

// The MediaService interface
type MediaService interface {
	// Creates and returns a list of Media objects from the provided `mediaTypes` and `mediaIds` arguments.
	// Returns an error if the length of mediaTypes and mediaIds do not match.
	ComposeMedia(ctx context.Context, reqID int64, mediaTypes []string, mediaIds []int64) ([]Media, error)
}

// Implementation of [MediaService]
type MediaServiceImpl struct{}

// Creates a [MediaService] instance that creates media objects
func NewMediaServiceImpl(ctx context.Context) (MediaService, error) {
	return &MediaServiceImpl{}, nil
}

// Implements MediaService interface
// Tomislav-RetCtx: MediaHandler::ComposeMedia, lines 30-60.
func (m *MediaServiceImpl) ComposeMedia(ctx context.Context, reqID int64, mediaTypes []string, mediaIds []int64) ([]Media, error) {
	var medias []Media
	if len(mediaTypes) != len(mediaIds) { // lines 44-50
		return medias, errors.New("The lengths of media_id list and media_type list are not equal")
	}
	for i := range mediaIds { // lines 52-57
		medias = append(medias, Media{MediaID: mediaIds[i], MediaType: mediaTypes[i]})
	}
	return medias, nil
}
