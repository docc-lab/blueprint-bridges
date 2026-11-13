package snbounded

// Tomislav-RetCtx: every block below names the upstream DeathStarBench code it implements. References are to
// DeathStarBench commit 6ecb097 (2024-06-27), socialNetwork/src/ComposePostService/ComposePostHandler.h unless another
// file is named. Upstream's OpenTracing calls (span start/inject/finish) have no counterpart here: Blueprint's
// OpenTelemetry instrumentation wraps every service call instead.

import (
	"context"
	"sync"
	"time"
)

// The ComposePostService interface
// Tomislav-RetCtx: upstream ComposePostService::ComposePost (social_network.thrift) returns void; so does this (error only).
type ComposePostService interface {
	// Compose a post from the provided arguments
	ComposePost(ctx context.Context, reqID int64, username string, userID int64, text string, mediaIDs []int64, mediaTypes []string, postType int64) error
}

// Implementation of [ComposePostService]
type ComposePostServiceImpl struct {
	postStorageService  PostStorageService
	userTimelineService UserTimelineService
	userService         UserService
	uniqueIDService     UniqueIdService
	mediaService        MediaService
	textService         TextService
	homeTimelineService HomeTimelineService
}

// Creates a [ComposePostService] instance that creates a post from the provided arguments and connects to the various internal services to store the newly created post and update the state.
// Tomislav-RetCtx: upstream ComposePostHandler::ComposePostHandler (lines 89-108) takes one client pool per downstream service.
func NewComposePostServiceImpl(ctx context.Context, postStorageService PostStorageService, userTimelineService UserTimelineService, userService UserService, uniqueIDService UniqueIdService, mediaService MediaService, textService TextService, homeTimelineService HomeTimelineService) (ComposePostService, error) {
	return &ComposePostServiceImpl{postStorageService: postStorageService, userTimelineService: userTimelineService, userService: userService, uniqueIDService: uniqueIDService, mediaService: mediaService, textService: textService, homeTimelineService: homeTimelineService}, nil
}

// ComposePost implements ComposePostService
// Tomislav-RetCtx: upstream ComposePostHandler::ComposePost, lines 361-441.
func (c *ComposePostServiceImpl) ComposePost(ctx context.Context, reqID int64, username string, userID int64, text string, mediaIDs []int64, mediaTypes []string, postType int64) error {
	// Tomislav-RetCtx: lines 374-385: text, creator, media and unique id are requested concurrently (std::launch::async);
	// the helpers they call are _ComposeTextHelper (147-182), _ComposeCreaterHelper (110-145), _ComposeMediaHelper
	// (184-221) and _ComposeUniqueIdHelper (223-258), each one RPC.
	var textErr, creatorErr, mediaErr, uniqueIDErr error
	var upText string
	var userMentions []UserMention
	var urls []URL
	var creator Creator
	var medias []Media
	var uniqueID int64
	var wg sync.WaitGroup
	wg.Add(4)
	go func() { // Tomislav-RetCtx: lines 374-376 -> _ComposeTextHelper
		defer wg.Done()
		upText, userMentions, urls, textErr = c.textService.ComposeText(ctx, reqID, text)
	}()
	go func() { // Tomislav-RetCtx: lines 377-379 -> _ComposeCreaterHelper
		defer wg.Done()
		creator, creatorErr = c.userService.ComposeCreatorWithUserId(ctx, reqID, userID, username)
	}()
	go func() { // Tomislav-RetCtx: lines 380-382 -> _ComposeMediaHelper
		defer wg.Done()
		medias, mediaErr = c.mediaService.ComposeMedia(ctx, reqID, mediaTypes, mediaIDs)
	}()
	go func() { // Tomislav-RetCtx: lines 383-385 -> _ComposeUniqueIdHelper
		defer wg.Done()
		uniqueID, uniqueIDErr = c.uniqueIDService.ComposeUniqueId(ctx, reqID, postType)
	}()

	// Tomislav-RetCtx: lines 387-391: the post's timestamp is taken after the four requests are launched.
	var post Post
	timestamp := time.Now().UnixMilli()
	post.Timestamp = timestamp

	// Tomislav-RetCtx: lines 395-403: results are collected in upstream's order (unique id, creator, media, text); a
	// future that threw rethrows at its get(). std::async futures are joined before the function returns, so every
	// request finishes before an error is returned, as wg.Wait() does here.
	wg.Wait()
	if uniqueIDErr != nil { // line 395
		return uniqueIDErr
	}
	post.PostID = uniqueID
	if creatorErr != nil { // line 396
		return creatorErr
	}
	post.Creator = creator
	if mediaErr != nil { // line 397
		return mediaErr
	}
	post.Medias = medias
	if textErr != nil { // line 398
		return textErr
	}
	post.Text = upText               // line 399
	post.Urls = urls                 // line 400
	post.UserMentions = userMentions // line 401
	post.ReqID = reqID               // line 402
	post.PostType = postType         // line 403

	// Tomislav-RetCtx: lines 410-413
	var userMentionIDs []int64
	for _, item := range post.UserMentions {
		userMentionIDs = append(userMentionIDs, item.UserID)
	}

	// Tomislav-RetCtx: lines 415-434: the post is stored first (std::launch::async, joined at line 432); the user and
	// home timeline uploads are std::launch::deferred, so each runs only when its get() is reached (lines 433, 434):
	// StorePost, then WriteUserTimeline, then WriteHomeTimeline, one after the other; an exception stops the rest.
	if err := c.postStorageService.StorePost(ctx, reqID, post); err != nil { // lines 419-421, 432 -> _UploadPostHelper (260-291)
		return err
	}
	if err := c.userTimelineService.WriteUserTimeline(ctx, reqID, post.PostID, userID, timestamp); err != nil { // lines 422-424, 433 -> _UploadUserTimelineHelper (293-324)
		return err
	}
	return c.homeTimelineService.WriteHomeTimeline(ctx, reqID, post.PostID, userID, timestamp, userMentionIDs) // lines 425-428, 434 -> _UploadHomeTimelineHelper (326-359)
}
