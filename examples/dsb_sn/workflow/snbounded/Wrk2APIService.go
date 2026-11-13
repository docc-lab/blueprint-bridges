package snbounded

// Tomislav-RetCtx: the HTTP entry service. Upstream's entry is nginx with the Lua scripts in DeathStarBench commit
// 6ecb097, socialNetwork/nginx-web-server/lua-scripts/wrk2-api/ (paths below are relative to that directory); each
// method here names the script and lines it implements. Upstream derives req_id from nginx's request_id (first 15 hex
// digits, line 18/22/58 of each script); here it is a random 63-bit number. Upstream's arguments arrive as strings and
// are checked with _StrIsEmpty; Blueprint's HTTP plugin decodes typed arguments, so an empty numeric argument arrives
// as 0.

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// The Wrk2APIService (Frontend) interface
type Wrk2APIService interface {
	// Reads the home timeline of the user with `userId`.
	// Tomislav-RetCtx: returns the posts, as upstream home-timeline/read.lua (cjson.encode of the posts, line 99).
	ReadHomeTimeline(ctx context.Context, userId int64, start int64, stop int64) ([]Post, error)
	// Reads the user timeline of the user with `userId`.
	// Tomislav-RetCtx: returns the posts, as upstream user-timeline/read.lua (line 99).
	ReadUserTimeline(ctx context.Context, userId int64, start int64, stop int64) ([]Post, error)
	// Creates a Follow-Link between users `userId`-`followeeID`.
	// If the user ids are not provided, then it creates a follow-link between users `username`-`followeeName`.
	// Returns an error if no pairs are provided.
	Follow(ctx context.Context, username string, followeeName string, userId int64, followeeID int64) error
	// Removes a Follow-Link between users `userId`-`followeeID`.
	// If the user ids are not provided, then it creates a follow-link between users `username`-`followeeName`.
	// Returns an error if no pairs are provided.
	Unfollow(ctx context.Context, username string, followeeName string, userId int64, followeeID int64) error
	// Registers a new user with the given `userId`.
	Register(ctx context.Context, firstName string, lastName string, username string, password string, userId int64) error
	// Composes a new post give the provided arguments.
	// Tomislav-RetCtx: returns nothing but success or failure, as upstream post/compose.lua ("Successfully upload post").
	ComposePost(ctx context.Context, userId int64, username string, post_type int64, text string, media_types []string, media_ids []int64) error
}

// Implementation of [Wrk2APIService]
type Wrk2APIServiceImpl struct {
	userService         UserService
	composePostService  ComposePostService
	userTimelineService UserTimelineService
	homeTimelineService HomeTimelineService
	socialGraphService  SocialGraphService
}

// Creates a [Wrk2APIService] instance to act as the gateway to internal services.
func NewWrk2APIServiceImpl(ctx context.Context, userService UserService, composePostService ComposePostService, userTimelineService UserTimelineService, homeTimelineService HomeTimelineService, socialGraphService SocialGraphService) (Wrk2APIService, error) {
	rand.Seed(time.Now().UnixNano())
	return &Wrk2APIServiceImpl{userService: userService, composePostService: composePostService, userTimelineService: userTimelineService, homeTimelineService: homeTimelineService, socialGraphService: socialGraphService}, nil
}

// Implements Wrk2APIService interface
// Tomislav-RetCtx: upstream user-timeline/read.lua, _M.ReadUserTimeline (lines 49-100).
func (w *Wrk2APIServiceImpl) ReadUserTimeline(ctx context.Context, userId int64, start int64, stop int64) ([]Post, error) {
	reqID := rand.Int63()                                                          // line 58
	return w.userTimelineService.ReadUserTimeline(ctx, reqID, userId, start, stop) // lines 81-82
}

// Implements Wrk2APIService interface
// Tomislav-RetCtx: upstream home-timeline/read.lua, _M.ReadHomeTimeline (lines 49-100).
func (w *Wrk2APIServiceImpl) ReadHomeTimeline(ctx context.Context, userId int64, start int64, stop int64) ([]Post, error) {
	reqID := rand.Int63()                                                          // line 58
	return w.homeTimelineService.ReadHomeTimeline(ctx, reqID, userId, start, stop) // lines 81-82
}

// Implements Wrk2APIService interface
// Tomislav-RetCtx: upstream user/follow.lua, _M.Follow (from line 11).
func (w *Wrk2APIServiceImpl) Follow(ctx context.Context, username string, followeeName string, userId int64, followeeID int64) error {
	reqID := rand.Int63()               // line 18
	if userId != 0 && followeeID != 0 { // lines 35-37
		return w.socialGraphService.Follow(ctx, reqID, userId, followeeID)
	} else if username != "" && followeeName != "" { // lines 38-40
		return w.socialGraphService.FollowWithUsername(ctx, reqID, username, followeeName)
	}
	return errors.New("Incomplete arguments") // lines 41-44
}

// Implements Wrk2APIService interface
// Tomislav-RetCtx: upstream user/unfollow.lua, _M.Unfollow (from line 11).
func (w *Wrk2APIServiceImpl) Unfollow(ctx context.Context, username string, followeeName string, userId int64, followeeID int64) error {
	reqID := rand.Int63()               // line 18
	if userId != 0 && followeeID != 0 { // lines 35-37
		return w.socialGraphService.Unfollow(ctx, reqID, userId, followeeID)
	} else if username != "" && followeeName != "" { // lines 38-40
		return w.socialGraphService.UnfollowWithUsername(ctx, reqID, username, followeeName)
	}
	return errors.New("Incomplete arguments") // lines 41-44
}

// Implements Wrk2APIService interface
// Tomislav-RetCtx: upstream user/register.lua, _M.RegisterUser (from line 11).
func (w *Wrk2APIServiceImpl) Register(ctx context.Context, firstName string, lastName string, username string, password string, userId int64) error {
	reqID := rand.Int63()                                                      // line 18
	if firstName == "" || lastName == "" || username == "" || password == "" { // lines 30-36 (user_id 0 is a real user: the seed registers 0..N-1)
		return errors.New("Incomplete arguments")
	}
	return w.userService.RegisterUserWithId(ctx, reqID, firstName, lastName, username, password, userId) // lines 41-42
}

// Implements Wrk2APIService interface
// Tomislav-RetCtx: upstream post/compose.lua, _M.ComposePost (lines 11-76).
func (w *Wrk2APIServiceImpl) ComposePost(ctx context.Context, userId int64, username string, post_type int64, text string, media_types []string, media_ids []int64) error {
	reqID := rand.Int63()             // line 22
	if username == "" || text == "" { // lines 30-36 (user_id and post_type are integers here; 0 is a valid value of both)
		return errors.New("Incomplete arguments")
	}
	return w.composePostService.ComposePost(ctx, reqID, username, userId, text, media_ids, media_types, post_type) // lines 46-56
}
