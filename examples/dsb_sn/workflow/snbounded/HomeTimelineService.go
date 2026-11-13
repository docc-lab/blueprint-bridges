package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/HomeTimelineService/HomeTimelineHandler.h. A home timeline is the Redis sorted set "<user_id>"
// (member = post id, score = post timestamp) in home-timeline-redis; only the single-Redis-instance branches are
// implemented (replication/cluster are deployment options this deployment does not use).

import (
	"context"
	"errors"
	"log"
	"sort"
	"strconv"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/redis"
)

// The HomeTimelineService Interface
// The full Timeline of a user is represented as an array of post ids: post_ids[id_0 ,..., id_n].
type HomeTimelineService interface {
	// Reads the timeline of the user that has the id `userID`.
	// Tomislav-RetCtx: returns the posts, as upstream (vector<Post>, line 215).
	ReadHomeTimeline(ctx context.Context, reqID int64, userID int64, start int64, stop int64) ([]Post, error)
	// Adds a new post to the home timeline of the following users:
	// (ii)  all the followers of the user with `userID`
	// (iii) all the mentioned users in the post listed in the `userMentionIDs`.
	WriteHomeTimeline(ctx context.Context, reqID int64, postID int64, userID int64, timestamp int64, userMentionIDs []int64) error
}

// Implementation of [HomeTimelineService]
type HomeTimelineServiceImpl struct {
	homeTimelineCache  backend.Cache
	postStorageService PostStorageService
	socialGraphService SocialGraphService
}

// Creates a [HomeTimelineService] instance that maintains the home timelines for the various users.
// Tomislav-RetCtx: HomeTimelineHandler::HomeTimelineHandler (lines 55-66).
func NewHomeTimelineServiceImpl(ctx context.Context, homeTimelineCache backend.Cache, postStorageService PostStorageService, socialGraphService SocialGraphService) (HomeTimelineService, error) {
	return &HomeTimelineServiceImpl{homeTimelineCache: homeTimelineCache, postStorageService: postStorageService, socialGraphService: socialGraphService}, nil
}

func (h *HomeTimelineServiceImpl) redisCache() (*redis.RedisCache, error) {
	cache, ok := h.homeTimelineCache.(*redis.RedisCache)
	if !ok {
		return nil, errors.New("snbounded: home timelines need a Redis cache, as upstream's home-timeline-redis")
	}
	return cache, nil
}

// Implements HomeTimelineService interface
// Tomislav-RetCtx: HomeTimelineHandler::WriteHomeTimeline, lines 99-211.
func (h *HomeTimelineServiceImpl) WriteHomeTimeline(ctx context.Context, reqID int64, postID int64, userID int64, timestamp int64, userMentionIDs []int64) error {
	// Tomislav-RetCtx: lines 109-134: the poster's followers, from the social-graph service.
	followers, err := h.socialGraphService.GetFollowers(ctx, reqID, userID)
	if err != nil {
		log.Printf("WriteHomeTimeline: GetFollowers FAILED reqID=%d userID=%d error=%v", reqID, userID, err)
		return err
	}
	// Tomislav-RetCtx: lines 136-137: followers and mentioned users as one std::set (deduplicated, ascending ids).
	set := make(map[int64]bool, len(followers)+len(userMentionIDs))
	for _, id := range followers {
		set[id] = true
	}
	for _, id := range userMentionIDs {
		set[id] = true
	}
	recipients := make([]int64, 0, len(set))
	for id := range set {
		recipients = append(recipients, id)
	}
	sort.Slice(recipients, func(i, j int) bool { return recipients[i] < recipients[j] })

	// Tomislav-RetCtx: lines 139-159: one pipeline, ZADD NX "<recipient>" post_id timestamp per recipient.
	cache, err := h.redisCache()
	if err != nil {
		return err
	}
	postIDStr := strconv.FormatInt(postID, 10) // line 144
	operations := make([]struct {
		Key    string
		Score  float64
		Member string
	}, 0, len(recipients))
	for _, id := range recipients { // lines 149-152
		operations = append(operations, struct {
			Key    string
			Score  float64
			Member string
		}{Key: strconv.FormatInt(id, 10), Score: float64(timestamp), Member: postIDStr})
	}
	if err := cache.ZAddNXBatch(ctx, operations); err != nil { // lines 153-158
		log.Printf("WriteHomeTimeline: ZAddNXBatch FAILED reqID=%d postID=%d recipients=%d error=%v", reqID, postID, len(recipients), err)
		return err
	}
	return nil
}

// Implements HomeTimelineService interface
// Tomislav-RetCtx: HomeTimelineHandler::ReadHomeTimeline, lines 214-280.
func (h *HomeTimelineServiceImpl) ReadHomeTimeline(ctx context.Context, reqID int64, userID int64, start int64, stop int64) ([]Post, error) {
	if stop <= start || start < 0 { // lines 226-228
		return []Post{}, nil
	}
	cache, err := h.redisCache()
	if err != nil {
		return nil, err
	}
	postIDStrs, err := cache.ZRevRange(ctx, strconv.FormatInt(userID, 10), start, stop-1) // lines 234-256: ZREVRANGE start stop-1
	if err != nil {
		return nil, err
	}
	postIDs := make([]int64, 0, len(postIDStrs))
	for _, s := range postIDStrs { // lines 258-261 (std::stoul)
		id, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		postIDs = append(postIDs, int64(id))
	}
	return h.postStorageService.ReadPosts(ctx, reqID, postIDs) // lines 263-279
}
