package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/UserTimelineService/UserTimelineHandler.h unless another file is named. A user timeline is the
// MongoDB document {user_id, posts: [{post_id, timestamp}, ...]} in user-timeline.user-timeline (newest first, grows
// without limit) and the Redis sorted set "<user_id>" (member = post id, score = timestamp) in user-timeline-redis.
// Only the single-Redis-instance branches are implemented (replication/cluster are deployment options this deployment
// does not use).

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/mongodb"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/redis"
	"go.mongodb.org/mongo-driver/bson"
)

// The UserTimelineService interface
// The full Timeline of a user is represented as an array of post ids: post_ids[id_0 ,..., id_n].
type UserTimelineService interface {
	// Reads the timeline of the user that has the id `userID`.
	// Tomislav-RetCtx: returns the posts, as upstream (vector<Post>, line 187).
	ReadUserTimeline(ctx context.Context, reqID int64, userID int64, start int64, stop int64) ([]Post, error)
	// Adds a new post to the user timeline of the user that has the id `userID`
	// The new post ID is placed at the 0th position in the post ids array.
	WriteUserTimeline(ctx context.Context, reqID int64, postID int64, userID int64, timestamp int64) error
}

// The format of a single post in a user's timeline stored in the backend.
// Tomislav-RetCtx: lines 123-124 (post_id, timestamp).
type PostInfo struct {
	PostID    int64 `bson:"post_id" json:"post_id"`
	Timestamp int64 `bson:"timestamp" json:"timestamp"`
}

// The format of a user's timeline stored in the backend.
// Tomislav-RetCtx: the document the upsert at lines 119-133 creates: user_id, posts.
type UserPosts struct {
	UserID int64      `bson:"user_id" json:"user_id"`
	Posts  []PostInfo `bson:"posts" json:"posts"`
}

// Implementation of [UserTimelineService]
type UserTimelineServiceImpl struct {
	userTimelineCache  backend.Cache
	userTimelineDB     backend.NoSQLDatabase
	postStorageService PostStorageService
}

// Tomislav-RetCtx: FindOneAndUpdate is outside backend.NoSQLCollection; the MongoDB plugin's collection is reached
// through its concrete type (an interface declared in this package could be matched by Blueprint's workflow parser as
// a backend client's service interface).
func findAndModifierOf(collection backend.NoSQLCollection) (*mongodb.MongoCollection, bool) {
	c, ok := collection.(*mongodb.MongoCollection)
	return c, ok
}

// Creates a [UserTimelineService] instance for managing the user timelines for the various users.
// Tomislav-RetCtx: UserTimelineHandler::UserTimelineHandler (lines 53-62); the index is created in
// UserTimelineService.cpp (lines 98-105, unique user-timeline.user_id), retried until it exists.
func NewUserTimelineServiceImpl(ctx context.Context, userTimelineCache backend.Cache, userTimelineDB backend.NoSQLDatabase, postStorageService PostStorageService) (UserTimelineService, error) {
	u := &UserTimelineServiceImpl{userTimelineCache: userTimelineCache, userTimelineDB: userTimelineDB, postStorageService: postStorageService}
	if err := ensureIndex(userTimelineDB, "user-timeline", "user-timeline", "user_id", true); err != nil {
		return nil, err
	}
	return u, nil
}

func (u *UserTimelineServiceImpl) redisCache() (*redis.RedisCache, error) {
	cache, ok := u.userTimelineCache.(*redis.RedisCache)
	if !ok {
		return nil, errors.New("snbounded: user timelines need a Redis cache, as upstream's user-timeline-redis")
	}
	return cache, nil
}

// Implements UserTimelineService interface
// Tomislav-RetCtx: UserTimelineHandler::WriteUserTimeline, lines 90-184.
func (u *UserTimelineServiceImpl) WriteUserTimeline(ctx context.Context, reqID int64, postID int64, userID int64, timestamp int64) error {
	collection, err := u.userTimelineDB.GetCollection(ctx, "user-timeline", "user-timeline") // lines 103-118
	if err != nil {
		log.Printf("WriteUserTimeline: GetCollection FAILED reqID=%d postID=%d userID=%d error=%v", reqID, postID, userID, err)
		return err
	}
	modifier, ok := findAndModifierOf(collection)
	if !ok {
		return errors.New("snbounded: the user-timeline database has no FindOneAndUpdate")
	}
	filter := bson.D{{Key: "user_id", Value: userID}}                         // lines 119-121
	push := bson.D{{Key: "$push", Value: bson.D{{Key: "posts", Value: bson.D{ // lines 122-125: prepend {post_id, timestamp}
		{Key: "$each", Value: bson.A{bson.D{{Key: "post_id", Value: postID}, {Key: "timestamp", Value: timestamp}}}},
		{Key: "$position", Value: int32(0)},
	}}}}}
	// Tomislav-RetCtx: lines 131-133: findAndModify upsert=true new=true; the returned document (the user's whole
	// timeline) is discarded unread (bson_destroy(&reply), line 157).
	if _, err := modifier.FindOneAndUpdate(ctx, filter, push, true); err != nil {
		// Tomislav-RetCtx: lines 136-140: update the newly inserted document (upsert: false)
		if _, err := modifier.FindOneAndUpdate(ctx, filter, push, false); err != nil {
			log.Printf("WriteUserTimeline: findAndModify FAILED reqID=%d postID=%d userID=%d error=%v", reqID, postID, userID, err) // lines 141-154
			return errors.New("Failed to update user-timeline for user " + strconv.FormatInt(userID, 10) + " to MongoDB: " + err.Error())
		}
	}

	// Tomislav-RetCtx: lines 162-182: ZADD NX "<user_id>" post_id timestamp.
	cache, err := u.redisCache()
	if err != nil {
		return err
	}
	if err := cache.ZAddNXBatch(ctx, []struct {
		Key    string
		Score  float64
		Member string
	}{{Key: strconv.FormatInt(userID, 10), Score: float64(timestamp), Member: strconv.FormatInt(postID, 10)}}); err != nil {
		log.Printf("WriteUserTimeline: Redis ZADD FAILED reqID=%d postID=%d userID=%d error=%v", reqID, postID, userID, err)
		return err
	}
	return nil
}

// Implements UserTimelineService interface
// Tomislav-RetCtx: UserTimelineHandler::ReadUserTimeline, lines 186-359.
func (u *UserTimelineServiceImpl) ReadUserTimeline(ctx context.Context, reqID int64, userID int64, start int64, stop int64) ([]Post, error) {
	if stop <= start || start < 0 { // lines 198-200
		return []Post{}, nil
	}
	cache, err := u.redisCache()
	if err != nil {
		return nil, err
	}
	userIDStr := strconv.FormatInt(userID, 10)
	postIDStrs, err := cache.ZRevRange(ctx, userIDStr, start, stop-1) // lines 206-222: ZREVRANGE start stop-1
	if err != nil {
		return nil, err
	}
	postIDs := make([]int64, 0, len(postIDStrs))
	for _, s := range postIDStrs { // lines 224-227 (std::stoul)
		id, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		postIDs = append(postIDs, int64(id))
	}

	// Tomislav-RetCtx: lines 230-301: if Redis holds fewer than requested, read posts [0, stop) of the MongoDB document;
	// those from index mongo_start on that Redis did not return are added to the result, and every post read goes back
	// into Redis.
	mongoStart := start + int64(len(postIDs)) // line 230
	var redisUpdate []struct {                // line 231 (an unordered_map keyed by post id string)
		Score  float64
		Member string
	}
	if mongoStart < stop { // line 232
		collection, err := u.userTimelineDB.GetCollection(ctx, "user-timeline", "user-timeline") // lines 233-250
		if err != nil {
			return nil, err
		}
		query := bson.D{{Key: "user_id", Value: userID}}                                                           // line 251
		projection := bson.D{{Key: "posts", Value: bson.D{{Key: "$slice", Value: bson.A{int32(0), int32(stop)}}}}} // lines 252-253
		res, err := collection.FindOne(ctx, query, projection)                                                     // lines 258-259
		if err != nil {
			return nil, err
		}
		var userPosts UserPosts
		found, err := res.One(ctx, &userPosts) // line 262
		if err != nil {
			return nil, err
		}
		if found { // lines 263-295
			seen := make(map[string]bool)
			for idx, post := range userPosts.Posts { // lines 271-294
				if int64(idx) >= mongoStart { // lines 282-288: skip ids Redis already returned (std::find)
					already := false
					for _, id := range postIDs {
						if id == post.PostID {
							already = true
							break
						}
					}
					if !already {
						postIDs = append(postIDs, post.PostID)
					}
				}
				key := strconv.FormatInt(post.PostID, 10) // lines 289-290: map insert keeps the first value for a key
				if !seen[key] {
					seen[key] = true
					redisUpdate = append(redisUpdate, struct {
						Score  float64
						Member string
					}{Score: float64(post.Timestamp), Member: key})
				}
			}
		}
	}

	// Tomislav-RetCtx: lines 303-324: the posts are read concurrently with the Redis update below.
	var posts []Post
	var postsErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		posts, postsErr = u.postStorageService.ReadPosts(ctx, reqID, postIDs) // line 315
	}()
	var redisErr error
	if len(redisUpdate) > 0 { // lines 326-351: one ZADD "<user_id>" with every post read from MongoDB
		redisErr = cache.ZAddMembers(ctx, userIDStr, redisUpdate)
	}
	wg.Wait()
	if redisErr != nil { // lines 345-349: a Redis error is rethrown (the future is joined as the stack unwinds)
		return nil, redisErr
	}
	return posts, postsErr // lines 352-357
}
