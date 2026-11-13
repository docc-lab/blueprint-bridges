package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/SocialGraphService/SocialGraphHandler.h unless another file is named. Upstream keeps each user's
// followers and followees in Redis sorted sets "<user_id>:followers" / "<user_id>:followees" (member = the other user's
// id, score = the follow timestamp in ms) and in MongoDB social-graph.social-graph documents {user_id, followers:
// [{user_id, timestamp}], followees: [...]}. Only the single-Redis-instance branches are implemented (the
// replication/cluster branches are deployment options this deployment does not use). GetUserId is a call to the user
// service, as upstream's (_user_service_client_pool).

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/redis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type SocialGraphService interface {
	GetFollowers(ctx context.Context, reqID int64, userID int64) ([]int64, error)
	GetFollowees(ctx context.Context, reqID int64, userID int64) ([]int64, error)
	Follow(ctx context.Context, reqID int64, userID int64, followeeID int64) error
	Unfollow(ctx context.Context, reqID int64, userID int64, followeeID int64) error
	FollowWithUsername(ctx context.Context, reqID int64, userUsername string, followeeUsername string) error
	UnfollowWithUsername(ctx context.Context, reqID int64, userUsername string, followeeUsername string) error
	InsertUser(ctx context.Context, reqID int64, userID int64) error
}

// Tomislav-RetCtx: one element of a social-graph document's followers or followees array (lines 145-147, 201-203).
type FollowerInfo struct {
	FollowerID int64 `bson:"user_id" json:"user_id"`
	Timestamp  int64 `bson:"timestamp" json:"timestamp"`
}

type FolloweeInfo struct {
	FolloweeID int64 `bson:"user_id" json:"user_id"`
	Timestamp  int64 `bson:"timestamp" json:"timestamp"`
}

// Tomislav-RetCtx: a social-graph document (InsertUser, lines 798-799).
type UserInfo struct {
	UserID    int64          `bson:"user_id" json:"user_id"`
	Followers []FollowerInfo `bson:"followers" json:"followers"`
	Followees []FolloweeInfo `bson:"followees" json:"followees"`
}

type SocialGraphServiceImpl struct {
	socialGraphCache backend.Cache
	socialGraphDB    backend.NoSQLDatabase
	userService      UserService
}

// Tomislav-RetCtx: SocialGraphHandler::SocialGraphHandler (lines 65-74); the index is created in SocialGraphService.cpp
// (line 95, unique social-graph.user_id), retried until it exists.
func NewSocialGraphServiceImpl(ctx context.Context, socialGraphCache backend.Cache, socialGraphDB backend.NoSQLDatabase, userService UserService) (SocialGraphService, error) {
	if err := ensureIndex(socialGraphDB, "social-graph", "social-graph", "user_id", true); err != nil {
		return nil, err
	}
	return &SocialGraphServiceImpl{socialGraphCache: socialGraphCache, socialGraphDB: socialGraphDB, userService: userService}, nil
}

func (s *SocialGraphServiceImpl) redisCache() (*redis.RedisCache, error) {
	cache, ok := s.socialGraphCache.(*redis.RedisCache)
	if !ok {
		return nil, errors.New("snbounded: the social graph needs a Redis cache, as upstream's social-graph-redis")
	}
	return cache, nil
}

// Tomislav-RetCtx: upstream's find_and_modify returns true when no document matched; the Go driver reports that as
// ErrNoDocuments, which is therefore not a failure here.
func findAndModify(ctx context.Context, collection backend.NoSQLCollection, filter bson.D, update bson.D, upsert bool) error {
	modifier, ok := findAndModifierOf(collection)
	if !ok {
		return errors.New("snbounded: the social-graph database has no FindOneAndUpdate")
	}
	if _, err := modifier.FindOneAndUpdate(ctx, filter, update, upsert); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}
	return nil
}

// Implements SocialGraphService interface
// Tomislav-RetCtx: SocialGraphHandler::GetFollowers, lines 485-621.
func (s *SocialGraphServiceImpl) GetFollowers(ctx context.Context, reqID int64, userID int64) ([]int64, error) {
	return s.getEdges(ctx, userID, "followers")
}

// Implements SocialGraphService interface
// Tomislav-RetCtx: SocialGraphHandler::GetFollowees, lines 623-766 (GetFollowers with "followees").
func (s *SocialGraphServiceImpl) GetFollowees(ctx context.Context, reqID int64, userID int64) ([]int64, error) {
	return s.getEdges(ctx, userID, "followees")
}

// Tomislav-RetCtx: the body of GetFollowers (lines 501-618) and GetFollowees (lines 639-763); field is "followers" or
// "followees".
func (s *SocialGraphServiceImpl) getEdges(ctx context.Context, userID int64, field string) ([]int64, error) {
	cache, err := s.redisCache()
	if err != nil {
		return nil, err
	}
	var result []int64
	key := strconv.FormatInt(userID, 10) + ":" + field // line 502
	members, err := cache.ZRange(ctx, key, 0, -1)      // lines 503-519: ZRANGE key 0 -1
	if err != nil {
		return nil, err
	}
	if len(members) > 0 { // lines 521-526: served from Redis
		for _, member := range members {
			id, err := strconv.ParseUint(member, 10, 64) // std::stoul
			if err != nil {
				return nil, err
			}
			result = append(result, int64(id))
		}
		return result, nil
	}
	// Tomislav-RetCtx: lines 528-610: not in Redis: MongoDB find {user_id}, then ZADD every edge to Redis in one command.
	collection, err := s.socialGraphDB.GetCollection(ctx, "social-graph", "social-graph") // lines 537-545
	if err != nil {
		return nil, err
	}
	res, err := collection.FindOne(ctx, bson.D{{Key: "user_id", Value: userID}}) // lines 546-552
	if err != nil {
		return nil, err
	}
	var info UserInfo
	found, err := res.One(ctx, &info) // line 554
	if err != nil {
		return nil, err
	}
	if !found { // lines 611-618: logged, nothing returned
		return result, nil
	}
	edges := info.Followers
	if field == "followees" {
		edges = make([]FollowerInfo, len(info.Followees))
		for i, e := range info.Followees {
			edges[i] = FollowerInfo{FollowerID: e.FolloweeID, Timestamp: e.Timestamp}
		}
	}
	zset := make([]struct {
		Score  float64
		Member string
	}, 0, len(edges))
	for _, edge := range edges { // lines 565-583: every {user_id, timestamp} of the array
		result = append(result, edge.FollowerID)
		zset = append(zset, struct {
			Score  float64
			Member string
		}{Score: float64(edge.Timestamp), Member: strconv.FormatInt(edge.FollowerID, 10)})
	}
	if err := cache.ZAddMembers(ctx, key, zset); err != nil { // lines 590-609: one ZADD key with all members
		return nil, err
	}
	return result, nil
}

// Implements SocialGraphService interface
// Tomislav-RetCtx: SocialGraphHandler::Follow, lines 103-301.
func (s *SocialGraphServiceImpl) Follow(ctx context.Context, reqID int64, userID int64, followeeID int64) error {
	timestamp := time.Now().UnixMilli() // lines 115-117
	var followerErr, followeeErr, redisErr error
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { // Tomislav-RetCtx: lines 119-174: follower->followee edge, only if not already there
		defer wg.Done()
		collection, err := s.socialGraphDB.GetCollection(ctx, "social-graph", "social-graph")
		if err != nil {
			followerErr = err
			return
		}
		filter := bson.D{{Key: "$and", Value: bson.A{ // lines 141-144
			bson.D{{Key: "user_id", Value: userID}},
			bson.D{{Key: "followees", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$elemMatch", Value: bson.D{{Key: "user_id", Value: followeeID}}}}}}}},
		}}}
		update := bson.D{{Key: "$push", Value: bson.D{{Key: "followees", Value: bson.D{ // lines 145-147
			{Key: "user_id", Value: followeeID}, {Key: "timestamp", Value: timestamp}}}}}}
		followerErr = findAndModify(ctx, collection, filter, update, false) // lines 152-154 (upsert false, new true)
	}()
	go func() { // Tomislav-RetCtx: lines 176-231: followee->follower edge, only if not already there
		defer wg.Done()
		collection, err := s.socialGraphDB.GetCollection(ctx, "social-graph", "social-graph")
		if err != nil {
			followeeErr = err
			return
		}
		filter := bson.D{{Key: "$and", Value: bson.A{ // lines 197-200
			bson.D{{Key: "user_id", Value: followeeID}},
			bson.D{{Key: "followers", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$elemMatch", Value: bson.D{{Key: "user_id", Value: userID}}}}}}}},
		}}}
		update := bson.D{{Key: "$push", Value: bson.D{{Key: "followers", Value: bson.D{ // lines 201-203
			{Key: "user_id", Value: userID}, {Key: "timestamp", Value: timestamp}}}}}}
		followeeErr = findAndModify(ctx, collection, filter, update, false) // lines 209-211
	}()
	go func() { // Tomislav-RetCtx: lines 233-288: one pipeline, ZADD NX on both sorted sets
		defer wg.Done()
		cache, err := s.redisCache()
		if err != nil {
			redisErr = err
			return
		}
		redisErr = cache.ZAddNXBatch(ctx, []struct {
			Key    string
			Score  float64
			Member string
		}{
			{Key: strconv.FormatInt(userID, 10) + ":followees", Score: float64(timestamp), Member: strconv.FormatInt(followeeID, 10)}, // lines 241-242
			{Key: strconv.FormatInt(followeeID, 10) + ":followers", Score: float64(timestamp), Member: strconv.FormatInt(userID, 10)}, // lines 243-244
		})
	}()
	wg.Wait()
	// Tomislav-RetCtx: lines 289-296: the futures are joined in this order; the first exception is rethrown.
	if redisErr != nil {
		return redisErr
	}
	if followerErr != nil {
		return followerErr
	}
	return followeeErr
}

// Implements SocialGraphService interface
// Tomislav-RetCtx: SocialGraphHandler::Unfollow, lines 303-483.
func (s *SocialGraphServiceImpl) Unfollow(ctx context.Context, reqID int64, userID int64, followeeID int64) error {
	var followerErr, followeeErr, redisErr error
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { // Tomislav-RetCtx: lines 315-367: $pull the followee from the user's followees
		defer wg.Done()
		collection, err := s.socialGraphDB.GetCollection(ctx, "social-graph", "social-graph")
		if err != nil {
			followerErr = err
			return
		}
		followerErr = findAndModify(ctx, collection, bson.D{{Key: "user_id", Value: userID}}, // lines 334-339, 345-347
			bson.D{{Key: "$pull", Value: bson.D{{Key: "followees", Value: bson.D{{Key: "user_id", Value: followeeID}}}}}}, false)
	}()
	go func() { // Tomislav-RetCtx: lines 369-421: $pull the user from the followee's followers
		defer wg.Done()
		collection, err := s.socialGraphDB.GetCollection(ctx, "social-graph", "social-graph")
		if err != nil {
			followeeErr = err
			return
		}
		followeeErr = findAndModify(ctx, collection, bson.D{{Key: "user_id", Value: followeeID}}, // lines 388-393, 399-401
			bson.D{{Key: "$pull", Value: bson.D{{Key: "followers", Value: bson.D{{Key: "user_id", Value: userID}}}}}}, false)
	}()
	go func() { // Tomislav-RetCtx: lines 423-472: one pipeline, ZREM from both sorted sets
		defer wg.Done()
		cache, err := s.redisCache()
		if err != nil {
			redisErr = err
			return
		}
		redisErr = cache.ZRemBatch(ctx, []struct {
			Key    string
			Member string
		}{
			{Key: strconv.FormatInt(userID, 10) + ":followees", Member: strconv.FormatInt(followeeID, 10)}, // lines 430, 432
			{Key: strconv.FormatInt(followeeID, 10) + ":followers", Member: strconv.FormatInt(userID, 10)}, // lines 431, 433
		})
	}()
	wg.Wait()
	// Tomislav-RetCtx: lines 474-480: joined in this order.
	if redisErr != nil {
		return redisErr
	}
	if followerErr != nil {
		return followerErr
	}
	return followeeErr
}

// Implements SocialGraphService interface
// Tomislav-RetCtx: SocialGraphHandler::FollowWithUsername, lines 824-896.
func (s *SocialGraphServiceImpl) FollowWithUsername(ctx context.Context, reqID int64, userUsername string, followeeUsername string) error {
	userID, followeeID, err := s.userIDs(ctx, reqID, userUsername, followeeUsername)
	if err != nil {
		return err
	}
	if userID >= 0 && followeeID >= 0 { // lines 892-894
		return s.Follow(ctx, reqID, userID, followeeID)
	}
	return nil
}

// Implements SocialGraphService interface
// Tomislav-RetCtx: SocialGraphHandler::UnfollowWithUsername, lines 898-973.
func (s *SocialGraphServiceImpl) UnfollowWithUsername(ctx context.Context, reqID int64, userUsername string, followeeUsername string) error {
	userID, followeeID, err := s.userIDs(ctx, reqID, userUsername, followeeUsername)
	if err != nil {
		return err
	}
	if userID >= 0 && followeeID >= 0 { // lines 965-971
		return s.Unfollow(ctx, reqID, userID, followeeID)
	}
	return nil
}

// Tomislav-RetCtx: lines 838-890 (and 911-963): both user ids are requested concurrently; the user's is joined first.
func (s *SocialGraphServiceImpl) userIDs(ctx context.Context, reqID int64, userUsername string, followeeUsername string) (int64, int64, error) {
	var userID, followeeID int64
	var userErr, followeeErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // lines 838-857
		defer wg.Done()
		userID, userErr = s.userService.GetUserId(ctx, reqID, userUsername)
	}()
	go func() { // lines 859-880
		defer wg.Done()
		followeeID, followeeErr = s.userService.GetUserId(ctx, reqID, followeeUsername)
	}()
	wg.Wait()
	if userErr != nil { // line 885
		return 0, 0, userErr
	}
	return userID, followeeID, followeeErr // line 886
}

// Implements SocialGraphService interface
// Tomislav-RetCtx: SocialGraphHandler::InsertUser, lines 768-822.
func (s *SocialGraphServiceImpl) InsertUser(ctx context.Context, reqID int64, userID int64) error {
	collection, err := s.socialGraphDB.GetCollection(ctx, "social-graph", "social-graph") // lines 788-796
	if err != nil {
		return err
	}
	doc := bson.D{{Key: "user_id", Value: userID}, {Key: "followers", Value: bson.A{}}, {Key: "followees", Value: bson.A{}}} // lines 798-799
	if err := collection.InsertOne(ctx, doc); err != nil {                                                                   // lines 804-816
		return errors.New("Failed to insert social graph for user " + strconv.FormatInt(userID, 10) + " to MongoDB: " + err.Error())
	}
	return nil
}
