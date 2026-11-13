package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/PostStorageService/PostStorageHandler.h unless another file is named. Posts live in MongoDB
// post.post and in post-storage-memcached, keyed by the decimal post id; the cached value is the MongoDB document as
// libbson's legacy JSON text (bson_as_json, lines 292 and 531), reproduced byte for byte by legacyJSON (common.go).

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strconv"
	"sync"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/memcached"
	"go.mongodb.org/mongo-driver/bson"
)

// The PostStorageService interface
type PostStorageService interface {
	// Stores a new `post` in the relevant backends.
	StorePost(ctx context.Context, reqID int64, post Post) error
	// Returns the post with id `postID`
	// If no post with id `postID` exists in the database, an error is returned.
	ReadPost(ctx context.Context, reqID int64, postID int64) (Post, error)
	// Returns a list of posts that have ids in the array `postIDs`.
	// Tomislav-RetCtx: as upstream: duplicated ids and posts that cannot be found are errors (lines 372-377, 605-618).
	ReadPosts(ctx context.Context, reqID int64, postIDs []int64) ([]Post, error)
}

// Implementation of [PostStorageService]
type PostStorageServiceImpl struct {
	postStorageCache backend.Cache
	postStorageDB    backend.NoSQLDatabase
}

// Creates a [PostStorageService] instance that manages the post backends.
// Tomislav-RetCtx: PostStorageHandler::PostStorageHandler (lines 41-46); the index is created in PostStorageService.cpp
// (line 65, unique post.post_id), retried until it exists.
func NewPostStorageServiceImpl(ctx context.Context, postStorageCache backend.Cache, postStorageDB backend.NoSQLDatabase) (PostStorageService, error) {
	p := &PostStorageServiceImpl{postStorageCache: postStorageCache, postStorageDB: postStorageDB}
	if err := ensureIndex(postStorageDB, "post", "post", "post_id", true); err != nil {
		return nil, err
	}
	return p, nil
}

// Implements PostStorageService interface
// Tomislav-RetCtx: PostStorageHandler::StorePost, lines 48-162: one MongoDB insert of the post document (lines 79-135:
// post_id, timestamp, text, req_id, post_type, creator, urls, user_mentions, media; the three arrays are always present,
// empty when there is nothing in them).
func (p *PostStorageServiceImpl) StorePost(ctx context.Context, reqID int64, post Post) error {
	collection, err := p.postStorageDB.GetCollection(ctx, "post", "post") // lines 64-77
	if err != nil {
		return err
	}
	if post.Urls == nil {
		post.Urls = []URL{}
	}
	if post.UserMentions == nil {
		post.UserMentions = []UserMention{}
	}
	if post.Medias == nil {
		post.Medias = []Media{}
	}
	if err := collection.InsertOne(ctx, post); err != nil { // lines 139-155
		return errors.New("Failed to insert post to MongoDB: " + err.Error())
	}
	return nil
}

// Tomislav-RetCtx: the memcached plugin's raw calls (PutRaw, GetRaw, MgetRaw, outside backend.Cache): upstream stores and
// reads the post JSON text byte for byte (memcached_set / memcached_get / memcached_mget). Reached through the concrete
// client type, as the Redis calls are: an interface declared in this package would be matched by Blueprint's workflow
// parser as the memcached client's service interface (it picks interfaces the client struct implements).
func (p *PostStorageServiceImpl) raw() (*memcached.Memcached, error) {
	cache, ok := p.postStorageCache.(*memcached.Memcached)
	if !ok {
		return nil, errors.New("snbounded: the post cache needs memcached, as upstream's post-storage-memcached")
	}
	return cache, nil
}

// Tomislav-RetCtx: a post as upstream parses it from cached JSON (lines 207-233, 441-469): the named fields; "_id" is
// ignored.
func postFromJSON(text []byte) (Post, error) {
	var post Post
	err := json.Unmarshal(text, &post)
	return post, err
}

// Implements PostStorageService interface
// Tomislav-RetCtx: PostStorageHandler::ReadPost, lines 164-352.
func (p *PostStorageServiceImpl) ReadPost(ctx context.Context, reqID int64, postID int64) (Post, error) {
	postIDStr := strconv.FormatInt(postID, 10) // line 176
	// Tomislav-RetCtx: lines 178-204: memcached get "<post_id>".
	cache, err := p.raw()
	if err != nil {
		return Post{}, err
	}
	cached, found, err := cache.GetRaw(ctx, postIDStr)
	if err != nil {
		found = false // lines 195-203: logged, then MongoDB
	}
	if found { // lines 205-234
		return postFromJSON(cached)
	}
	// Tomislav-RetCtx: lines 235-347: MongoDB post.post find {post_id}; the document's JSON goes to memcached.
	collection, err := p.postStorageDB.GetCollection(ctx, "post", "post") // lines 237-255
	if err != nil {
		return Post{}, err
	}
	res, err := collection.FindOne(ctx, bson.D{{Key: "post_id", Value: postID}}) // lines 256-262
	if err != nil {
		return Post{}, err
	}
	var raw bson.Raw
	found, err = res.One(ctx, &raw) // line 264
	if err != nil {
		return Post{}, err
	}
	if !found { // lines 266-289
		return Post{}, errors.New("Post_id: " + postIDStr + " doesn't exist in MongoDB")
	}
	text, err := legacyJSON(raw) // line 292 (bson_as_json)
	if err != nil {
		return Post{}, err
	}
	post, err := postFromJSON(text) // lines 293-322
	if err != nil {
		return Post{}, err
	}
	cache.PutRaw(ctx, postIDStr, text) // lines 325-345 (a failure is only logged)
	return post, nil
}

// Implements PostStorageService interface
// Tomislav-RetCtx: PostStorageHandler::ReadPosts, lines 353-631.
func (p *PostStorageServiceImpl) ReadPosts(ctx context.Context, reqID int64, postIDs []int64) ([]Post, error) {
	if len(postIDs) == 0 { // lines 367-369
		return []Post{}, nil
	}
	notCached := make(map[int64]bool, len(postIDs)) // line 371 (std::set)
	for _, id := range postIDs {
		notCached[id] = true
	}
	if len(notCached) != len(postIDs) { // lines 372-377
		return nil, errors.New("Post_ids are duplicated")
	}

	// Tomislav-RetCtx: lines 379-479: one memcached multi-get of every id (keys = decimal ids, in request order).
	cache, err := p.raw()
	if err != nil {
		return nil, err
	}
	returnMap := make(map[int64]Post, len(postIDs)) // line 379
	keys := make([]string, len(postIDs))
	for i, id := range postIDs { // lines 395-401
		keys[i] = strconv.FormatInt(id, 10)
	}
	values, err := cache.MgetRaw(ctx, keys) // lines 402-413
	if err != nil {
		return nil, err
	}
	for _, value := range values { // lines 422-471: every value fetched
		if value == nil {
			continue
		}
		post, err := postFromJSON(value) // lines 440-469
		if err != nil {
			return nil, err
		}
		returnMap[post.PostID] = post
		delete(notCached, post.PostID)
	}

	// Tomislav-RetCtx: lines 481-603: the rest from MongoDB with one find {post_id: {$in: [...]}} (ids in ascending
	// order, std::set iteration); each document's JSON is written to memcached afterwards, sequentially, on one client,
	// in a concurrent task (lines 581-602).
	var setDone sync.WaitGroup
	if len(notCached) > 0 { // line 485
		ids := make([]int64, 0, len(notCached))
		for id := range notCached {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		collection, err := p.postStorageDB.GetCollection(ctx, "post", "post") // lines 487-502
		if err != nil {
			return nil, err
		}
		in := bson.A{}
		for _, id := range ids { // lines 503-518
			in = append(in, id)
		}
		cursor, err := collection.FindMany(ctx, bson.D{{Key: "post_id", Value: bson.D{{Key: "$in", Value: in}}}}) // lines 519-520
		if err != nil {
			return nil, err
		}
		var docs []bson.Raw
		if err := cursor.All(ctx, &docs); err != nil { // lines 525-575 (cursor errors are rethrown, lines 563-575)
			return nil, err
		}
		jsonByID := make(map[int64][]byte, len(docs)) // line 482 (std::map: ascending ids)
		for _, raw := range docs {
			text, err := legacyJSON(raw) // line 531
			if err != nil {
				return nil, err
			}
			post, err := postFromJSON(text) // lines 532-557
			if err != nil {
				return nil, err
			}
			jsonByID[post.PostID] = text  // line 558
			returnMap[post.PostID] = post // line 559
		}
		setDone.Add(1)
		go func() { // lines 581-602
			defer setDone.Done()
			setIDs := make([]int64, 0, len(jsonByID))
			for id := range jsonByID {
				setIDs = append(setIDs, id)
			}
			sort.Slice(setIDs, func(i, j int) bool { return setIDs[i] < setIDs[j] })
			for _, id := range setIDs { // lines 594-601 (failures are only logged)
				if err := cache.PutRaw(context.Background(), strconv.FormatInt(id, 10), jsonByID[id]); err != nil {
					log.Printf("ReadPosts: memcached set of post %d failed: %v", id, err)
				}
			}
		}()
	}

	if len(returnMap) != len(postIDs) { // lines 605-618
		setDone.Wait()
		return nil, errors.New("Return set incomplete")
	}
	posts := make([]Post, 0, len(postIDs))
	for _, id := range postIDs { // lines 620-622: in request order
		posts = append(posts, returnMap[id])
	}
	setDone.Wait() // lines 624-629
	return posts, nil
}
