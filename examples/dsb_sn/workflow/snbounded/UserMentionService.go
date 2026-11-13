package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/UserMentionService/UserMentionHandler.h. The cache is user-memcached (shared with the user service),
// key "<username>:user_id", value the decimal user id.

import (
	"context"
	"sort"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.mongodb.org/mongo-driver/bson"
)

// The UserMentionService interface
type UserMentionService interface {
	// Composes a list of usermentions from the provided usernames.
	ComposeUserMentions(ctx context.Context, reqID int64, usernames []string) ([]UserMention, error)
}

// Implementation of [UserMentionService]
type UserMentionServiceImpl struct {
	userCache backend.Cache
	userDB    backend.NoSQLDatabase
}

// Creates a [UserMentionService] instance that looks up the user ids of the mentioned usernames.
// Tomislav-RetCtx: UserMentionHandler::UserMentionHandler (lines 32-37).
func NewUserMentionServiceImpl(ctx context.Context, userCache backend.Cache, userDB backend.NoSQLDatabase) (UserMentionService, error) {
	return &UserMentionServiceImpl{userCache: userCache, userDB: userDB}, nil
}

// Implements the UserMentionService interface
// Tomislav-RetCtx: UserMentionHandler::ComposeUserMentions, lines 39-231.
func (u *UserMentionServiceImpl) ComposeUserMentions(ctx context.Context, reqID int64, usernames []string) ([]UserMention, error) {
	var userMentions []UserMention
	if len(usernames) == 0 { // line 54: nothing at all is done for a post without mentions
		return userMentions, nil
	}
	notCached := make(map[string]bool, len(usernames)) // lines 55-59 (std::map<std::string, bool>)
	for _, name := range usernames {
		notCached[name] = true
	}

	// Tomislav-RetCtx: lines 61-143: one memcached multi-get of "<username>:user_id" for every username, in request
	// order; each value returned becomes a mention (memcached returns a value per requested key, duplicates included).
	keys := make([]string, len(usernames))
	values := make([]int64, len(usernames))
	found := make([]bool, len(usernames))
	targets := make([]interface{}, len(usernames))
	for i, name := range usernames { // lines 74-83
		keys[i] = name + ":user_id"
		values[i] = -1
		targets[i] = &values[i]
	}
	if err := u.userCache.Mget(ctx, keys, targets); err == nil { // lines 86-96 (an error is logged, MongoDB is asked)
		for i := range keys {
			found[i] = values[i] != -1
		}
	}
	for i, name := range usernames { // lines 104-136
		if found[i] {
			userMentions = append(userMentions, UserMention{Username: name, UserID: values[i]}) // lines 125-132
			delete(notCached, name)                                                             // line 133
		}
	}

	// Tomislav-RetCtx: lines 145-227: the rest from MongoDB user.user, one find {username: {$in: [...]}} (usernames in
	// std::map order, i.e. sorted); every document returned becomes a mention. Nothing is written back to memcached.
	if len(notCached) > 0 { // line 146
		names := make([]string, 0, len(notCached))
		for name := range notCached {
			names = append(names, name)
		}
		sort.Strings(names)
		collection, err := u.userDB.GetCollection(ctx, "user", "user") // lines 147-165
		if err != nil {
			return userMentions, err
		}
		in := bson.A{}
		for _, name := range names { // lines 166-180
			in = append(in, name)
		}
		cursor, err := collection.FindMany(ctx, bson.D{{Key: "username", Value: bson.D{{Key: "$in", Value: in}}}}) // lines 186-187
		if err != nil {
			return userMentions, err
		}
		var users []User
		if err := cursor.All(ctx, &users); err != nil { // lines 190-220
			return userMentions, err
		}
		for _, user := range users {
			userMentions = append(userMentions, UserMention{UserID: user.UserID, Username: user.Username}) // lines 193-219
		}
	}
	return userMentions, nil // line 229
}
