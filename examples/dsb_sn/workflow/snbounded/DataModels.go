package snbounded

// Tomislav-RetCtx: field names and order follow upstream's stored documents and thrift structs (DeathStarBench commit
// 6ecb097, socialNetwork/gen-cpp/social_network_types.h from socialNetwork/social_network.thrift; the MongoDB documents
// are built in the handlers named below). bson tags name the MongoDB fields, json tags the fields of cached values and
// HTTP responses. Go's encoders write struct fields in declaration order, so the declaration order is upstream's
// field order.

// The format of a user stored in the user database
// Tomislav-RetCtx: UserService/UserHandler.h RegisterUserWithId lines 173-181: user_id, first_name, last_name, username,
// salt, password.
type User struct {
	UserID    int64  `bson:"user_id" json:"user_id"`
	FirstName string `bson:"first_name" json:"first_name"`
	LastName  string `bson:"last_name" json:"last_name"`
	Username  string `bson:"username" json:"username"`
	Salt      string `bson:"salt" json:"salt"`
	PwdHashed string `bson:"password" json:"password"`
}

// The format of a media stored as part of a post.
// Tomislav-RetCtx: PostStorageService/PostStorageHandler.h StorePost lines 131-132 (media_id, media_type).
type Media struct {
	MediaID   int64  `bson:"media_id" json:"media_id"`
	MediaType string `bson:"media_type" json:"media_type"`
}

// The format of a url stored in the url-shorten database
// Tomislav-RetCtx: UrlShortenService/UrlShortenHandler.h ComposeUrls lines 129-130 and PostStorageHandler.h StorePost
// lines 102-103 (shortened_url, expanded_url).
type URL struct {
	ShortenedUrl string `bson:"shortened_url" json:"shortened_url"`
	ExpandedUrl  string `bson:"expanded_url" json:"expanded_url"`
}

// The format of a usermention stored as part of a post
// Tomislav-RetCtx: PostStorageHandler.h StorePost lines 116-117 (user_id, username).
type UserMention struct {
	UserID   int64  `bson:"user_id" json:"user_id"`
	Username string `bson:"username" json:"username"`
}

// The format of a creator stored as part of a post
// Tomislav-RetCtx: PostStorageHandler.h StorePost lines 88-89 (user_id, username).
type Creator struct {
	UserID   int64  `bson:"user_id" json:"user_id"`
	Username string `bson:"username" json:"username"`
}

// The type of the post.
type PostType int64

// Enums aren't supported atm. So just use integers instead.
// Tomislav-RetCtx: social_network.thrift enum PostType { POST, REPOST, REPLY, DM }.
const (
	POST int64 = iota
	REPOST
	REPLY
	DM
)

// Tomislav-RetCtx: PostStorageHandler.h StorePost lines 80-132: post_id, timestamp, text, req_id, post_type, creator,
// urls, user_mentions, media. Upstream always writes the three arrays, empty when there is nothing in them (see
// StorePost in PostStorageService.go).
type Post struct {
	PostID       int64         `bson:"post_id" json:"post_id"`
	Timestamp    int64         `bson:"timestamp" json:"timestamp"`
	Text         string        `bson:"text" json:"text"`
	ReqID        int64         `bson:"req_id" json:"req_id"`
	PostType     int64         `bson:"post_type" json:"post_type"`
	Creator      Creator       `bson:"creator" json:"creator"`
	Urls         []URL         `bson:"urls" json:"urls"`
	UserMentions []UserMention `bson:"user_mentions" json:"user_mentions"`
	Medias       []Media       `bson:"media" json:"media"`
}
