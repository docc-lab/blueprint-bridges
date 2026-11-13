package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/UserService/UserHandler.h unless another file is named. As upstream, the user service and the
// social-graph service call each other (InsertUser one way, GetUserId the other). The Blueprint port had moved GetUserId
// into a separate userid service on the belief that Blueprint cannot wire that cycle; it can when the services talk over
// RPC (the server side of an RPC pointer is built lazily behind its address; checked 2026-11-06 with a two-service
// ping<->pong application), so GetUserId is back in the user service and the extra service is gone.

import (
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"context"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	jwt "github.com/dgrijalva/jwt-go"
	"go.mongodb.org/mongo-driver/bson"
)

type UserService interface {
	Login(ctx context.Context, reqID int64, username string, password string) (string, error)
	RegisterUserWithId(ctx context.Context, reqID int64, firstName string, lastName string, username string, password string, userID int64) error
	RegisterUser(ctx context.Context, reqID int64, firstName string, lastName string, username string, password string) error
	ComposeCreatorWithUserId(ctx context.Context, reqID int64, userID int64, username string) (Creator, error)
	ComposeCreatorWithUsername(ctx context.Context, reqID int64, username string) (Creator, error)
	GetUserId(ctx context.Context, reqID int64, username string) (int64, error)
}

// Tomislav-RetCtx: lines 94-100 (fields of UserHandler); the counter state is upstream's static current_timestamp and
// counter (lines 37-38), guarded by _thread_lock.
type UserServiceImpl struct {
	machine_id         string
	counter            int64
	current_timestamp  int64
	lock               sync.Mutex
	secret             string
	userCache          backend.Cache
	userDB             backend.NoSQLDatabase
	socialGraphService SocialGraphService
}

// Tomislav-RetCtx: the "<username>:login" value upstream caches (login_json, lines 676-678, dumped at line 733). nlohmann
// json dumps object keys in sorted order without spaces; these fields are declared in that order.
type LoginObj struct {
	Password string `json:"password"`
	Salt     string `json:"salt"`
	UserID   int64  `json:"user_id"`
}

// Tomislav-RetCtx: UserHandler::UserHandler (lines 103-115); the index is created in UserService.cpp (line 78, unique
// user.user_id), retried until it exists.
func NewUserServiceImpl(ctx context.Context, userCache backend.Cache, userDB backend.NoSQLDatabase, socialGraphService SocialGraphService, secret string) (UserService, error) {
	if err := ensureIndex(userDB, "user", "user", "user_id", true); err != nil {
		return nil, err
	}
	return &UserServiceImpl{counter: 0, current_timestamp: -1, machine_id: GetMachineID(), userCache: userCache, userDB: userDB, socialGraphService: socialGraphService, secret: secret}, nil
}

// Tomislav-RetCtx: GetCounter, lines 40-52 (a clock step backwards is fatal, as upstream).
func (u *UserServiceImpl) getCounter(timestamp int64) int64 {
	if u.current_timestamp > timestamp {
		log.Fatalf("Timestamps are not incremental.")
	}
	if u.current_timestamp == timestamp {
		retVal := u.counter
		u.counter += 1
		return retVal
	}
	u.current_timestamp = timestamp
	u.counter = 1
	return 0
}

// Tomislav-RetCtx: GenRandomString, lines 54-68: 0-9A-Za-z, from a generator freshly seeded from the system's random
// device on every call (std::random_device + std::mt19937).
func genRandomString(length int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var seed [8]byte
	crand.Read(seed[:])
	gen := rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(seed[:]))))
	s := make([]byte, length)
	for i := range s {
		s[i] = alphanum[gen.Intn(len(alphanum))]
	}
	return string(s)
}

// Tomislav-RetCtx: picosha2::hash256_hex_string (lines 180, 328, 700): lowercase hex SHA-256.
func hashPassword(passwordAndSalt string) string {
	sum := sha256.Sum256([]byte(passwordAndSalt))
	return hex.EncodeToString(sum[:])
}

// Tomislav-RetCtx: UserHandler::Login, lines 559-748.
func (u *UserServiceImpl) Login(ctx context.Context, reqID int64, username string, password string) (string, error) {
	// Tomislav-RetCtx: lines 575-608: memcached get "<username>:login" ({password, salt, user_id}).
	var login LoginObj
	cached, err := u.userCache.Get(ctx, username+":login", &login)
	if err != nil {
		cached = false // line 587-589: a memcached error is logged, then MongoDB is asked
	}
	passwordStored, saltStored, userIDStored := "", "", int64(-1)
	if cached {
		passwordStored, saltStored, userIDStored = login.Password, login.Salt, login.UserID // lines 603-607
	} else {
		// Tomislav-RetCtx: lines 611-695: MongoDB user.user find {username}; read password, salt, user_id.
		collection, err := u.userDB.GetCollection(ctx, "user", "user")
		if err != nil {
			return "", err
		}
		res, err := collection.FindOne(ctx, bson.D{{Key: "username", Value: username}}) // lines 631-637
		if err != nil {
			return "", err
		}
		var user User
		found, err := res.One(ctx, &user) // line 639
		if err != nil {
			return "", err
		}
		if !found { // lines 655-664
			return "", errors.New("User: " + username + " is not registered")
		}
		passwordStored, saltStored, userIDStored = user.PwdHashed, user.Salt, user.UserID  // lines 670-675
		login = LoginObj{Password: passwordStored, Salt: saltStored, UserID: userIDStored} // lines 676-678
	}

	// Tomislav-RetCtx: lines 697-723: check the password and sign the token.
	var token string
	if userIDStored != -1 && saltStored != "" && passwordStored != "" {
		if hashPassword(password+saltStored) == passwordStored { // lines 699-700
			claims := jwt.MapClaims{ // lines 702-710: HS256, payload user_id, username, timestamp (seconds), ttl 3600
				"user_id":   strconv.FormatInt(userIDStored, 10),
				"username":  username,
				"timestamp": strconv.FormatInt(time.Now().Unix(), 10),
				"ttl":       "3600",
			}
			token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(u.secret)) // line 711
			if err != nil {
				return "", err
			}
		} else { // lines 712-717
			return "", errors.New("Incorrect username or password")
		}
	} else { // lines 718-723
		return "", errors.New("Username: " + username + " incomplete login information.")
	}

	// Tomislav-RetCtx: lines 725-746: only a value read from MongoDB is written to memcached.
	if !cached {
		u.userCache.Put(ctx, username+":login", login) // lines 733-737 (a failure is only logged, lines 739-743)
	}
	return token, nil
}

// Tomislav-RetCtx: UserHandler::ComposeCreatorWithUserId, lines 538-557.
func (u *UserServiceImpl) ComposeCreatorWithUserId(ctx context.Context, reqID int64, userID int64, username string) (Creator, error) {
	return Creator{UserID: userID, Username: username}, nil // lines 547-551
}

// Tomislav-RetCtx: UserHandler::ComposeCreatorWithUsername, lines 380-536.
func (u *UserServiceImpl) ComposeCreatorWithUsername(ctx context.Context, reqID int64, username string) (Creator, error) {
	// Tomislav-RetCtx: lines 395-425: memcached get "<username>:user_id".
	userID := int64(-1)
	cached, err := u.userCache.Get(ctx, username+":user_id", &userID)
	if err != nil {
		cached = false // lines 406-413: logged, then MongoDB
	}
	if !cached {
		// Tomislav-RetCtx: lines 427-502: MongoDB user.user find {username}; read user_id.
		collection, err := u.userDB.GetCollection(ctx, "user", "user")
		if err != nil {
			return Creator{}, err
		}
		res, err := collection.FindOne(ctx, bson.D{{Key: "username", Value: username}}) // lines 446-452
		if err != nil {
			return Creator{}, err
		}
		var user User
		found, err := res.One(ctx, &user) // line 454
		if err != nil {
			return Creator{}, err
		}
		if !found { // lines 456-478
			return Creator{}, errors.New("User: " + username + " is not registered")
		}
		userID = user.UserID // lines 480-483
	}
	creator := Creator{Username: username, UserID: userID} // lines 504-506
	if userID != -1 && !cached {                           // lines 512-530: write back only a value read from MongoDB
		u.userCache.Put(ctx, username+":user_id", userID)
	}
	return creator, nil // line 509
}

// Tomislav-RetCtx: UserHandler::RegisterUserWithId, lines 117-230.
func (u *UserServiceImpl) RegisterUserWithId(ctx context.Context, reqID int64, firstName string, lastName string, username string, password string, userID int64) error {
	return u.registerUser(ctx, reqID, firstName, lastName, username, password, userID)
}

// Tomislav-RetCtx: UserHandler::RegisterUser, lines 232-378: the user id is composed like a post id (lines 246-277),
// then the user is stored exactly as RegisterUserWithId stores it (lines 280-376 repeat lines 132-228).
func (u *UserServiceImpl) RegisterUser(ctx context.Context, reqID int64, firstName string, lastName string, username string, password string) error {
	u.lock.Lock() // lines 247-253
	timestamp := time.Now().UnixMilli() - customEpoch
	idx := u.getCounter(timestamp)
	u.lock.Unlock()
	userIDStr := u.machine_id + lastHex(timestamp, 10) + lastHex(idx, 3) // lines 255-275
	userID, err := strconv.ParseUint(userIDStr, 16, 64)                  // line 276
	if err != nil {
		return err
	}
	return u.registerUser(ctx, reqID, firstName, lastName, username, password, int64(userID&0x7FFFFFFFFFFFFFFF))
}

// Tomislav-RetCtx: the body shared by RegisterUserWithId (lines 132-228) and RegisterUser (lines 280-376).
func (u *UserServiceImpl) registerUser(ctx context.Context, reqID int64, firstName string, lastName string, username string, password string, userID int64) error {
	collection, err := u.userDB.GetCollection(ctx, "user", "user") // lines 133-143
	if err != nil {
		return err
	}
	res, err := collection.FindOne(ctx, bson.D{{Key: "username", Value: username}}) // lines 144-148
	if err != nil {
		return err
	}
	var existing User
	found, err := res.One(ctx, &existing) // line 151
	if err != nil {
		return err
	}
	if found { // lines 162-171
		return errors.New("User " + username + " already existed")
	}
	salt := genRandomString(32) // line 178
	user := User{               // lines 173-181: user_id, first_name, last_name, username, salt, password (sha256 hex of password+salt)
		UserID:    userID,
		FirstName: firstName,
		LastName:  lastName,
		Username:  username,
		Salt:      salt,
		PwdHashed: hashPassword(password + salt),
	}
	if err := collection.InsertOne(ctx, user); err != nil { // lines 186-198
		return errors.New("Failed to insert user " + username + " to MongoDB: " + err.Error())
	}
	return u.socialGraphService.InsertUser(ctx, reqID, userID) // lines 210-227
}

// Tomislav-RetCtx: UserHandler::GetUserId, lines 749-897.
func (u *UserServiceImpl) GetUserId(ctx context.Context, reqID int64, username string) (int64, error) {
	// Tomislav-RetCtx: lines 764-795: memcached get "<username>:user_id" (the value is the decimal id).
	userID := int64(-1)
	cached, err := u.userCache.Get(ctx, username+":user_id", &userID)
	if err != nil {
		cached = false // lines 776-783: logged, then MongoDB
	}
	if !cached {
		// Tomislav-RetCtx: lines 796-869: MongoDB user.user find {username}; read user_id.
		collection, err := u.userDB.GetCollection(ctx, "user", "user")
		if err != nil {
			return -1, err
		}
		res, err := collection.FindOne(ctx, bson.D{{Key: "username", Value: username}}) // lines 814-820
		if err != nil {
			return -1, err
		}
		var user User
		found, err := res.One(ctx, &user) // line 822
		if err != nil {
			return -1, err
		}
		if !found { // lines 824-846
			return -1, errors.New("User: " + username + " is not registered")
		}
		userID = user.UserID // lines 848-851
	}
	if !cached { // lines 872-893: write back only a value read from MongoDB (a failure is only logged)
		u.userCache.Put(ctx, username+":user_id", userID)
	}
	return userID, nil // line 896
}
