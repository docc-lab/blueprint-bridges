package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/UrlShortenService/UrlShortenHandler.h. Upstream's handler also receives url-shorten-memcached, but
// only GetExtendedUrls would use it and upstream leaves that unimplemented (lines 169-176), so no cache is wired here.

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
)

// The UrlShortenService interface
type UrlShortenService interface {
	// Converts raw `urls` into shortened urls to be used within the application. Returns the list of shortened urls.
	ComposeUrls(ctx context.Context, reqID int64, urls []string) ([]URL, error)
	// Converts the list of shortened urls into their extended forms.
	GetExtendedUrls(ctx context.Context, reqID int64, shortenedUrls []string) ([]string, error)
}

// Implementation of [UrlShortenService]
type UrlShortenServiceImpl struct {
	urlShortenDB backend.NoSQLDatabase
	hostname     string
	lock         sync.Mutex // Tomislav-RetCtx: upstream _thread_lock around the generator (lines 61-65)
	generator    *rand.Rand // Tomislav-RetCtx: upstream's static std::mt19937 seeded with the start-up time (lines 44-45)
}

// Creates a [UrlShortenService] instance for converting raw urls to shortened urls and vice versa.
// Tomislav-RetCtx: UrlShortenHandler::UrlShortenHandler (lines 47-55); the index is created in UrlShortenService.cpp
// (line 63, unique url-shorten.shortened_url), retried until it exists.
func NewUrlShortenServiceImpl(ctx context.Context, urlShortenDB backend.NoSQLDatabase) (UrlShortenService, error) {
	if err := ensureIndex(urlShortenDB, "url-shorten", "url-shorten", "shortened_url", true); err != nil {
		return nil, err
	}
	return &UrlShortenServiceImpl{urlShortenDB: urlShortenDB, hostname: "http://short-url/", // HOSTNAME, line 18
		generator: rand.New(rand.NewSource(time.Now().UnixMilli() % 0xffffffff))}, nil
}

// Tomislav-RetCtx: UrlShortenHandler::_GenRandomStr, lines 57-67: a-zA-Z0-9 (62 characters), uniform, under the lock.
func (u *UrlShortenServiceImpl) genRandomStr(length int) string {
	const charMap = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" // lines 58-59
	b := make([]byte, length)
	u.lock.Lock()
	for i := range b {
		b[i] = charMap[u.generator.Intn(62)] // line 63 (uniform_int_distribution(0, 61), line 54)
	}
	u.lock.Unlock()
	return string(b)
}

// Implements ComposeUrls interface
// Tomislav-RetCtx: UrlShortenHandler::ComposeUrls, lines 68-167.
func (u *UrlShortenServiceImpl) ComposeUrls(ctx context.Context, reqID int64, urls []string) ([]URL, error) {
	var targetURLs []URL
	if len(urls) == 0 { // line 87: nothing is stored for a post without URLs
		return targetURLs, nil
	}
	docs := make([]interface{}, 0, len(urls))
	for _, url := range urls { // lines 88-94: HOSTNAME + 10 random characters per URL
		target := URL{ShortenedUrl: u.hostname + u.genRandomStr(10), ExpandedUrl: url}
		targetURLs = append(targetURLs, target)
		docs = append(docs, target) // lines 127-132: {shortened_url, expanded_url}
	}
	// Tomislav-RetCtx: lines 96-153: one bulk insert of all documents (mongoc_bulk_operation_execute, line 134), run as
	// a future that is joined right away (lines 155-162).
	collection, err := u.urlShortenDB.GetCollection(ctx, "url-shorten", "url-shorten") // lines 106-115
	if err != nil {
		return []URL{}, err
	}
	if err := collection.InsertMany(ctx, docs); err != nil { // lines 125-146
		return []URL{}, err
	}
	return targetURLs, nil // line 164
}

// Implements UrlShortenService interface.
// Tomislav-RetCtx: UrlShortenHandler::GetExtendedUrls, lines 169-176: upstream returns without doing anything.
func (u *UrlShortenServiceImpl) GetExtendedUrls(ctx context.Context, reqID int64, shortenedUrls []string) ([]string, error) {
	return []string{}, nil
}
