package snbounded

// Tomislav-RetCtx: references are to DeathStarBench commit 6ecb097 (2024-06-27),
// socialNetwork/src/TextService/TextHandler.h. Upstream uses std::regex (ECMAScript); Go's regexp (RE2) finds the same
// matches for these two patterns (leftmost, non-overlapping, greedy character classes).

import (
	"context"
	"regexp"
	"sync"
)

// The TextService interface
type TextService interface {
	// Parses the raw `text` to return an edited text with the urls replaced with shortened urls, usermention objects to be stored with the post, and the url objects to be stored with the post.
	ComposeText(ctx context.Context, reqID int64, text string) (string, []UserMention, []URL, error)
}

// Implementation of [TextService]
type TextServiceImpl struct {
	urlShortenService  UrlShortenService
	userMentionService UserMentionService
}

// Creates a [TextService] instance for parsing texts in created posts.
// Tomislav-RetCtx: TextHandler::TextHandler (lines 33-39).
func NewTextServiceImpl(ctx context.Context, urlShortenService UrlShortenService, userMentionService UserMentionService) (TextService, error) {
	return &TextServiceImpl{urlShortenService: urlShortenService, userMentionService: userMentionService}, nil
}

// Implements TextService interface
// Tomislav-RetCtx: TextHandler::ComposeText, lines 41-169. The regexes are built on every call, as upstream's std::regex
// objects are (lines 55, 65).
func (t *TextServiceImpl) ComposeText(ctx context.Context, reqID int64, text string) (string, []UserMention, []URL, error) {
	mentionRe := regexp.MustCompile(`@[a-zA-Z0-9-_]+`) // line 55
	var usernames []string
	for _, m := range mentionRe.FindAllString(text, -1) { // lines 56-62: every @mention, without the '@'
		usernames = append(usernames, m[1:])
	}
	urlRe := regexp.MustCompile(`(http://|https://)([a-zA-Z0-9_!~*'().&=+$%-]+)`) // line 65
	urlStrings := urlRe.FindAllString(text, -1)                                   // lines 66-71

	// Tomislav-RetCtx: lines 73-132: URL shortening and user mentions are requested concurrently, both always (also
	// with empty lists); lines 134-148 join the URL future first.
	var urls []URL
	var userMentions []UserMention
	var urlErr, mentionErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // lines 73-99
		defer wg.Done()
		urls, urlErr = t.urlShortenService.ComposeUrls(ctx, reqID, urlStrings)
	}()
	go func() { // lines 101-132
		defer wg.Done()
		userMentions, mentionErr = t.userMentionService.ComposeUserMentions(ctx, reqID, usernames)
	}()
	wg.Wait()
	if urlErr != nil { // lines 134-140
		return text, userMentions, urls, urlErr
	}
	if mentionErr != nil { // lines 142-148
		return text, userMentions, urls, mentionErr
	}

	// Tomislav-RetCtx: lines 150-163: each URL is replaced by its shortened URL, match by match; upstream never appends
	// the text that follows the last URL, so that tail is dropped (kept as upstream does it).
	updatedText := ""
	if len(urlStrings) != 0 { // line 151
		prev := 0
		for idx, match := range urlRe.FindAllStringIndex(text, -1) { // lines 154-160
			updatedText += text[prev:match[0]] + urls[idx].ShortenedUrl // line 157
			prev = match[1]                                             // line 158
		}
	} else {
		updatedText = text // line 162
	}
	return updatedText, userMentions, urls, nil // lines 165-167
}
