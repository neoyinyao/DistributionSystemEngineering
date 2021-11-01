package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}
type SafeFetch struct{
	v map[string] bool
	mux sync.Mutex
}
func (s SafeFetch) Value(url string) bool{
	s.mux.Lock()
	defer s.mux.Unlock()
	_,ok := s.v[url]
	if ok{
		return ok
	}else {
		s.v[url] = true
		return ok
	}

}
// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.

func Worker(url string,ch chan []string,fetch Fetcher){
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	ch <- urls
	fmt.Printf("found: %s %q\n", url, body)
}

func Master(ch chan []string, fetcher Fetcher) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	visited := make(map[string]bool)
	for urls := range ch{
		for _,url := range urls{
			_,ok := visited[url]
			if !ok{
				visited[url] = true
				go Worker(url,ch,fetcher)
			}
		}
	}
	close(ch)
}

func main() {
	url := "https://golang.org/"
	ch := make(chan []string)
	go func(){
		ch <- []string{url}
	}()
	Master(ch,fetcher)
	//time.Sleep(time.Second)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}

