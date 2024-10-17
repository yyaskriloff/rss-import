package main

import (
	"fmt"
	"log"

	"github.com/SlyMarbo/rss"
)

func main() {
	feed, err := rss.Fetch("https://anchor.fm/s/5772c9f8/podcast/rss")
	if err != nil {
		log.Fatal(err)
	}

	feedItems := feed.Items
	fmt.Printf("Got podcast feed (%s) with %d episodes\n", feed.Title, len(feedItems))

}
