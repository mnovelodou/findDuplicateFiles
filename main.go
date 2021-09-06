package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const goRoutines = 16

var directory = flag.String("dir", "/Volumes/NO NAME/Musica", "")

func main() {
	flag.Parse()

	start := time.Now().UnixNano()
	defer func() {
		fmt.Println("Duration: ", time.Now().UnixNano() - start)
	}()

	fileinfo, err := os.Stat(*directory)
	paths := make(map[int64][]string)
	count := 0

	if os.IsNotExist(err) {
		panic(fmt.Errorf("%v is not a file", *directory))
	}
	if !fileinfo.IsDir() {
		panic(fmt.Errorf("%v is not a directory", *directory))
	}

	err = filepath.Walk(*directory, func(path string, info os.FileInfo, er error) error {
		if info.IsDir() {
			return nil
		}

		paths[info.Size()] = append(paths[info.Size()], path)
		count++
		return nil
	})

	if err != nil {
		panic(err)
	}
	fmt.Printf("Same size are %d, Total is %d\n", len(paths), count)

	removed := 0
	ch := make(chan []string, goRoutines)
	wg := sync.WaitGroup{}
	mux := sync.Mutex{}
	wg.Add(goRoutines)

	for i := 0; i < goRoutines; i++ {
		go func() {
			defer wg.Done()
			for list := range ch {
				hashes := make(map[string]struct{})

				for _, file := range list {
					hash, err := hashFileMD5(file)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Could not calculate MD5 for file: %s\n%s\n", file, err.Error())
						continue
					}

					if _, ok := hashes[hash]; ok {
						os.Remove(file)
						mux.Lock()
						removed++
						mux.Unlock()
					} else {
						hashes[hash] = struct{}{}
					}
				}
			}
		}()
	}

	for _, list := range paths {
		if len(list) == 1 {
			continue
		}

		ch <- list
	}

	close(ch)

	wg.Wait()
	fmt.Println("Removed files: ", removed)
}

func hashFileMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}

	defer file.Close()
	hash := md5.New()

	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)[:16]), nil
}