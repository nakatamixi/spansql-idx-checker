package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/spanner/spansql"
	"github.com/nktks/spansql-idx-checker/checker"
	"github.com/nktks/spansql-idx-checker/query"
)

var (
	schema    = flag.String("s", "", "path to input schama")
	squery    = flag.String("q", "", "query or dml")
	failOnErr = flag.Bool("fail-on-err", false, "exit if parse failed")
)

func main() {
	flag.Parse()
	if *schema == "" {
		log.Fatal("need schema file.")
	}
	ddls, err := read(*schema)
	if err != nil {
		log.Fatal(err)
	}
	if *squery == "" {
		b, err := readStdin()
		if err != nil {
			log.Fatalf("Read from stdin failed: %v", err)
		}
		*squery = string(b)
	}
	*squery = normalize(*squery)
	if *squery == "" {
		return
	}

	// spansql not allow backquote
	ddls = strings.Replace(ddls, "`", "", -1)
	d, err := spansql.ParseDDL(*schema, ddls)
	if err != nil {
		printOrFatal(err, *failOnErr)
		return
	}
	checker := checker.NewChecker(d)
	q, err := query.NewQuery(*squery)
	if err != nil {
		printOrFatal(err, *failOnErr)
		return
	}
	found, err := checker.Check(q)
	if err != nil {
		printOrFatal(err, *failOnErr)
		return
	}
	if !found {
		printOrFatal(fmt.Errorf("%s where clause does not incluse pk or secondary index first key", *squery), *failOnErr)
		return
	}
}
func read(file string) (string, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}
	body := string(data)
	return body, nil

}
func readStdin() ([]byte, error) {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return []byte{}, err
	}
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return []byte{}, err
		}
		return b, nil
	} else {
		return []byte{}, nil
	}
}

func normalize(s string) string {
	s = strings.Replace(s, "\n", " ", -1)
	s = strings.TrimLeft(s, " ")
	return s
}

func printOrFatal(err error, failOnErr bool) {
	if failOnErr {
		log.Fatal(err)
	} else {
		fmt.Println(err)
	}
}
