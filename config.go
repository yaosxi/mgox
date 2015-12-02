package mgox

import (
	"strings"
	"os"
	"bufio"
	"io"
	"github.com/alecthomas/log4go"
)


type dbconfig struct {
	host string
	database string
	username string
	password string
}

var DBConfig dbconfig

type PropertyReader struct {
	m map[string]string
}

func (p *PropertyReader) init(path string) {

	p.m = make(map[string]string)

	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		s := strings.TrimSpace(string(b))

		//log.Println(s)

		if strings.Index(s, "#") == 0 {
			continue
		}

		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}

		frist := strings.TrimSpace(s[:index])
		if len(frist) == 0 {
			continue
		}
		second := strings.TrimSpace(s[index+1:])

		pos := strings.Index(second, "\t#")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " #")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, "\t//")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " //")
		if pos > -1 {
			second = second[0:pos]
		}

		if len(second) == 0 {
			continue
		}

		p.m[frist] = strings.TrimSpace(second)
	}
}

func (c PropertyReader) Read(key string) string {
	v, found := c.m[key]
	if !found {
		return ""
	}
	return v
}

func init()  {
	db := new(PropertyReader)
	db.init("mgox.properties")
	log4go.Debug(db.m)
	DBConfig.host = db.m["host"]
	DBConfig.database = db.m["database"]
	DBConfig.username = db.m["username"]
	DBConfig.password = db.m["password"]
}