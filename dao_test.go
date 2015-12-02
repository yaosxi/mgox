package mgox_test

import (
	"gopkg.in/mgo.v2/bson"
	"testing"
	"github.com/yaosxi/mgox"
	"time"
	"github.com/alecthomas/log4go"
)

func handleError(t *testing.T, err error) bool {
	if err == nil {
		return false
	}
	panic(err)
	t.Fail()
	return true
}

var UserCollectionName = new(User)
type User struct {
	Id           bson.ObjectId              `json:"id" bson:"_id,omitempty"`
	Name         string                     `json:"name"`
	Age          int                        `json:"age"`
	Sex          int                        `json:"sex"`
	FirstCreator string                     `json:"firstcreator"`
	FirstCreated time.Time                  `json:"firstcreated"`
	LastModifier string                     `json:"lastmodifier"`
	LastModified time.Time                  `json:"lastmodified"`
}

func getFirst() User {
	var user User
	mgox.Dao().Find().First(&user)
	return user
}

func TestConnection(t *testing.T) {
	dao := mgox.Dao().Connect()
	defer dao.Close()
	var user User
	dao.Find().IgnoreNFE().First(&user)
	log4go.Debug(user.Id)
	dao.Find().IgnoreNFE().First(&user)
	err := dao.Find().IgnoreNFE().First(&user)
	if handleError(t, err) {
		return
	}
}

func TestInsert(t *testing.T) {
	err := mgox.Dao("111111").Insert(
		&User{Name : "yaosxi", Age : 2, Sex :1},
		&User{Name : "yaosxi2", Age : 3, Sex :1},
		bson.M{"name": "yaosxi3", "age" : 3, "sex" :1},
	)
	if handleError(t, err) {
		return
	}

	err = mgox.Dao("111111").Insert(
		User{Name : "yaosxi4", Age : 2, Sex :1},
	)
	if handleError(t, err) {
		return
	}
}

func TestRemove(t *testing.T) {

	user := getFirst()
	err := mgox.Dao().Remove(user, user.Id)
	if handleError(t, err) {
		return
	}

	user = getFirst()
	err = mgox.Dao().Remove("user", user.Id)
	if handleError(t, err) {
		return
	}

	err = mgox.Dao().Remove(user, bson.M{"name": "carson2"})
	if handleError(t, err) {
		return
	}
}


func TestSet(t *testing.T) {

	user := getFirst()

	name := "ysx111"
	err := mgox.Dao().Set(user, user.Id, "name", name)
	if handleError(t, err) {
		return
	}
	user = getFirst()
	if user.Name != name {
		t.Fail()
		return
	}

	name = "ysx222"
	err = mgox.Dao().Set(&user, user.Id.Hex(), "name", name)
	if handleError(t, err) {
		return
	}
	user = getFirst()
	if user.Name != name {
		t.Fail()
		return
	}

	name = "ysx333"
	err = mgox.Dao().Set("user", user.Id.Hex(), "name", name)
	if handleError(t, err) {
		return
	}
	user = getFirst()
	if user.Name != name {
		t.Fail()
	}

	name = "ysx444"
	err = mgox.Dao().Set(UserCollectionName, user.Id.Hex(), bson.M{"name": name})
	if handleError(t, err) {
		return
	}
	user = getFirst()
	if user.Name != name {
		t.Fail()
	}
}

func TestInc(t *testing.T) {
	user := getFirst()
	err := mgox.Dao("uuuuuuuuuuuuuu").Inc(user, user.Id, "age", 1)
	if handleError(t, err) {
		return
	}
	age := user.Age
	user = getFirst()
	if user.Age != age + 1 {
		t.Fail()
	}
}

func TestReplace(t *testing.T) {
	user := getFirst()
	err := mgox.Dao().Replace(user, user.Id, "name", "carson", "age", 10)
	if handleError(t, err) {
		return
	}
	user = getFirst()
	if user.Name != "carson" {
		log4go.Critical(user.Name)
		t.Fail()
		return
	}
	err = mgox.Dao().ReplaceDoc(User{Id:user.Id, Name : "carson2"})
	if handleError(t, err) {
		return
	}
	user = getFirst()
	if user.Name != "carson2" {
		log4go.Critical(user.Name)
		t.Fail()
	}
}

func TestCount(t *testing.T) {
	n, err := mgox.Dao().Find().Count(UserCollectionName)
	if handleError(t, err) {
		return
	}
	if n != 4 {
		log4go.Debug(n)
		t.Fail()
	}
}

func TestFirst(t *testing.T) {
	var user User
	err := mgox.Dao().Find().IgnoreNFE().First(&user)
	if handleError(t, err) {
		return
	}
	err = mgox.Dao().Find().First(&user)
	if handleError(t, err) {
		return
	}
}

func TestLast(t *testing.T) {
	var user User
	err := mgox.Dao().Find("name", "yaosxi").IgnoreNFE().Last(&user)
	if handleError(t, err) {
		return
	}
	log4go.Debug(user.Name)

	user = User{}
	err = mgox.Dao().Find("name", "name").IgnoreNFE().Last(&user)
	if handleError(t, err) {
		return
	}
	log4go.Debug(user.Name)
}

func TestFind(t *testing.T) {
	var users []User
	err := mgox.Dao().Find().Result(&users)
	if handleError(t, err) {
		return
	}
	log4go.Debug(users)

	p := mgox.Page{Count: 1}
	err = mgox.Dao().Find().Page(&p).Sort("age", "-name").Result(&users)
	if handleError(t, err) {
		return
	}
	log4go.Debug(users)

	var user User
	err = mgox.Dao().Find(users[0].Id).Result(&user)
	if handleError(t, err) {
		return
	}
	log4go.Debug(user)

	user = User{}
	err = mgox.Dao().Find("name", "yaosxi2").Result(&user)
	if handleError(t, err) {
		return
	}
	log4go.Debug(user)
}


func TestGet(t *testing.T) {

	var user User
	err := mgox.Dao().Get().Result(&user)
	if handleError(t, err) {
		return
	}
	log4go.Debug(user)

	id := user.Id
	user = User{}
	err = mgox.Dao().Get(id).Result(&user)
	if handleError(t, err) {
		return
	}
	log4go.Debug(user)

	user = User{}
	err = mgox.Dao().Get("name", "yaosxi2").Result(&user)
	if handleError(t, err) {
		return
	}
	log4go.Debug(user)

	var users []User
	err = mgox.Dao().Find().Result(&users)
	if handleError(t, err) {
		return
	}
}

func TestExist(t *testing.T) {

	b, err := mgox.Dao().Find().Exist("user")
	if handleError(t, err) {
		return
	}
	if !b {
		log4go.Debug(b)
		t.Fail()
		return
	}

	b, err = mgox.Dao().Find("56597ab9f918ad09b4000001").Exist("user")
	if handleError(t, err) {
		return
	}
	if !b {
		log4go.Debug(b)
		t.Fail()
		return
	}

	b, err = mgox.Dao().Find("name", "yaosxi").Exist("user")
	if handleError(t, err) {
		return
	}
	if !b {
		log4go.Debug(b)
		t.Fail()
		return
	}

	b, err = mgox.Dao().Find(bson.M{"name": "yaosxi"}).Exist("user")
	if handleError(t, err) {
		return
	}
	if !b {
		log4go.Debug(b)
		t.Fail()
		return
	}
}