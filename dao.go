package mgox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alecthomas/log4go"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"time"
)

type dao struct {
	db  *mgo.Database
	uid interface{}
	Err error
}

func Dao(configs ...interface{}) *dao {
	d := new(dao)
	if len(configs) > 0 {
		d.uid = configs[0]
	}
	return d
}

func (d *dao) Connect() *dao {
	if d.Err == nil {
		d.db, d.Err = GetDatabase()
	}
	return d
}

func (d *dao) Close() {
	if d.db != nil {
		d.db.Session.Close()
		d.db = nil
		log4go.Debug("Closed DB connection succssfully")
	}
}

func (d *dao) Insert(docs ...interface{}) error {

	if d.Err != nil {
		return d.Err
	}

	for i, _ := range docs {

		if docs[i] == nil {
			d.Err = errors.New("cannot insert empty document")
			return d.Err
		}

		mType := reflect.TypeOf(docs[i])
		mValue := reflect.ValueOf(docs[i])
		mType, mValue = getElem(mType, mValue)

		if mType.Kind() == reflect.Struct && mValue.IsValid() {
			field := mValue.FieldByName("Id")
			if field.IsValid() && field.String() == "" && field.CanSet() {
				field.Set(reflect.ValueOf(bson.NewObjectId()))
			}
			now := reflect.ValueOf(time.Now())
			field = mValue.FieldByName("FirstCreated")
			if field.CanSet() {
				field.Set(now)
			}
			field = mValue.FieldByName("LastModified")
			if field.CanSet() {
				field.Set(now)
			}

			if d.uid != nil {
				field = mValue.FieldByName("FirstCreator")
				if field.CanSet() {
					field.Set(reflect.ValueOf(d.uid))
				}
				field = mValue.FieldByName("LastModifier")
				if field.CanSet() {
					field.Set(reflect.ValueOf(d.uid))
				}
			}

		} else if mType.Kind() == reflect.Map {
			m, ok := docs[i].(bson.M)
			if !ok {
				m, _ = docs[i].(map[string]interface{})
			}
			if len(m) > 0 {
				if _, ok := m["_id"]; !ok {
					m["_id"] = bson.NewObjectId()
				}
				now := time.Now()
				m["firstcreated"] = now
				m["lastmodified"] = now
				if d.uid != nil {
					m["firstcreator"] = d.uid
					m["lastmodifier"] = d.uid
				}
			}
		}
	}

	if d.db == nil {
		d.Connect()
		if d.Err != nil {
			return d.Err
		}
		defer d.Close()
	}

	log4go.Debug("insert")

	d.Err = d.db.C(getCollectionName(docs[0])).Insert(docs...)
	return d.Err
}

func (d *dao) Remove(collectionName interface{}, selector interface{}) error {
	if d.Err != nil {
		return d.Err
	}

	if d.db == nil {
		d.Connect()
		if d.Err != nil {
			return d.Err
		}
		defer d.Close()
	}

	log4go.Debug("remove")

	c := d.db.C(getCollectionName(collectionName))

	var id bson.ObjectId
	if strId, ok := selector.(string); ok {
		if strId == "" {
			d.Err = errors.New("id can't be empty")
			return d.Err
		}
		id = bson.ObjectIdHex(strId)
	} else if oId, ok := selector.(bson.ObjectId); ok {
		if oId == "" {
			d.Err = errors.New("id can't be empty")
			return d.Err
		}
		id = oId
	} else if _, ok := selector.(bson.M); ok {

	} else if _, ok := selector.(map[string]interface{}); ok {

	} else {
		d.Err = errors.New("unrecognized selector for remove")
		return d.Err
	}

	if id != "" {
		d.Err = c.RemoveId(id)
	} else {
		_, d.Err = c.RemoveAll(selector)
	}

	return d.Err
}

func (d *dao) isID(selectors ...interface{}) bool {
	if len(selectors) != 1 {
		return false
	}
	return !d.isM(selectors[0])
}

func (d *dao) isM(v interface{}) bool {
	if _, ok := v.(bson.M); ok {
		return true
	} else if _, ok := v.(map[string]interface{}); ok {
		return true
	}
	return false
}

func (d *dao) getM(values ...interface{}) bson.M {
	m := bson.M{}
	for i := 0; i < len(values); i++ {
		if _m, ok := values[i].(bson.M); ok {
			for key, value := range _m {
				m[key] = value
			}
		} else if _m, ok := values[i].(map[string]interface{}); ok {
			for key, value := range _m {
				m[key] = value
			}
		} else {
			if i == len(values)-1 {
				break
			}
			m[values[i].(string)] = values[i+1]
			i++
		}
	}
	return m
}

func (d *dao) update(operator string, collectionName interface{}, selector interface{}, updates ...interface{}) error {

	if d.Err != nil {
		return d.Err
	}

	if len(updates) == 0 {
		d.Err = errors.New("updates can't be empty")
		return d.Err
	}

	isStruct := false
	mType := reflect.TypeOf(updates[0])
	mValue := reflect.ValueOf(updates[0])
	mType, mValue = getElem(mType, mValue)
	if mType.Kind() == reflect.Struct && mValue.IsValid() {

		isStruct = true

		field := mValue.FieldByName("LastModified")
		if field.CanSet() {
			field.Set(reflect.ValueOf(time.Now()))
		}

		if d.uid != nil {
			field := mValue.FieldByName("LastModifier")
			if field.CanSet() {
				field.Set(reflect.ValueOf(d.uid))
			}
		}
	}

	var update bson.M

	if !isStruct {

		update = d.getM(updates...)

		if operator == "$inc" || operator == "$push" {
			m := bson.M{"lastmodified": time.Now()}
			if d.uid != nil {
				m["lastmodifier"] = d.uid
			}
			update = bson.M{"$set": m, operator: update}
		} else {
			if operator == "$set" || operator == "$update" {
				update["lastmodified"] = time.Now()
				if d.uid != nil {
					update["lastmodifier"] = d.uid
				}
			}

			if operator != "update" {
				update = bson.M{operator: update}
			}
		}
	}

	if d.db == nil {
		d.Connect()
		if d.Err != nil {
			return d.Err
		}
		defer d.Close()
	}

	_collectionName := getCollectionName(collectionName)
	c := d.db.C(_collectionName)

	var id bson.ObjectId
	if strId, ok := selector.(string); ok {
		if strId == "" {
			d.Err = errors.New("id can't be empty")
			return d.Err
		}
		id = bson.ObjectIdHex(strId)
	} else if oId, ok := selector.(bson.ObjectId); ok {
		if oId == "" {
			d.Err = errors.New("id can't be empty")
			return d.Err
		}
		id = oId
	} else if _, ok := selector.(bson.M); ok {

	} else if _, ok := selector.(map[string]interface{}); ok {

	} else {
		d.Err = errors.New("unrecognized selector for update")
		return d.Err
	}

	if isStruct {
		if id != "" {
			log4go.Debug(fmt.Sprintf("[%s]collection=%s,id=%s,struct=%s", operator, _collectionName, id, updates[0]))
			d.Err = c.UpdateId(id, updates[0])
		} else {
			log4go.Debug(fmt.Sprintf("[%s]collection=%s,selector=%s,struct=%s", operator, _collectionName, selector, updates[0]))
			_, d.Err = c.UpdateAll(selector, updates[0])
		}
	} else {
		if id != "" {
			log4go.Debug(fmt.Sprintf("id=%s", id))
			log4go.Debug(fmt.Sprintf("[%s]collection=%s,id=%s,update=%s", operator, _collectionName, id, update))
			d.Err = c.UpdateId(id, update)
		} else {
			log4go.Debug(fmt.Sprintf("[%s]collection=%s,selector=%s,struct=%s", operator, _collectionName, selector, update))
			_, d.Err = c.UpdateAll(selector, update)
		}
	}

	return d.Err
}

func (d *dao) Set(collectionName interface{}, selector interface{}, updates ...interface{}) error {
	return d.update("$set", collectionName, selector, updates...)
}

func (d *dao) Inc(collectionName interface{}, selector interface{}, updates ...interface{}) error {
	return d.update("$inc", collectionName, selector, updates...)
}

func (d *dao) Push(collectionName interface{}, selector interface{}, updates ...interface{}) error {
	return d.update("$push", collectionName, selector, updates...)
}

func (d *dao) Pull(collectionName interface{}, selector interface{}, updates ...interface{}) error {
	return d.update("$pull", collectionName, selector, updates...)
}

func (d *dao) Replace(collectionName interface{}, selector interface{}, updates ...interface{}) error {
	return d.update("update", collectionName, selector, updates...)
}

func (d *dao) ReplaceDoc(doc interface{}) error {
	return d.update("update", doc, getObjectId(doc), doc)
}

type Query struct {
	collection string
	dao        *dao
	ignoreNFE  bool
	queries    []interface{}
	sorts      []string
	page       *Page
	distinct   string
}

func (d *dao) Find(queries ...interface{}) *Query {
	query := &Query{}
	query.dao = d
	query.queries = queries
	return query
}

func (d *dao) Get(queries ...interface{}) *Query {
	return d.Find(queries...)
}

func (q *Query) IgnoreNFE() *Query {
	q.ignoreNFE = true
	return q
}

func (q *Query) Page(page *Page) *Query {
	if page != nil && page.Cursor >= 0 {
		q.page = page
	}
	return q
}

func (q *Query) Sort(sorts ...string) *Query {
	q.sorts = sorts
	return q
}

func (q *Query) Distinct(collectionName interface{}, distinct string, result interface{}) error {
	q.distinct = distinct
	q.collection = getCollectionName(collectionName)
	return q.Result(result)
}

func (q *Query) Result(result interface{}) error {

	if q.dao.Err != nil {
		return q.dao.Err
	}
	if q.dao.db == nil {
		q.dao.Connect()
		if q.dao.Err != nil {
			return q.dao.Err
		}
		defer q.dao.Close()
	}

	collectionName := q.collection
	if collectionName == "" {
		collectionName = getCollectionName(result)
	}
	c := q.dao.db.C(collectionName)

	var selector bson.M
	var mgoQuery *mgo.Query

	var log bytes.Buffer
	log.WriteString(fmt.Sprintf("[query]collection=%s", collectionName))

	if q.dao.isID(q.queries...) {
		if IsSlice(result) {
			panic("result argument can't be a slice address")
		}
		log.WriteString(fmt.Sprintf(",id=%s", q.queries[0]))
		if str, ok := q.queries[0].(string); ok {
			mgoQuery = c.FindId(bson.ObjectIdHex(str))
		} else {
			mgoQuery = c.FindId(q.queries[0])
		}
	} else {
		selector = q.dao.getM(q.queries...)
		log.WriteString(fmt.Sprintf(",selector=%s", selector))
		mgoQuery = c.Find(selector)
	}

	if q.sorts != nil && len(q.sorts) > 0 {
		mgoQuery = mgoQuery.Sort(q.sorts...)
		log.WriteString(fmt.Sprintf(",sort=%s", q.sorts))
	}

	if q.page != nil {
		log.WriteString(fmt.Sprintf(",page=%s", q.page))
		q.page.Total, q.dao.Err = mgoQuery.Count()
		if q.dao.Err != nil {
			return q.dao.Err
		}
		if q.page.Count == 0 {
			q.page.Count = PAGE_RECORD_COUNT
		}
		q.page.Next = q.page.Total
		mgoQuery = mgoQuery.Skip(q.page.Cursor).Limit(q.page.Count)
	}

	if q.distinct != "" {
		log.WriteString(fmt.Sprintf(",distinct=%s", q.distinct))
		q.dao.Err = mgoQuery.Distinct(q.distinct, result)
	} else if IsSlice(result) {
		q.dao.Err = mgoQuery.All(result)
		if q.page != nil {
			len := GetValueLen(result)
			if len > 0 {
				q.page.Next = q.page.Cursor + len
			}
		}
	} else {
		q.dao.Err = mgoQuery.One(result)
		if q.page != nil {
			len := 1
			if q.dao.Err == mgo.ErrNotFound {
				len = 0
			}
			q.page.Next = q.page.Cursor + len
		}
	}

	if q.ignoreNFE && q.dao.Err == mgo.ErrNotFound {
		q.dao.Err = nil
	}

	log4go.Trace(log.String())

	if q.page != nil {
		b, _ := json.Marshal(q.page)
		log4go.Trace("page: %s", string(b))
	}
	b, _ := json.Marshal(result)
	log4go.Trace("result: %s", string(b))

	return q.dao.Err
}

func (q *Query) Count(collectionName interface{}) (int, error) {
	if q.dao.Err != nil {
		return -1, q.dao.Err
	}
	if q.dao.db == nil {
		q.dao.Connect()
		if q.dao.Err != nil {
			return -1, q.dao.Err
		}
		defer q.dao.Close()
	}

	log4go.Debug("count")

	c := q.dao.db.C(getCollectionName(collectionName))
	var mgoQuery *mgo.Query
	if q.dao.isID(q.queries...) {
		log4go.Debug("query id: %s", q.queries[0])
		if str, ok := q.queries[0].(string); ok {
			mgoQuery = c.FindId(bson.ObjectIdHex(str))
		} else {
			mgoQuery = c.FindId(q.queries[0])
		}
	} else {
		selector := q.dao.getM(q.queries...)
		log4go.Debug("queries: %s", selector)
		mgoQuery = c.Find(selector)
	}
	var n int
	n, q.dao.Err = mgoQuery.Count()
	log4go.Debug(n)
	return n, q.dao.Err
}

func (q *Query) First(result interface{}) error {
	log4go.Debug("first")
	q.page = &Page{Count: 1}
	return q.Result(result)
}

func (q *Query) Last(result interface{}) error {
	if q.dao.Err != nil {
		return q.dao.Err
	}
	if q.dao.db == nil {
		q.dao.Connect()
		if q.dao.Err != nil {
			return q.dao.Err
		}
		defer q.dao.Close()
	}
	count, _ := q.Count(result)
	if q.dao.Err != nil {
		return q.dao.Err
	}
	if count == 0 {
		count = 1
	}
	log4go.Debug("last")
	q.page = &Page{Cursor: count - 1, Count: 1}
	return q.Result(result)
}

func (q *Query) Exist(collectionName interface{}) (bool, error) {
	n, _ := q.Count(collectionName)
	return n > 0, q.dao.Err
}
