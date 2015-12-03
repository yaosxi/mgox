
// Created by yaoshuangxi

package mgox

import (
	"gopkg.in/mgo.v2"
	"github.com/alecthomas/log4go"
)

var dbSession *mgo.Session

func GetDatabase() (*mgo.Database, error) {

	if dbSession == nil {
		var err error
		dbSession, err = mgo.Dial(DBConfig.host)
		if err != nil {
			return nil, err
		}
		dbSession.SetMode(mgo.Strong, true)
	}

	database := dbSession.Clone().DB(DBConfig.database)

	if DBConfig.username != "" {
		loginErr := database.Login(DBConfig.username, DBConfig.password)
		if loginErr != nil {
			return database, nil
		}
	}

	log4go.Debug("Opened DB connnection successfully")

	return database, nil
}