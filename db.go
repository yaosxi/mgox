
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
		log4go.Info("Dial to %s", DBConfig.host)
		dbSession, err = mgo.Dial(DBConfig.host)
		if err != nil {
			return nil, err
		}
		dbSession.SetMode(mgo.Strong, true)
	}

	log4go.Finest("Try to open DB connnection: %s", DBConfig.database)
	database := dbSession.Clone().DB(DBConfig.database)

	if DBConfig.username != "" {
		loginErr := database.Login(DBConfig.username, DBConfig.password)
		if loginErr != nil {
			return database, nil
		}
	}

	log4go.Finest("Opened DB connnection successfully")

	return database, nil
}