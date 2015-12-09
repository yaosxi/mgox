
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
		log4go.Info("Dial to %s", DBConfig.Host)
		dbSession, err = mgo.Dial(DBConfig.Host)
		if err != nil {
			return nil, err
		}
		dbSession.SetMode(mgo.Strong, true)
	}

	log4go.Finest("Try to open DB connnection: %s", DBConfig.Database)
	database := dbSession.Clone().DB(DBConfig.Database)

	if DBConfig.Username != "" {
		loginErr := database.Login(DBConfig.Username, DBConfig.Password)
		if loginErr != nil {
			return database, nil
		}
	}

	log4go.Finest("Opened DB connnection successfully")

	return database, nil
}