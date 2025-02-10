package recovery

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

/*
   Logs come in the following forms:

	 TABLE log -- create a table;
	 < create tblType table tblName >

   EDIT log -- actions that modify database state;
   < Tx, table, INSERT|DELETE|UPDATE, key, oldval, newval >

   START log -- start of a transaction:
   < Tx start >

   COMMIT log -- end of a transaction:
   < Tx commit >

   CHECKPOINT log -- lists the currently running transactions:
   < Tx1, Tx2... checkpoint >
*/

// Interface that all log structs share.
type log interface {
	toString() string // Serializes the log to a string
}

// Log for creating a table.
type tableLog struct {
	tblType string // The type of table created, either "btree" or "hash"
	tblName string // The name of the table created
}

func (tl tableLog) toString() string {
	return fmt.Sprintf("< create %s table %s >\n", tl.tblType, tl.tblName)
}

// The type of edit action. Either insert, delete, or update.
type action string

const (
	INSERT_ACTION action = "INSERT"
	UPDATE_ACTION action = "UPDATE"
	DELETE_ACTION action = "DELETE"
)

// Log for making a change to a database entry within a transaction.
type editLog struct {
	id        uuid.UUID // The id of the transaction this edit was done in
	tablename string    // The name of the table where the edit took place
	action    action    // The type of edit action taken
	key       int64     // The key of the tuple that was edited
	oldval    int64     // The old value before the edit
	newval    int64     // The new value after the edit
}

func (el editLog) toString() string {
	return fmt.Sprintf("< %s, %s, %s, %v, %v, %v >\n", el.id.String(), el.tablename, el.action, el.key, el.oldval, el.newval)
}

// Log for starting a transaction.
type startLog struct {
	id uuid.UUID // The id of the transaction
}

func (sl startLog) toString() string {
	return fmt.Sprintf("< %s start >\n", sl.id.String())
}

// Log for committing a transaction.
type commitLog struct {
	id uuid.UUID // The id of the transaction
}

func (cl commitLog) toString() string {
	return fmt.Sprintf("< %s commit >\n", cl.id.String())
}

// Log for making a checkpoint.
type checkpointLog struct {
	ids []uuid.UUID // The currently running transactions.
}

func (cl checkpointLog) toString() string {
	idStrings := make([]string, 0)
	for _, id := range cl.ids {
		idStrings = append(idStrings, id.String())
	}
	if len(idStrings) == 0 {
		return "< checkpoint >\n"
	}
	return fmt.Sprintf("< %s checkpoint >\n", strings.Join(idStrings, ", "))
}

// Regex pattern for a uuid
const uuidPattern = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

var tableExp = regexp.MustCompile("< create (?P<tblType>\\w+) table (?P<tblName>\\w+) >")

var editExp = regexp.MustCompile(fmt.Sprintf("< (?P<uuid>%s), (?P<table>\\w+), (?P<action>UPDATE|INSERT|DELETE), (?P<key>\\d+), (?P<oldval>\\d+), (?P<newval>\\d+) >", uuidPattern))
var startExp = regexp.MustCompile(fmt.Sprintf("< (%s) start >", uuidPattern))
var commitExp = regexp.MustCompile(fmt.Sprintf("< (%s) commit >", uuidPattern))
var checkpointExp = regexp.MustCompile(fmt.Sprintf("< (%s,?\\s)*checkpoint >", uuidPattern))
var uuidExp = regexp.MustCompile(uuidPattern)

// Convert the textual representation of a log to its respective struct.
// Returns an error if the string could not be parsed into a log.
func logFromString(s string) (log, error) {
	switch {
	case tableExp.MatchString(s):
		expStrs := tableExp.FindStringSubmatch(s)
		tblType := expStrs[1]
		tblName := expStrs[2]
		return tableLog{
			tblType: tblType,
			tblName: tblName,
		}, nil
	case editExp.MatchString(s):
		expStrs := editExp.FindStringSubmatch(s)
		uuid := uuid.MustParse(expStrs[1])
		key, _ := strconv.Atoi(expStrs[4])
		oldval, _ := strconv.Atoi(expStrs[5])
		newval, _ := strconv.Atoi(expStrs[6])
		return editLog{
			id:        uuid,
			tablename: expStrs[2],
			action:    action(expStrs[3]),
			key:       int64(key),
			oldval:    int64(oldval),
			newval:    int64(newval),
		}, nil
	case startExp.MatchString(s):
		uuid := uuid.MustParse(uuidExp.FindString(s))
		return startLog{id: uuid}, nil
	case commitExp.MatchString(s):
		uuid := uuid.MustParse(uuidExp.FindString(s))
		return commitLog{id: uuid}, nil
	case checkpointExp.MatchString(s):
		uuidStrs := uuidExp.FindAllString(s, -1)
		uuids := make([]uuid.UUID, 0)
		for _, uuidStr := range uuidStrs {
			uuids = append(uuids, uuid.MustParse(uuidStr))
		}
		return checkpointLog{ids: uuids}, nil
	default:
		return nil, errors.New("could not parse log")
	}
}
