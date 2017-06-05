package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

type ReleaseLog struct {
	Currency    string `json:"currency"`
	Count       int64  `json:"cont"`
	ReleaseTime int64  `json:"releaseTime"`
}

type Assign struct {
	Currency   string `json:"currency`
	Owner      string `json:"owner"`
	Count      int64  `json:"count"`
	AssignTime int64  `json:"assignTime"`
}
type AssignLog struct {
	ToMe []*Assign `json:"toMe"`
	MeTo []*Assign `json:"meTo"`
}

// queryCurrency
func (c *BusinessCC) queryCurrencyByID() ([]byte, error) {
	myLogger.Debug("queryCurrency...")

	if len(c.args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	id := c.args[0]

	_, currency, err := c.getCurrencyByID(id)
	if err != nil {
		myLogger.Errorf("queryCurrencyByID error1:%s", err)
		return nil, err
	}
	if currency == nil {
		return nil, NoDataErr
	}

	return json.Marshal(&currency)
}

// queryAllCurrency
func (c *BusinessCC) queryAllCurrency() ([]byte, error) {
	myLogger.Debug("queryCurrency...")

	if len(c.args) != 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0")
	}

	_, infos, err := c.getAllCurrency()
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, NoDataErr
	}

	return json.Marshal(&infos)
}

// queryTxLogs
func (c *BusinessCC) queryTxLogs() ([]byte, error) {
	myLogger.Debug("queryTxLogs...")

	if len(c.args) != 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0")
	}

	rows, err := getRows(c.stub, TableTxLog2, nil)
	if err != nil {
		myLogger.Errorf("queryTxLogs error1:%s", err)
		return nil, fmt.Errorf("getRows operation failed. %s", err)
	}

	var infos []*Order
	for _, row := range rows {
		var info Order
		err = json.Unmarshal([]byte(row.Columns[1].GetString_()), &info)
		if err == nil {
			myLogger.Errorf("queryTxLogs error2:%s", err)
			infos = append(infos, &info)

		}
	}

	return json.Marshal(&infos)
}

// queryAssetByOwner
func (c *BusinessCC) queryAssetByOwner() ([]byte, error) {
	myLogger.Debug("queryAssetByOwner...")

	if len(c.args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	owner := c.args[0]
	_, assets, err := c.getOwnerAllAsset(owner)
	if err != nil {
		myLogger.Errorf("queryAssetByOwner error1:%s", err)
		return nil, err
	}
	if len(assets) == 0 {
		return nil, NoDataErr
	}
	return json.Marshal(&assets)
}

// queryMyCurrency
func (c *BusinessCC) queryMyCurrency() ([]byte, error) {
	myLogger.Debug("queryCurrency...")

	if len(c.args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	owner := c.args[0]
	currencys, err := c.getMyCurrency(owner)
	if err != nil {
		return nil, err
	}

	return json.Marshal(&currencys)
}

// queryReleaseLog
func (c *BusinessCC) queryMyReleaseLog() ([]byte, error) {
	myLogger.Debug("queryMyReleaseLog...")

	if len(c.args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}
	owner := c.args[0]
	currencys, err := c.getMyCurrency(owner)
	if err != nil {
		return nil, err
	}

	var logs []*ReleaseLog
	for _, v := range currencys {
		rows, err := getRows(c.stub, TableCurrencyReleaseLog, []shim.Column{
			shim.Column{Value: &shim.Column_String_{String_: v.ID}},
		})
		if err != nil {
			continue
		}

		for _, row := range rows {
			logs = append(logs,
				&ReleaseLog{
					Currency:    row.Columns[0].GetString_(),
					Count:       row.Columns[1].GetInt64(),
					ReleaseTime: row.Columns[2].GetInt64(),
				})
		}
	}

	return json.Marshal(logs)
}

// queryMyAssignLog
func (c *BusinessCC) queryMyAssignLog() ([]byte, error) {
	myLogger.Debug("queryAssignLog...")

	if len(c.args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}
	owner := c.args[0]

	currencys, err := c.getMyCurrency(owner)
	if err != nil {
		return nil, err
	}

	logs := &AssignLog{}

	rows, err := getRows(c.stub, TableCurrencyAssignLog, nil)
	if err != nil {
		return nil, fmt.Errorf("getRows operation failed. %s", err)
	}

	for _, row := range rows {
		assign := &Assign{
			Currency:   row.Columns[0].GetString_(),
			Owner:      row.Columns[1].GetString_(),
			Count:      row.Columns[2].GetInt64(),
			AssignTime: row.Columns[3].GetInt64(),
		}

		if assign.Owner == owner {
			logs.ToMe = append(logs.ToMe, assign)
		}
		for _, v := range currencys {
			if v.ID == assign.Currency {
				logs.MeTo = append(logs.MeTo, assign)
			}
		}
	}

	return json.Marshal(logs)
}
