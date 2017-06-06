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
func (c *ExternalityChaincode) queryCurrencyByID() ([]byte, error) {
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
func (c *ExternalityChaincode) queryAllCurrency() ([]byte, error) {
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
func (c *ExternalityChaincode) queryTxLogs() ([]byte, error) {
	myLogger.Debug("queryTxLogs...")

	if len(c.args) != 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0")
	}

	rowChannel, err := c.stub.GetRows(TableTxLog2, nil)
	if err != nil {
		myLogger.Errorf("queryTxLogs error1:%s", err)
		return nil, fmt.Errorf("getRows operation failed. %s", err)
	}

	var infos []*Order
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				var info Order
				err = json.Unmarshal(row.Columns[1].GetBytes(), &info)
				if err == nil {
					myLogger.Errorf("queryTxLogs error2:%s", err)
					infos = append(infos, &info)
				}
			}
		}
		if rowChannel == nil {
			break
		}
	}

	return json.Marshal(&infos)
}

// queryAssetByOwner
func (c *ExternalityChaincode) queryAssetByOwner() ([]byte, error) {
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
func (c *ExternalityChaincode) queryMyCurrency() ([]byte, error) {
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
func (c *ExternalityChaincode) queryMyReleaseLog() ([]byte, error) {
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
		rowChannel, err := c.stub.GetRows(TableCurrencyReleaseLog, []shim.Column{
			shim.Column{Value: &shim.Column_String_{String_: v.ID}},
		})
		if err != nil {
			continue
		}

		for {
			select {
			case row, ok := <-rowChannel:
				if !ok {
					rowChannel = nil
				} else {
					logs = append(logs,
						&ReleaseLog{
							Currency:    row.Columns[0].GetString_(),
							Count:       row.Columns[1].GetInt64(),
							ReleaseTime: row.Columns[2].GetInt64(),
						})
				}
			}
			if rowChannel == nil {
				break
			}
		}
	}

	return json.Marshal(logs)
}

// queryMyAssignLog
func (c *ExternalityChaincode) queryMyAssignLog() ([]byte, error) {
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

	rowChannel, err := c.stub.GetRows(TableCurrencyAssignLog, nil)
	if err != nil {
		return nil, fmt.Errorf("getRows operation failed. %s", err)
	}

	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
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
		}
		if rowChannel == nil {
			break
		}
	}

	return json.Marshal(logs)
}
