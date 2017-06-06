package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

type FailInfo struct {
	Id   string `json:"id"`
	Info string `json:"info"`
}

type BatchResult struct {
	EventName string     `json:"eventName"`
	SrcMethod string     `json:"srcMethod"`
	Success   []string   `json:""success`
	Fail      []FailInfo `json:"fail"`
}

type Order struct {
	UUID         string `json:"uuid"`
	Account      string `json:"account"`
	SrcCurrency  string `json:"srcCurrency"`
	SrcCount     int64  `json:"srcCount"`
	DesCurrency  string `json:"desCurrency"`
	DesCount     int64  `json:"desCount"`
	IsBuyAll     bool   `json:"isBuyAll"`
	ExpiredTime  int64  `json:"expiredTime"`
	PendingTime  int64  `json:"PendingTime"`
	PendedTime   int64  `json:"PendedTime"`
	MatchedTime  int64  `json:"matchedTime"`
	FinishedTime int64  `json:"finishedTime"`
	RawUUID      string `json:"rawUUID"`
	Metadata     string `json:"metadata"`
	FinalCost    int64  `json:"finalCost"`
}

// initAccount init account (CNY/USD currency) when user first login
// args: user
func (c *ExternalityChaincode) initAccount() ([]byte, error) {
	myLogger.Debug("Init account...")

	if len(c.args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	user := c.args[0]

	// find CNY of the user
	assetRow, _, err := c.getOwnerOneAsset(user, CNY)
	if err != nil {
		myLogger.Errorf("initAccount error1:%s", err)
		return nil, fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", CNY, err)
	}
	if len(assetRow.Columns) == 0 {
		_, err = c.stub.InsertRow(TableAssets,
			shim.Row{
				Columns: []*shim.Column{
					&shim.Column{Value: &shim.Column_String_{String_: user}},
					&shim.Column{Value: &shim.Column_String_{String_: CNY}},
					&shim.Column{Value: &shim.Column_Int64{Int64: 0}},
					&shim.Column{Value: &shim.Column_Int64{Int64: 0}},
				},
			})
		if err != nil {
			myLogger.Errorf("initAccount error2:%s", err)
			return nil, err
		}
	}

	// fins USD of the user
	assetRow, _, err = c.getOwnerOneAsset(user, USD)
	if err != nil {
		myLogger.Errorf("initAccount error3:%s", err)
		return nil, fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", USD, err)
	}
	if len(assetRow.Columns) == 0 {
		_, err = c.stub.InsertRow(TableAssets,
			shim.Row{
				Columns: []*shim.Column{
					&shim.Column{Value: &shim.Column_String_{String_: user}},
					&shim.Column{Value: &shim.Column_String_{String_: USD}},
					&shim.Column{Value: &shim.Column_Int64{Int64: 0}},
					&shim.Column{Value: &shim.Column_Int64{Int64: 0}},
				},
			})
		if err != nil {
			myLogger.Errorf("initAccount error4:%s", err)
			return nil, err
		}
	}

	myLogger.Debug("Init account...done")

	return nil, nil
}

// create create currency
// args:currency id, currency count, currency creator
func (c *ExternalityChaincode) create() ([]byte, error) {
	myLogger.Debug("Create Currency...")

	if len(c.args) != 3 {
		return nil, errors.New("Incorrect number of arguments. Expecting 3")
	}

	id := c.args[0]
	count, _ := strconv.ParseInt(c.args[1], 10, 64)
	creator := c.args[2]
	now := time.Now().Unix()

	ok, err := c.stub.InsertRow(TableCurrency,
		shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: id}},
				&shim.Column{Value: &shim.Column_Int64{Int64: count}},
				&shim.Column{Value: &shim.Column_Int64{Int64: count}},
				&shim.Column{Value: &shim.Column_String_{String_: creator}},
				&shim.Column{Value: &shim.Column_Int64{Int64: now}},
			},
		})
	if err != nil {
		myLogger.Errorf("create error2:%s", err)
		return nil, err
	}
	if !ok {
		return nil, errors.New("Currency was already existed")
	}

	if count > 0 {
		err = c.saveReleaseLog(id, count, now)
		if err != nil {
			return nil, err
		}
	}

	myLogger.Debug("Create Currency...done")

	return nil, nil
}

// release release currency
// args: currency id, release count
func (c *ExternalityChaincode) release() ([]byte, error) {
	myLogger.Debug("Release Currency...")

	if len(c.args) != 2 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2")
	}

	id := c.args[0]
	count, err := strconv.ParseInt(c.args[1], 10, 64)
	if err != nil || count <= 0 {
		return nil, errors.New("The currency release count must be > 0")
	}

	if id == CNY || id == USD {
		return nil, errors.New("Currency can't be CNY or USD")
	}

	currRow, curr, err := c.getCurrencyByID(id)
	if err != nil {
		myLogger.Errorf("releaseCurrency error1:%s", err)
		return nil, fmt.Errorf("Failed retrieving currency [%s]: [%s]", id, err)
	}
	if len(currRow.Columns) == 0 {
		return nil, fmt.Errorf("Can't find currency [%s]", id)
	}

	// update currency data
	currRow.Columns[1].Value = &shim.Column_Int64{Int64: curr.Count + count}
	currRow.Columns[2].Value = &shim.Column_Int64{Int64: curr.LeftCount + count}
	ok, err := c.stub.ReplaceRow(TableCurrency, currRow)
	if err != nil {
		myLogger.Errorf("releaseCurrency error2:%s", err)
		return nil, fmt.Errorf("Failed replacing row [%s]", err)
	}
	if !ok {
		return nil, errors.New("Failed replacing row")
	}

	err = c.saveReleaseLog(id, count, time.Now().Unix())
	if err != nil {
		return nil, err
	}

	myLogger.Debug("Release Currency...done")

	return nil, nil
}

// assign  assign currency
// args: json{currency id, []{reciver, count}}
func (c *ExternalityChaincode) assign() ([]byte, error) {
	myLogger.Debug("Assign Currency...")

	if len(c.args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	assign := struct {
		Currency string `json:"currency"`
		Assigns  []struct {
			Owner string `json:"owner"`
			Count int64  `json:"count"`
		} `json:"assigns"`
	}{}

	err := json.Unmarshal([]byte(c.args[0]), &assign)
	if err != nil {
		myLogger.Errorf("assignCurrency error1:%s", err)
		return nil, fmt.Errorf("Failed unmarshalling assign data: [%s]", err)
	}

	if len(assign.Assigns) == 0 {
		return nil, nil
	}

	currRow, curr, err := c.getCurrencyByID(assign.Currency)
	if err != nil {
		myLogger.Errorf("assignCurrency error2:%s", err)
		return nil, fmt.Errorf("Failed retrieving currency [%s]: [%s]", assign.Currency, err)
	}
	if len(currRow.Columns) == 0 {
		return nil, fmt.Errorf("Can't find currency [%s]", assign.Currency)
	}

	assignCount := int64(0)
	for _, v := range assign.Assigns {
		if v.Count <= 0 {
			continue
		}

		assignCount += v.Count
		if assignCount > curr.LeftCount {
			return nil, fmt.Errorf("The left count [%d] of currency [%s] is insufficient", curr.LeftCount, assign.Currency)
		}
	}

	for _, v := range assign.Assigns {
		if v.Count <= 0 {
			continue
		}

		err = c.saveAssignLog(assign.Currency, v.Owner, v.Count)
		if err != nil {
			myLogger.Errorf("assignCurrency error3:%s", err)
			return nil, err
		}

		assetRow, asset, err := c.getOwnerOneAsset(v.Owner, assign.Currency)
		if err != nil {
			myLogger.Errorf("assignCurrency error4:%s", err)
			return nil, fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", assign.Currency, err)
		}
		if len(assetRow.Columns) == 0 {
			_, err = c.stub.InsertRow(TableAssets,
				shim.Row{
					Columns: []*shim.Column{
						&shim.Column{Value: &shim.Column_String_{String_: v.Owner}},
						&shim.Column{Value: &shim.Column_String_{String_: assign.Currency}},
						&shim.Column{Value: &shim.Column_Int64{Int64: v.Count}},
						&shim.Column{Value: &shim.Column_Int64{Int64: 0}},
					},
				})
			if err != nil {
				myLogger.Errorf("assignCurrency error5:%s", err)
				return nil, err
			}
		} else {
			assetRow.Columns[2].Value = &shim.Column_Int64{Int64: asset.Count + v.Count}
			_, err = c.stub.ReplaceRow(TableAssets, assetRow)
			if err != nil {
				myLogger.Errorf("assignCurrency error6:%s", err)
				return nil, err
			}
		}

		curr.LeftCount -= v.Count
	}

	if curr.LeftCount != currRow.Columns[2].GetInt64() {
		currRow.Columns[2].Value = &shim.Column_Int64{Int64: curr.LeftCount}
		_, err = c.stub.ReplaceRow(TableCurrency, currRow)
		if err != nil {
			myLogger.Errorf("assignCurrency error7:%s", err)
			return nil, err
		}
	}

	myLogger.Debug("Assign Currency...done")
	return nil, nil
}

// lock lock or unlock user asset when commit a exchange or cancel exchange
// args: json []{user, currency id, lock count, lock order}, islock, srcMethod
func (c *ExternalityChaincode) lock() ([]byte, error) {
	myLogger.Debug("Lock Asset Balance...")

	if len(c.args) != 3 {
		return nil, errors.New("Incorrect number of arguments. Expecting 3")
	}

	var lockInfos []struct {
		Owner    string `json:"owner"`
		Currency string `json:"currency"`
		OrderId  string `json:"orderId"`
		Count    int64  `json:"count"`
	}

	err := json.Unmarshal([]byte(c.args[0]), &lockInfos)
	if err != nil {
		myLogger.Errorf("lock error1:%s", err)
		return nil, err
	}
	islock, _ := strconv.ParseBool(c.args[1])

	var successInfos []string
	var failInfos []FailInfo

	for _, v := range lockInfos {
		err, errType := c.lockOrUnlockBalance(v.Owner, v.Currency, v.OrderId, v.Count, islock)
		if errType == CheckErr && err != ExecedErr {
			failInfos = append(failInfos, FailInfo{Id: v.OrderId, Info: err.Error()})
			continue
		} else if errType == WorldStateErr {
			myLogger.Errorf("lock error2:%s", err)
			return nil, err
		}
		successInfos = append(successInfos, v.OrderId)
	}

	batch := BatchResult{EventName: "chaincode_lock", Success: successInfos, Fail: failInfos, SrcMethod: c.args[2]}
	result, err := json.Marshal(&batch)
	if err != nil {
		myLogger.Errorf("lock error3:%s", err)
		return nil, err
	}

	c.stub.SetEvent(batch.EventName, result)

	myLogger.Debug("Lock Asset Balance...done")
	return nil, nil
}

// exchange exchange asset
// args: exchange order 1, exchange order 2
func (c *ExternalityChaincode) exchange() ([]byte, error) {
	myLogger.Debug("Exchange...")

	if len(c.args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	var exchangeOrders []struct {
		BuyOrder  Order `json:"buyOrder"`
		SellOrder Order `json:"sellOrder"`
	}
	err := json.Unmarshal([]byte(c.args[0]), &exchangeOrders)
	if err != nil {
		myLogger.Errorf("exchange error1:%s", err)
		return nil, errors.New("Failed unmarshalling order")
	}

	var successInfos []string
	var failInfos []FailInfo

	for _, v := range exchangeOrders {
		buyOrder := v.BuyOrder
		sellOrder := v.SellOrder
		matchOrder := buyOrder.UUID + "," + sellOrder.UUID

		if buyOrder.SrcCurrency != sellOrder.DesCurrency ||
			buyOrder.DesCurrency != sellOrder.SrcCurrency {
			return nil, errors.New("The exchange is invalid")
		}

		// check exchanged or not
		buyRow, _, err := c.getTxLogByID(buyOrder.UUID)
		if err != nil {
			myLogger.Errorf("exchange error2:%s", err)
			failInfos = append(failInfos, FailInfo{Id: matchOrder, Info: err.Error()})
			continue
		}
		if len(buyRow.Columns) > 0 {
			// exchanged
		}
		sellRow, _, err := c.getTxLogByID(sellOrder.UUID)
		if err != nil {
			myLogger.Errorf("exchange error3:%s", err)
			failInfos = append(failInfos, FailInfo{Id: matchOrder, Info: err.Error()})
			continue
		}
		if len(sellRow.Columns) > 0 {
			// exchanged
		}

		// execTx
		err, errType := c.execTx(&buyOrder, &sellOrder)
		if errType == CheckErr && err != ExecedErr {
			failInfos = append(failInfos, FailInfo{Id: matchOrder, Info: err.Error()})
			continue
		} else if errType == WorldStateErr {
			myLogger.Errorf("exchange error4:%s", err)
			return nil, err
		}

		// txlog
		err = c.saveTxLog(&buyOrder, &sellOrder)
		if err != nil {
			myLogger.Errorf("exchange error5:%s", err)
			return nil, err
		}

		successInfos = append(successInfos, matchOrder)
	}

	batch := BatchResult{EventName: "chaincode_exchange", Success: successInfos, Fail: failInfos}
	result, err := json.Marshal(&batch)
	if err != nil {
		myLogger.Errorf("exchange error6:%s", err)
		return nil, err
	}
	c.stub.SetEvent(batch.EventName, result)

	myLogger.Debug("Exchange...done")
	return nil, nil
}
