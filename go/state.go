package main

import (
	"encoding/json"
	"strconv"
)

var NilValue = []byte{0x00}

func (c *ExchangeChaincode) putCompositeValue(indexName string, compositeValue []string) error {
	indexKey, err := c.stub.CreateCompositeKey(indexName, compositeValue)
	if err != nil {
		return err
	}

	err = c.stub.PutState(indexKey, NilValue)
	if err != nil {
		return err
	}
	return nil
}

func (c *ExchangeChaincode) getCompositeValue(indexName string, compositeValue []string, keyIndex int) ([][]byte, error) {
	var bb [][]byte

	resultsIterator, err := c.stub.GetStateByPartialCompositeKey(indexName, compositeValue)
	if err != nil {
		return nil, err
	}
	defer resultsIterator.Close()

	for resultsIterator.HasNext() {
		compositeKey, _, err := resultsIterator.Next()
		if err != nil {
			return nil, err
		}

		_, compositeKeyParts, err := c.stub.SplitCompositeKey(compositeKey)
		if err != nil {
			return nil, err
		}

		key := compositeKeyParts[keyIndex]
		b, err := c.stub.GetState(key)
		if err != nil {
			return nil, err
		}
		bb = append(bb, b)
	}

	return bb, nil
}

// Asset Asset
type Asset struct {
	UUID      string `json:"uuid"`
	Owner     string `json:"owner"`
	Currency  string `json:"currency"`
	Count     int64  `json:"count"`
	LockCount int64  `json:"lockCount"`
}

func (c *ExchangeChaincode) putAsset(asset *Asset) error {
	if asset.UUID == "" {
		asset.UUID = GenerateUUID()
	}
	r, err := json.Marshal(asset)
	if err != nil {
		return err
	}

	err = c.stub.PutState(asset.UUID, r)
	if err != nil {
		return err
	}

	err = c.putCompositeValue("Asset~owner~currency~uuid", []string{asset.Owner, asset.Currency, asset.UUID})
	if err != nil {
		return err
	}

	err = c.putCompositeValue("Asset~owner~uuid", []string{asset.Owner, asset.UUID})
	if err != nil {
		return err
	}

	return nil
}

func (c *ExchangeChaincode) getAsset(key string) (*Asset, error) {
	assetByte, err := c.stub.GetState(key)
	if err != nil {
		return nil, err
	}

	var asset *Asset
	err = json.Unmarshal(assetByte, asset)
	if err != nil {
		return nil, err
	}
	return asset, nil
}

// getOwnerOneAsset
func (c *ExchangeChaincode) getOwnerOneAsset(owner, currency string) (*Asset, error) {
	bb, err := c.getCompositeValue("Asset~owner~currency~uuid", []string{owner, currency}, 2)
	if err != nil {
		return nil, err
	}

	var asset *Asset
	err = json.Unmarshal(bb[0], asset)
	if err != nil {
		return nil, err
	}
	return asset, nil
}

// getOwnerAllAsset
func (c *ExchangeChaincode) getOwnerAllAsset(owner string) ([]*Asset, error) {
	bb, err := c.getCompositeValue("Asset~owner~uuid", []string{owner}, 1)
	if err != nil {
		return nil, err
	}

	var assets []*Asset
	for _, v := range bb {
		var asset *Asset
		err = json.Unmarshal(v, asset)
		if err != nil {
			return nil, err
		}
		assets = append(assets, asset)
	}

	return assets, nil
}

// Currency Currency
type Currency struct {
	UUID       string `json:"uuid"`
	Name       string `json:"name"`
	Count      int64  `json:"count"`
	LeftCount  int64  `json:"leftCount"`
	Creator    string `json:"creator"`
	CreateTime int64  `json:"createTime"`
}

// putCurrency putCurrency
func (c *ExchangeChaincode) putCurrency(currency *Currency) error {
	if currency.UUID == "" {
		currency.UUID = GenerateUUID()
	}
	r, err := json.Marshal(currency)
	if err != nil {
		return err
	}

	err = c.stub.PutState(currency.UUID, r)
	if err != nil {
		return err
	}

	err = c.putCompositeValue("Currency~name~uuid", []string{currency.Name, currency.UUID})
	if err != nil {
		return err
	}

	err = c.putCompositeValue("Currency~uuid", []string{currency.UUID})
	if err != nil {
		return err
	}

	err = c.putCompositeValue("Currency~owner~uuid", []string{currency.Creator, currency.UUID})
	if err != nil {
		return err
	}
	return nil
}

func (c *ExchangeChaincode) getCurrency(key string) (*Currency, error) {
	currByte, err := c.stub.GetState(key)
	if err != nil {
		return nil, err
	}

	var curr *Currency
	err = json.Unmarshal(currByte, curr)
	if err != nil {
		return nil, err
	}
	return curr, nil
}

// getCurrencyByID
func (c *ExchangeChaincode) getCurrencyByName(name string) (*Currency, error) {
	bb, err := c.getCompositeValue("Currency~name~uuid", []string{name}, 1)
	if err != nil {
		return nil, err
	}
	var curr *Currency
	err = json.Unmarshal(bb[0], curr)
	if err != nil {
		return nil, err
	}
	return curr, nil
}

// getAllCurrency
func (c *ExchangeChaincode) getAllCurrency() ([]*Currency, error) {
	bb, err := c.getCompositeValue("Currency~uuid", nil, 0)
	if err != nil {
		return nil, err
	}

	var currs []*Currency
	for _, v := range bb {
		var curr *Currency
		err = json.Unmarshal(v, curr)
		if err != nil {
			return nil, err
		}

		currs = append(currs, curr)
	}

	return currs, nil
}

// getMyCurrency
func (c *ExchangeChaincode) getMyCurrency(owner string) ([]*Currency, error) {
	bb, err := c.getCompositeValue("Currency~owner~uuid", []string{owner}, 1)
	if err != nil {
		return nil, err
	}

	var currs []*Currency
	for _, v := range bb {
		var curr *Currency
		err = json.Unmarshal(v, curr)
		if err != nil {
			return nil, err
		}

		currs = append(currs, curr)
	}

	return currs, nil
}

type ReleaseLog struct {
	UUID        string `json:"uuid"`
	Currency    string `json:"currency"`
	Releaser    string `json:"releaser`
	Count       int64  `json:"cont"`
	ReleaseTime int64  `json:"releaseTime"`
}

// saveReleaseLog
func (c *ExchangeChaincode) putReleaseLog(log *ReleaseLog) error {
	if log.UUID == "" {
		log.UUID = GenerateUUID()
	}
	r, err := json.Marshal(log)
	if err != nil {
		return err
	}

	err = c.stub.PutState(log.UUID, r)
	if err != nil {
		return err
	}

	err = c.putCompositeValue("ReleaseLog~owner~uuid", []string{log.Releaser, log.UUID})
	if err != nil {
		return err
	}
	return nil
}

func (c *ExchangeChaincode) getReleaseLog(key string) (*ReleaseLog, error) {
	logByte, err := c.stub.GetState(key)
	if err != nil {
		return nil, err
	}

	var log *ReleaseLog
	err = json.Unmarshal(logByte, log)
	if err != nil {
		return nil, err
	}
	return log, nil
}

func (c *ExchangeChaincode) getMyReleaseLog(owner string) ([]*ReleaseLog, error) {
	bb, err := c.getCompositeValue("ReleaseLog~owner~uuid", []string{owner}, 1)
	if err != nil {
		return nil, err
	}

	var logs []*ReleaseLog
	for _, v := range bb {
		var log *ReleaseLog
		err = json.Unmarshal(v, log)
		if err != nil {
			return nil, err
		}

		logs = append(logs, log)
	}

	return logs, nil
}

type AssignLog struct {
	UUID       string `json:"uuid"`
	Currency   string `json:"currency"`
	FromUser   string `json:"fromUser"`
	ToUser     string `json:"toUser"`
	Count      int64  `json:"count"`
	AssignTime int64  `json:"assignTime"`
}

// saveAssignLog
func (c *ExchangeChaincode) putAssignLog(log *AssignLog) error {
	if log.UUID == "" {
		log.UUID = GenerateUUID()
	}
	r, err := json.Marshal(log)
	if err != nil {
		return err
	}

	err = c.stub.PutState(log.UUID, r)
	if err != nil {
		return err
	}

	err = c.putCompositeValue("AssignLog~from~uuid", []string{log.FromUser, log.UUID})
	if err != nil {
		return err
	}

	err = c.putCompositeValue("AssignLog~to~uuid", []string{log.ToUser, log.UUID})
	if err != nil {
		return err
	}
	return nil
}

func (c *ExchangeChaincode) getFromAssignLog(owner string) ([]*AssignLog, error) {
	bb, err := c.getCompositeValue("AssignLog~from~uuid", []string{owner}, 1)
	if err != nil {
		return nil, err
	}

	var logs []*AssignLog
	for _, v := range bb {
		var log *AssignLog
		err = json.Unmarshal(v, log)
		if err != nil {
			return nil, err
		}

		logs = append(logs, log)
	}
	return logs, nil
}

func (c *ExchangeChaincode) getToAssignLog(owner string) ([]*AssignLog, error) {
	bb, err := c.getCompositeValue("AssignLog~to~uuid", []string{owner}, 1)
	if err != nil {
		return nil, err
	}

	var logs []*AssignLog
	for _, v := range bb {
		var log *AssignLog
		err = json.Unmarshal(v, log)
		if err != nil {
			return nil, err
		}

		logs = append(logs, log)
	}
	return logs, nil
}

type LockLog struct {
	UUID      string `json:"uuid"`
	Owner     string `json:"owner"`
	Currency  string `json:"currency"`
	Order     string `json:"order"`
	IsLock    bool   `json:"isLock"`
	LockCount int64  `json:"lockCount"`
	LockTime  int64  `json:"lockTime"`
}

func (c *ExchangeChaincode) putLockLog(log *LockLog) error {
	if log.UUID == "" {
		log.UUID = GenerateUUID()
	}
	r, err := json.Marshal(log)
	if err != nil {
		return err
	}

	err = c.stub.PutState(log.UUID, r)
	if err != nil {
		return err
	}

	err = c.putCompositeValue("LockLog~owner~curr~order~islock~uuid", []string{log.Owner, log.Currency, log.Order, strconv.FormatBool(log.IsLock), log.UUID})
	if err != nil {
		return err
	}

	return nil
}

// getLockLog getLockLog
func (c *ExchangeChaincode) getLockLog(key string) (*LockLog, error) {
	logByte, err := c.stub.GetState(key)
	if err != nil {
		return nil, err
	}

	var log *LockLog
	err = json.Unmarshal(logByte, logByte)
	if err != nil {
		return nil, err
	}
	return log, nil
}

// getLockLog getLockLog
func (c *ExchangeChaincode) getLockLogByParm(owner, currency, order string, islock bool) (*LockLog, error) {
	// var log *LockLog
	bb, err := c.getCompositeValue("LockLog~owner~curr~order~islock~uuid", []string{owner, currency, order, strconv.FormatBool(islock)}, 4)
	if err != nil {
		return nil, err
	}

	var log *LockLog
	err = json.Unmarshal(bb[0], log)
	if err != nil {
		return nil, err
	}
	return log, nil
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

// putTxLog
func (c *ExchangeChaincode) putTxLog(buyOrder, sellOrder *Order) error {
	buyJson, err := json.Marshal(buyOrder)
	if err != nil {
		return err
	}
	sellJson, err := json.Marshal(sellOrder)
	if err != nil {
		return err
	}

	err = c.stub.PutState(buyOrder.UUID, buyJson)
	if err != nil {
		return err
	}

	err = c.stub.PutState(sellOrder.UUID, sellJson)
	if err != nil {
		return err
	}

	indexName := "Order~owner~src~des~raw~uuid"
	err = c.putCompositeValue(indexName, []string{buyOrder.Account, buyOrder.SrcCurrency, buyOrder.DesCurrency, buyOrder.RawUUID, buyOrder.UUID})
	if err != nil {
		return err
	}

	err = c.putCompositeValue(indexName, []string{sellOrder.Account, sellOrder.SrcCurrency, sellOrder.DesCurrency, sellOrder.RawUUID, sellOrder.UUID})
	if err != nil {
		return err
	}

	err = c.putCompositeValue("Order~uuid", []string{buyOrder.UUID})
	if err != nil {
		return err
	}
	return nil
}

// getTxLog
func (c *ExchangeChaincode) getTxLog(key string) (*Order, error) {
	orderByte, err := c.stub.GetState(key)
	if err != nil {
		return nil, err
	}

	var order *Order
	err = json.Unmarshal(orderByte, order)
	if err != nil {
		return nil, err
	}
	return order, nil
}

// getTXs
func (c *ExchangeChaincode) getTXs(owner, srcCurrency, desCurrency, rawOrder string) ([]*Order, error) {
	bb, err := c.getCompositeValue("Order~owner~src~des~raw~uuid", []string{owner, srcCurrency, desCurrency, rawOrder}, 4)
	if err != nil {
		return nil, err
	}

	var orders []*Order
	for _, v := range bb {
		var order *Order
		err = json.Unmarshal(v, order)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}

	return orders, nil
}

func (c *ExchangeChaincode) getAllTxLog() ([]*Order, error) {
	bb, err := c.getCompositeValue("Order~uuid", nil, 0)
	if err != nil {
		return nil, err
	}

	var orders []*Order
	for _, v := range bb {
		var order *Order
		err = json.Unmarshal(v, order)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}

	return orders, nil
}
