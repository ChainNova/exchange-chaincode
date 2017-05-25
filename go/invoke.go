package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
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

type ErrType string

const (
	CheckErr      = ErrType("CheckErr")
	WorldStateErr = ErrType("WdErr")
)

var (
	ExecedErr = errors.New("execed")
	NoDataErr = errors.New("No row data")
)

// initAccount init account (CNY/USD currency) when user first login
// args: user
func (c *ExchangeChaincode) initAccount() pb.Response {
	myLogger.Debug("Init account...")

	if len(c.args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
	}

	user := c.args[0]

	// find CNY of the user
	asset, err := c.getOwnerOneAsset(user, CNY)
	if err != nil {
		myLogger.Errorf("initAccount error1:%s", err)
		return shim.Error(fmt.Sprintf("Failed retrieving asset [%s] of the user: [%s]", CNY, err))
	}
	if asset == nil || asset.UUID == "" {
		err = c.putAsset(&Asset{
			Owner:     user,
			Currency:  CNY,
			Count:     0,
			LockCount: 0,
		})
		if err != nil {
			return shim.Error(err.Error())
		}
	}

	// fins USD of the user
	asset, err = c.getOwnerOneAsset(user, USD)
	if err != nil {
		myLogger.Errorf("initAccount error3:%s", err)
		return shim.Error(fmt.Sprintf("Failed retrieving asset [%s] of the user: [%s]", USD, err))
	}
	if asset == nil || asset.UUID == "" {
		err = c.putAsset(&Asset{
			Owner:     user,
			Currency:  USD,
			Count:     0,
			LockCount: 0,
		})
		if err != nil {
			return shim.Error(err.Error())
		}
	}

	myLogger.Debug("Init account...done")

	return shim.Success(nil)
}

// create create currency
// args:currency id, currency count, currency creator
func (c *ExchangeChaincode) create() pb.Response {
	myLogger.Debug("Create Currency...")

	if len(c.args) != 3 {
		return shim.Error("Incorrect number of arguments. Expecting 3")
	}

	name := c.args[0]
	count, _ := strconv.ParseInt(c.args[1], 10, 64)
	creator := c.args[2]
	now := time.Now().Unix()

	err := c.putCurrency(&Currency{
		Name:       name,
		Count:      count,
		LeftCount:  count,
		Creator:    creator,
		CreateTime: now,
	})
	if err != nil {
		myLogger.Errorf("create error2:%s", err)
		return shim.Error(err.Error())
	}

	if count > 0 {
		err = c.putReleaseLog(&ReleaseLog{
			Currency:    name,
			Releaser:    creator,
			Count:       count,
			ReleaseTime: now,
		})
		if err != nil {
			return shim.Error(err.Error())
		}
	}

	myLogger.Debug("Create Currency...done")

	return shim.Success(nil)
}

// release release currency
// args: currency id, release count
func (c *ExchangeChaincode) release() pb.Response {
	myLogger.Debug("Release Currency...")

	if len(c.args) != 2 {
		return shim.Error("Incorrect number of arguments. Expecting 2")
	}

	id := c.args[0]
	count, err := strconv.ParseInt(c.args[1], 10, 64)
	if err != nil || count <= 0 {
		return shim.Error("The currency release count must be > 0")
	}

	if id == CNY || id == USD {
		return shim.Error("Currency can't be CNY or USD")
	}

	curr, err := c.getCurrencyByName(id)
	if err != nil {
		myLogger.Errorf("releaseCurrency error1:%s", err)
		return shim.Error(fmt.Sprintf("Failed retrieving currency [%s]: [%s]", id, err))
	}

	// update currency data
	curr.Count = curr.Count + count
	curr.LeftCount = curr.LeftCount + count
	err = c.putCurrency(curr)
	if err != nil {
		myLogger.Errorf("releaseCurrency error2:%s", err)
		return shim.Error(fmt.Sprintf("Failed replacing row [%s]", err))
	}

	err = c.putReleaseLog(&ReleaseLog{
		Currency:    id,
		Releaser:    curr.Creator,
		Count:       count,
		ReleaseTime: time.Now().Unix(),
	})
	if err != nil {
		return shim.Error(err.Error())
	}

	myLogger.Debug("Release Currency...done")

	return shim.Success(nil)
}

// assign  assign currency
// args: json{currency id, []{reciver, count}}
func (c *ExchangeChaincode) assign() pb.Response {
	myLogger.Debug("Assign Currency...")

	if len(c.args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
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
		return shim.Error(fmt.Sprintf("Failed unmarshalling assign data: [%s]", err))
	}

	if len(assign.Assigns) == 0 {
		return shim.Success(nil)
	}

	curr, err := c.getCurrencyByName(assign.Currency)
	if err != nil {
		myLogger.Errorf("assignCurrency error2:%s", err)
		return shim.Error(fmt.Sprintf("Failed retrieving currency [%s]: [%s]", assign.Currency, err))
	}

	assignCount := int64(0)
	for _, v := range assign.Assigns {
		if v.Count <= 0 {
			continue
		}

		assignCount += v.Count
		if assignCount > curr.LeftCount {
			return shim.Error(fmt.Sprintf("The left count [%d] of currency [%s] is insufficient", curr.LeftCount, assign.Currency))
		}
	}

	for _, v := range assign.Assigns {
		if v.Count <= 0 {
			continue
		}

		err = c.putAssignLog(&AssignLog{
			Currency:   assign.Currency,
			FromUser:   curr.Creator,
			ToUser:     v.Owner,
			Count:      v.Count,
			AssignTime: time.Now().Unix(),
		})
		if err != nil {
			myLogger.Errorf("assignCurrency error3:%s", err)
			return shim.Error(err.Error())
		}

		asset, err := c.getOwnerOneAsset(v.Owner, assign.Currency)
		if err != nil {
			myLogger.Errorf("assignCurrency error4:%s", err)
			return shim.Error(fmt.Sprintf("Failed retrieving asset [%s] of the user: [%s]", assign.Currency, err))
		}

		asset.Count = asset.Count + v.Count
		err = c.putAsset(asset)
		if err != nil {
			return shim.Error(err.Error())
		}

		curr.LeftCount -= v.Count
	}

	err = c.putCurrency(curr)
	if err != nil {
		return shim.Error(err.Error())
	}

	myLogger.Debug("Assign Currency...done")
	return shim.Success(nil)
}

// lock lock or unlock user asset when commit a exchange or cancel exchange
// args: json []{user, currency id, lock count, lock order}, islock, srcMethod
func (c *ExchangeChaincode) lock() pb.Response {
	myLogger.Debug("Lock Asset Balance...")

	if len(c.args) != 3 {
		return shim.Error("Incorrect number of arguments. Expecting 3")
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
		return shim.Error(err.Error())
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
			return shim.Error(err.Error())
		}
		successInfos = append(successInfos, v.OrderId)
	}

	batch := BatchResult{EventName: "chaincode_lock", Success: successInfos, Fail: failInfos, SrcMethod: c.args[2]}
	result, err := json.Marshal(&batch)
	if err != nil {
		myLogger.Errorf("lock error3:%s", err)
		return shim.Error(err.Error())
	}

	c.stub.SetEvent(batch.EventName, result)

	myLogger.Debug("Lock Asset Balance...done")
	return shim.Success(nil)
}

// exchange exchange asset
// args: exchange order 1, exchange order 2
func (c *ExchangeChaincode) exchange() pb.Response {
	myLogger.Debug("Exchange...")

	if len(c.args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
	}

	var exchangeOrders []struct {
		BuyOrder  Order `json:"buyOrder"`
		SellOrder Order `json:"sellOrder"`
	}
	err := json.Unmarshal([]byte(c.args[0]), &exchangeOrders)
	if err != nil {
		myLogger.Errorf("exchange error1:%s", err)
		return shim.Error("Failed unmarshalling order")
	}

	var successInfos []string
	var failInfos []FailInfo

	for _, v := range exchangeOrders {
		buyOrder := v.BuyOrder
		sellOrder := v.SellOrder
		matchOrder := buyOrder.UUID + "," + sellOrder.UUID

		if buyOrder.SrcCurrency != sellOrder.DesCurrency ||
			buyOrder.DesCurrency != sellOrder.SrcCurrency {
			return shim.Error("The exchange is invalid")
		}

		// check exchanged or not
		buy, err := c.getTxLog(buyOrder.UUID)
		if err != nil {
			myLogger.Errorf("exchange error2:%s", err)
			failInfos = append(failInfos, FailInfo{Id: matchOrder, Info: err.Error()})
			continue
		}
		if buy != nil && buy.UUID != "" {
			// exchanged
		}

		sell, err := c.getTxLog(sellOrder.UUID)
		if err != nil {
			myLogger.Errorf("exchange error3:%s", err)
			failInfos = append(failInfos, FailInfo{Id: matchOrder, Info: err.Error()})
			continue
		}
		if sell != nil && sell.UUID != "" {
			// exchanged
		}

		// execTx
		err, errType := c.execTx(&buyOrder, &sellOrder)
		if errType == CheckErr && err != ExecedErr {
			failInfos = append(failInfos, FailInfo{Id: matchOrder, Info: err.Error()})
			continue
		} else if errType == WorldStateErr {
			myLogger.Errorf("exchange error4:%s", err)
			return shim.Error(err.Error())
		}

		// txlog
		err = c.putTxLog(&buyOrder, &sellOrder)
		if err != nil {
			myLogger.Errorf("exchange error5:%s", err)
			return shim.Error(err.Error())
		}

		successInfos = append(successInfos, matchOrder)
	}

	batch := BatchResult{EventName: "chaincode_exchange", Success: successInfos, Fail: failInfos}
	result, err := json.Marshal(&batch)
	if err != nil {
		myLogger.Errorf("exchange error6:%s", err)
		return shim.Error(err.Error())
	}
	c.stub.SetEvent(batch.EventName, result)

	myLogger.Debug("Exchange...done")
	return shim.Success(nil)
}

// execTx execTx
func (c *ExchangeChaincode) execTx(buyOrder, sellOrder *Order) (error, ErrType) {
	// UUID=rawuuID
	if buyOrder.IsBuyAll && buyOrder.UUID == buyOrder.RawUUID {
		unlock, err := c.computeBalance(buyOrder.Account, buyOrder.SrcCurrency, buyOrder.DesCurrency, buyOrder.RawUUID, buyOrder.FinalCost)
		if err != nil {
			myLogger.Errorf("execTx error1:%s", err)
			return errors.New("Failed compute balance"), CheckErr
		}
		myLogger.Debugf("Order %s balance %d", buyOrder.UUID, unlock)
		if unlock > 0 {
			err, errType := c.lockOrUnlockBalance(buyOrder.Account, buyOrder.SrcCurrency, buyOrder.RawUUID, unlock, false)
			if err != nil {
				myLogger.Errorf("execTx error2:%s", err)
				return errors.New("Failed unlock balance"), errType
			}
		}
	}

	// buy order srcCurrency -
	buySrcAsset, err := c.getOwnerOneAsset(buyOrder.Account, buyOrder.SrcCurrency)
	if err != nil {
		myLogger.Errorf("execTx error3:%s", err)
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", buyOrder.SrcCurrency, err), CheckErr
	}
	if buySrcAsset == nil || buySrcAsset.UUID == "" {
		return fmt.Errorf("The user have not currency [%s]", buyOrder.SrcCurrency), CheckErr
	}
	buySrcAsset.LockCount = buySrcAsset.LockCount - buyOrder.FinalCost
	err = c.putAsset(buySrcAsset)
	if err != nil {
		myLogger.Errorf("execTx error4:%s", err)
		return errors.New("Failed updating row"), WorldStateErr
	}

	// buy order srcCurrency +
	buyDesAsset, err := c.getOwnerOneAsset(buyOrder.Account, buyOrder.DesCurrency)
	if err != nil {
		myLogger.Errorf("execTx error5:%s", err)
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", buyOrder.DesCurrency, err), CheckErr
	}
	if buyDesAsset == nil || buyDesAsset.UUID == "" {
		err = c.putAsset(&Asset{
			Owner:     buyOrder.Account,
			Currency:  buyOrder.DesCurrency,
			Count:     buyOrder.DesCount,
			LockCount: int64(0),
		})

		if err != nil {
			myLogger.Errorf("execTx error6:%s", err)
			return errors.New("Failed inserting row"), WorldStateErr
		}
	} else {
		buyDesAsset.Count = buyDesAsset.Count + buyOrder.DesCount
		err = c.putAsset(buyDesAsset)
		if err != nil {
			myLogger.Errorf("execTx error7:%s", err)
			return errors.New("Failed updating row"), WorldStateErr
		}
	}

	// UUID=rawuuid
	if sellOrder.IsBuyAll && sellOrder.UUID == sellOrder.RawUUID {
		unlock, err := c.computeBalance(sellOrder.Account, sellOrder.SrcCurrency, sellOrder.DesCurrency, sellOrder.RawUUID, sellOrder.FinalCost)
		if err != nil {
			myLogger.Errorf("execTx error8:%s", err)
			return errors.New("Failed compute balance"), CheckErr
		}
		myLogger.Debugf("Order %s balance %d", sellOrder.UUID, unlock)
		if unlock > 0 {
			err, errType := c.lockOrUnlockBalance(sellOrder.Account, sellOrder.SrcCurrency, sellOrder.RawUUID, unlock, false)
			if err != nil {
				myLogger.Errorf("execTx error9:%s", err)
				return errors.New("Failed unlock balance"), errType
			}
		}
	}

	// sell order srcCurrency -
	sellSrcAsset, err := c.getOwnerOneAsset(sellOrder.Account, sellOrder.SrcCurrency)
	if err != nil {
		myLogger.Errorf("execTx error10:%s", err)
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", sellOrder.SrcCurrency, err), CheckErr
	}
	if sellSrcAsset == nil || sellSrcAsset.UUID == "" {
		return fmt.Errorf("The user have not currency [%s]", sellOrder.SrcCurrency), CheckErr
	}
	sellSrcAsset.LockCount = sellSrcAsset.LockCount - sellOrder.FinalCost
	err = c.putAsset(sellSrcAsset)
	if err != nil {
		myLogger.Errorf("execTx error11:%s", err)
		return errors.New("Failed updating row"), WorldStateErr
	}

	// sell order desCurrency +
	sellDesAsset, err := c.getOwnerOneAsset(sellOrder.Account, sellOrder.DesCurrency)
	if err != nil {
		myLogger.Errorf("execTx error12:%s", err)
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", sellOrder.DesCurrency, err), CheckErr
	}
	if sellDesAsset == nil || sellDesAsset.UUID == "" {
		err = c.putAsset(&Asset{
			Owner:     sellOrder.Account,
			Currency:  sellOrder.DesCurrency,
			Count:     sellOrder.DesCount,
			LockCount: int64(0),
		})
		if err != nil {
			myLogger.Errorf("execTx error13:%s", err)
			return errors.New("Failed inserting row"), WorldStateErr
		}
	} else {
		sellDesAsset.Count = sellDesAsset.Count + sellOrder.DesCount
		err = c.putAsset(sellDesAsset)
		if err != nil {
			myLogger.Errorf("execTx error14:%s", err)
			return errors.New("Failed updating row"), WorldStateErr
		}
	}
	return nil, ErrType("")
}

// computeBalance
func (c *ExchangeChaincode) computeBalance(owner string, srcCurrency, desCurrency, rawUUID string, currentCost int64) (int64, error) {
	txs, err := c.getTXs(owner, srcCurrency, desCurrency, rawUUID)
	if err != nil {
		return 0, err
	}
	lockLog, err := c.getLockLogByParm(owner, srcCurrency, rawUUID, true)
	if err != nil {
		return 0, err
	}
	if lockLog == nil || lockLog.UUID == "" {
		return 0, errors.New("can't find lock log")
	}

	lock := lockLog.LockCount
	sumCost := int64(0)
	for _, tx := range txs {
		sumCost += tx.FinalCost
	}

	return lock - sumCost - currentCost, nil
}

// lockOrUnlockBalance lockOrUnlockBalance
func (c *ExchangeChaincode) lockOrUnlockBalance(owner string, currency, order string, count int64, islock bool) (error, ErrType) {
	asset, err := c.getOwnerOneAsset(owner, currency)
	if err != nil {
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", currency, err), CheckErr
	}
	if asset == nil || asset.UUID == "" {
		return fmt.Errorf("The user have not currency [%s]", currency), CheckErr
	}
	if islock && asset.Count < count {
		return fmt.Errorf("Currency [%s] of the user is insufficient", currency), CheckErr
	} else if !islock && asset.LockCount < count {
		return fmt.Errorf("Locked currency [%s] of the user is insufficient", currency), CheckErr
	}

	// check the order is locked/unlocked or not
	lockLog, err := c.getLockLogByParm(owner, currency, order, islock)
	if err != nil {
		return err, CheckErr
	}

	if lockLog != nil && lockLog.UUID != "" {
		return ExecedErr, CheckErr
	}

	if islock {
		asset.Count = asset.Count - count
		asset.LockCount = asset.LockCount + count
	} else {
		asset.Count = asset.Count + count
		asset.LockCount = asset.LockCount - count
	}

	err = c.putAsset(asset)
	if err != nil {
		return err, WorldStateErr
	}

	err = c.putLockLog(&LockLog{
		Owner:     owner,
		Currency:  currency,
		Order:     order,
		IsLock:    islock,
		LockCount: count,
		LockTime:  time.Now().Unix(),
	})
	if err != nil {
		return err, WorldStateErr
	}

	return nil, ErrType("")
}
