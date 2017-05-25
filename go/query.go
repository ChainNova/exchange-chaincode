package main

import (
	"encoding/json"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// queryCurrency
func (c *ExchangeChaincode) queryCurrencyByID() pb.Response {
	myLogger.Debug("queryCurrency...")

	if len(c.args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
	}

	name := c.args[0]

	currency, err := c.getCurrencyByName(name)
	if err != nil {
		myLogger.Errorf("queryCurrencyByID error1:%s", err)
		return shim.Error(err.Error())
	}
	if currency == nil {
		return shim.Error(NoDataErr.Error())
	}
	payload, err := json.Marshal(&currency)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(payload)
}

// queryAllCurrency
func (c *ExchangeChaincode) queryAllCurrency() pb.Response {
	myLogger.Debug("queryCurrency...")

	if len(c.args) != 0 {
		return shim.Error("Incorrect number of arguments. Expecting 0")
	}

	infos, err := c.getAllCurrency()
	if err != nil {
		return shim.Error(err.Error())
	}
	if len(infos) == 0 {
		return shim.Error(NoDataErr.Error())
	}

	payload, err := json.Marshal(&infos)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(payload)
}

// queryTxLogs
func (c *ExchangeChaincode) queryTxLogs() pb.Response {
	myLogger.Debug("queryTxLogs...")

	if len(c.args) != 0 {
		return shim.Error("Incorrect number of arguments. Expecting 0")
	}

	infos, err := c.getAllTxLog()
	if err != nil {
		return shim.Error(err.Error())
	}
	if len(infos) == 0 {
		return shim.Error(NoDataErr.Error())
	}

	payload, err := json.Marshal(&infos)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(payload)
}

// queryAssetByOwner
func (c *ExchangeChaincode) queryAssetByOwner() pb.Response {
	myLogger.Debug("queryAssetByOwner...")

	if len(c.args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
	}

	owner := c.args[0]
	assets, err := c.getOwnerAllAsset(owner)
	if err != nil {
		myLogger.Errorf("queryAssetByOwner error1:%s", err)
		return shim.Error(err.Error())
	}
	if len(assets) == 0 {
		return shim.Error(NoDataErr.Error())
	}
	payload, err := json.Marshal(&assets)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(payload)
}

// queryMyCurrency
func (c *ExchangeChaincode) queryMyCurrency() pb.Response {
	myLogger.Debug("queryCurrency...")

	if len(c.args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
	}

	owner := c.args[0]
	currencys, err := c.getMyCurrency(owner)
	if err != nil {
		return shim.Error(err.Error())
	}

	payload, err := json.Marshal(&currencys)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(payload)
}

// queryReleaseLog
func (c *ExchangeChaincode) queryMyReleaseLog() pb.Response {
	myLogger.Debug("queryMyReleaseLog...")

	if len(c.args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
	}
	owner := c.args[0]
	logs, err := c.getMyReleaseLog(owner)
	if err != nil {
		return shim.Error(err.Error())
	}

	payload, err := json.Marshal(logs)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(payload)
}

// queryMyAssignLog
func (c *ExchangeChaincode) queryMyAssignLog() pb.Response {
	myLogger.Debug("queryAssignLog...")

	if len(c.args) != 1 {
		return shim.Error("Incorrect number of arguments. Expecting 1")
	}
	owner := c.args[0]
	logToMe, err := c.getFromAssignLog(owner)
	if err != nil {
		return shim.Error(err.Error())
	}

	logMeTo, err := c.getToAssignLog(owner)
	if err != nil {
		return shim.Error(err.Error())
	}

	logs := &struct {
		ToMe []*AssignLog `json:"toMe"`
		MeTo []*AssignLog `json:"meTo"`
	}{
		ToMe: logToMe,
		MeTo: logMeTo,
	}

	payload, err := json.Marshal(logs)
	if err != nil {
		return shim.Error(err.Error())
	}

	return shim.Success(payload)
}
