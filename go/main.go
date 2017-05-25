package main

import (
	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/op/go-logging"
)

var myLogger = logging.MustGetLogger("exchange")

// ExchangeChaincode ExchangeChaincode
type ExchangeChaincode struct {
	stub shim.ChaincodeStubInterface
	args []string
}

// Init init
func (c *ExchangeChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	myLogger.Debug("Init Chaincode...")

	args := stub.GetStringArgs()
	if len(args) != 0 {
		return shim.Error("Incorrect number of arguments. Expecting 0")
	}

	c.stub = stub
	c.args = args

	err := c.initCurrency()
	if err != nil {
		return shim.Error(err.Error())
	}

	myLogger.Debug("Init Chaincode...done")

	return shim.Success(nil)
}

// Invoke invoke
func (c *ExchangeChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	myLogger.Debug("Invoke Chaincode...")

	function, args := stub.GetFunctionAndParameters()
	c.stub = stub
	c.args = args

	if function == "initAccount" {
		return c.initAccount()
	} else if function == "create" {
		return c.create()
	} else if function == "release" {
		return c.release()
	} else if function == "assign" {
		return c.assign()
	} else if function == "lock" {
		return c.lock()
	} else if function == "exchange" {
		return c.exchange()
	} else if function == "queryCurrencyByID" {
		return c.queryCurrencyByID()
	} else if function == "queryAllCurrency" {
		return c.queryAllCurrency()
	} else if function == "queryTxLogs" {
		return c.queryTxLogs()
	} else if function == "queryAssetByOwner" {
		return c.queryAssetByOwner()
	} else if function == "queryMyCurrency" {
		return c.queryMyCurrency()
	} else if function == "queryMyReleaseLog" {
		return c.queryMyReleaseLog()
	} else if function == "queryMyAssignLog" {
		return c.queryMyAssignLog()
	}

	myLogger.Debug("Invoke Chaincode...done")

	return shim.Success([]byte("Invalid invoke function name. Expecting \"invoke\" \"query\""))
}

func main() {
	// primitives.SetSecurityLevel("SHA3", 256)
	err := shim.Start(new(ExchangeChaincode))
	if err != nil {
		myLogger.Errorf("Error starting exchange chaincode: %s", err)
	}
}
