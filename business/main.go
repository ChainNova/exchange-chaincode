package main

import (
	"errors"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/crypto/primitives"
	"github.com/op/go-logging"
)

var myLogger = logging.MustGetLogger("externality")

// ExternalityChaincode ExternalityChaincode
type ExternalityChaincode struct {
	stub shim.ChaincodeStubInterface
	args []string
}

// Init is called during Deploy transaction after the container has been
// established, allowing the chaincode to initialize its internal data
func (c *ExternalityChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	myLogger.Debug("Init Chaincode...")

	function, args = dealParam(function, args)
	myLogger.Debugf("Init function:%s ,args:%s", function, args)

	c.stub = stub
	c.args = args

	if len(args) != 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0")
	}

	err := c.CreateTable()
	if err != nil {
		myLogger.Errorf("Init error [CreateTable]:%s", err)
		return nil, err
	}

	err = c.InitTable()
	if err != nil {
		myLogger.Errorf("Init error [InitTable]:%s", err)
		return nil, err
	}
	myLogger.Debug("Init Chaincode...done")

	return nil, nil
}

// Invoke is called for every Invoke transactions. The chaincode may change
// its state variables
func (c *ExternalityChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	myLogger.Debug("Invoke Chaincode...")

	function, args = dealParam(function, args)
	myLogger.Debugf("Invoke function:%s ,args:%s", function, args)

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
	}

	myLogger.Debug("Invoke Chaincode...done")

	return nil, errors.New("Received unknown function invocation")
}

// Query is called for Query transactions. The chaincode may only read
// (but not modify) its state variables and return the result
func (c *ExternalityChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	myLogger.Debug("Query Chaincode...")

	function, args = dealParam(function, args)
	myLogger.Debugf("Query function:%s ,args:%s", function, args)

	c.stub = stub
	c.args = args

	if function == "queryCurrencyByID" {
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

	myLogger.Debug("Query Chaincode...done")

	return nil, errors.New("Received unknown function query")
}

func main() {
	primitives.SetSecurityLevel("SHA3", 256)
	err := shim.Start(new(ExternalityChaincode))
	if err != nil {
		myLogger.Errorf("Error starting exchange chaincode: %s", err)
	}
}
