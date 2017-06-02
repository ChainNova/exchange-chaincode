package main

import (
	"encoding/json"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/util"
)

func createTable(stub shim.ChaincodeStubInterface, tableName string, columnDefinitions []*shim.ColumnDefinition) error {
	js, err := json.Marshal(columnDefinitions)
	if err != nil {
		return err
	}

	_, err = stub.InvokeChaincode(chaincodeName, util.ToChaincodeArgs("CreateTable", tableName, string(js)))

	return err
}

func insertRow(stub shim.ChaincodeStubInterface, tableName string, row shim.Row) (bool, error) {
	js, err := json.Marshal(row)
	if err != nil {
		return false, err
	}

	b, err := stub.InvokeChaincode(chaincodeName, util.ToChaincodeArgs("InsertRow", tableName, string(js)))
	if err != nil {
		return false, err
	}

	return strconv.ParseBool(string(b))
}

func replaceRow(stub shim.ChaincodeStubInterface, tableName string, row shim.Row) (bool, error) {
	js, err := json.Marshal(row)
	if err != nil {
		return false, err
	}

	b, err := stub.InvokeChaincode(chaincodeName, util.ToChaincodeArgs("ReplaceRow", tableName, string(js)))
	if err != nil {
		return false, err
	}

	return strconv.ParseBool(string(b))
}

func getRow(stub shim.ChaincodeStubInterface, tableName string, key []shim.Column) ([]byte, error) {
	js, err := json.Marshal(key)
	if err != nil {
		return nil, err
	}

	return stub.InvokeChaincode(chaincodeName, util.ToChaincodeArgs("GetRow", tableName, string(js)))
}

func getRows(stub shim.ChaincodeStubInterface, tableName string, key []shim.Column) ([]byte, error) {
	js, err := json.Marshal(key)
	if err != nil {
		return nil, err
	}

	return stub.InvokeChaincode(chaincodeName, util.ToChaincodeArgs("GetRows", tableName, string(js)))
}
