package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/crypto/primitives"
	logging "github.com/op/go-logging"
)

type BaseCC struct {
}

var myLogger = logging.MustGetLogger("BaseCC")

// Init is called during Deploy transaction after the container has been
// established, allowing the chaincode to initialize its internal data
func (c *BaseCC) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	return nil, nil
}

// Invoke is called for every Invoke transactions. The chaincode may change
// its state variables
func (c *BaseCC) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	myLogger.Debug("Invoke Chaincode...")

	// function, args = dealParam(function, args)
	myLogger.Debugf("Invoke function:%s ,args:%s", function, args)

	if function == "CreateTable" {
		return createTable(stub, args)
	} else if function == "InsertRow" {
		return insertRow(stub, args)
	} else if function == "ReplaceRow" {
		return replaceRow(stub, args)
	}

	myLogger.Debug("Invoke Chaincode...done")

	return nil, errors.New("Received unknown function invocation")
}

func createTable(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	myLogger.Debug("createTable...")

	if len(args) != 2 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2")
	}

	tableName := args[0]

	var columnDefinitions []*shim.ColumnDefinition
	err := json.Unmarshal([]byte(args[1]), &columnDefinitions)
	if err != nil {
		myLogger.Errorf("createTable error1:%s", err)
		return nil, err
	}

	err = stub.CreateTable(tableName, columnDefinitions)
	if err != nil {
		myLogger.Errorf("createTable error2:%s", err)
		return nil, err
	}

	myLogger.Debug("createTable...done")
	return nil, nil
}

func insertRow(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	myLogger.Debug("insertRow...")

	if len(args) != 2 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2")
	}

	tableName := args[0]

	row, err := unmarshalRow([]byte(args[1]))
	// err := json.Unmarshal([]byte(args[1]), &row)
	if err != nil {
		myLogger.Errorf("insertRow error1:%s", err)
		return nil, err
	}

	ok, err := stub.InsertRow(tableName, row)
	if err != nil {
		myLogger.Errorf("insertRow error2:%s", err)
		return nil, err
	}

	myLogger.Debug("insertRow...done")
	return strconv.AppendBool([]byte{}, ok), nil
}

func replaceRow(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	myLogger.Debug("replaceRow...")

	if len(args) != 2 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2")
	}

	tableName := args[0]

	row, err := unmarshalRow([]byte(args[1]))
	// err := json.Unmarshal([]byte(args[1]), &row)
	if err != nil {
		myLogger.Errorf("replaceRow error1:%s", err)
		return nil, err
	}

	ok, err := stub.ReplaceRow(tableName, row)
	if err != nil {
		myLogger.Errorf("replaceRow error2:%s", err)
		return nil, err
	}

	myLogger.Debug("replaceRow...done")
	return strconv.AppendBool([]byte{}, ok), nil
}

// Query is called for Query transactions. The chaincode may only read
// (but not modify) its state variables and return the result
func (c *BaseCC) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	myLogger.Debug("Query Chaincode...")

	// function, args = dealParam(function, args)
	myLogger.Debugf("Query function:%s ,args:%s", function, args)

	if function == "GetRow" {
		return getRow(stub, args)
	} else if function == "GetRows" {
		return getRows(stub, args)
	}

	myLogger.Debug("Query Chaincode...done")

	return nil, errors.New("Received unknown function query")
}
func getRow(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	tableName := args[0]

	key, err := unmarshalColumns([]byte(args[1]))
	// err := json.Unmarshal([]byte(args[1]), &key)
	if err != nil {
		myLogger.Errorf("getRow error1:%s", err)
		return nil, err
	}

	row, err := stub.GetRow(tableName, key)
	if err != nil {
		myLogger.Errorf("getRow error2:%s", err)
		return nil, err
	}

	return json.Marshal(row)
}

func getRows(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	tableName := args[0]

	key, err := unmarshalColumns([]byte(args[1]))
	// err := json.Unmarshal([]byte(args[1]), &key)
	if err != nil {
		myLogger.Errorf("getRows error1:%s", err)
		return nil, err
	}

	rowChannel, err := stub.GetRows(tableName, key)
	if err != nil {
		myLogger.Errorf("getRows error2:%s", err)
		return nil, err
	}

	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	return json.Marshal(rows)
}

func main() {
	primitives.SetSecurityLevel("SHA3", 256)
	err := shim.Start(new(BaseCC))
	if err != nil {
		myLogger.Errorf("Error starting exchange chaincode: %s", err)
	}
}

func dealParam(function string, args []string) (string, []string) {
	function_b, err := base64.StdEncoding.DecodeString(function)
	if err != nil {
		return function, args
	}
	for k, v := range args {
		arg_b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return function, args
		}
		args[k] = string(arg_b)
	}

	return string(function_b), args
}

type tempColumn struct {
	Value interface{} `json:"value"`
}
type tempRow struct {
	Columns []*tempColumn `json:"columns,omitempty"`
}

func unmarshalRow(js []byte) (row shim.Row, err error) {
	temp := tempRow{}

	err = json.Unmarshal(js, &temp)
	if err != nil {
		myLogger.Errorf("Error Unmarshal raw json: %s", err)
		return
	}

	for _, column := range temp.Columns {

		c := convColumn(*column)

		row.Columns = append(row.Columns, &c)
	}
	return
}

func unmarshalColumns(js []byte) (columns []shim.Column, err error) {
	temp := []tempColumn{}

	err = json.Unmarshal(js, &temp)
	if err != nil {
		myLogger.Errorf("Error Unmarshal columns json: %s", err)
		return
	}

	for _, column := range temp {

		c := convColumn(column)

		columns = append(columns, c)
	}
	return
}

func convColumn(temp tempColumn) shim.Column {
	c := shim.Column{}

	if value, ok := temp.Value.(map[string]interface{}); ok {
		for k, v := range value {
			switch k {
			case "String_":
				c.Value = &shim.Column_String_{String_: v.(string)}
			case "Int32":
				c.Value = &shim.Column_Int32{Int32: int32(v.(float64))}
			case "Int64":
				c.Value = &shim.Column_Int64{Int64: int64(v.(float64))}
			case "Uint32":
				c.Value = &shim.Column_Uint32{Uint32: uint32(v.(float64))}
			case "Uint64":
				c.Value = &shim.Column_Uint64{Uint64: uint64(v.(float64))}
			case "Bytes":
				c.Value = &shim.Column_Bytes{Bytes: []byte(v.(string))}
			case "Bool":
				c.Value = &shim.Column_Bool{Bool: v.(bool)}
			}
		}
	}
	return c
}
