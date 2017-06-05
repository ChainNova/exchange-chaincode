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

func getRow(stub shim.ChaincodeStubInterface, tableName string, key []shim.Column) (shim.Row, error) {
	js, err := json.Marshal(key)
	if err != nil {
		return shim.Row{}, err
	}

	b, err := stub.InvokeChaincode(chaincodeName, util.ToChaincodeArgs("GetRow", tableName, string(js)))
	if err != nil {
		return shim.Row{}, err
	}

	return unmarshalRow(b)
}

func getRows(stub shim.ChaincodeStubInterface, tableName string, key []shim.Column) ([]shim.Row, error) {
	js, err := json.Marshal(key)
	if err != nil {
		return nil, err
	}

	b, err := stub.InvokeChaincode(chaincodeName, util.ToChaincodeArgs("GetRows", tableName, string(js)))
	if err != nil {
		return nil, err
	}
	return unmarshalRows(b)
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

func unmarshalRows(js []byte) (rows []shim.Row, err error) {
	temp := []tempRow{}

	err = json.Unmarshal(js, &temp)
	if err != nil {
		myLogger.Errorf("Error Unmarshal raw json: %s", err)
		return
	}

	for _, row := range temp {
		r := shim.Row{}
		for _, column := range row.Columns {

			c := convColumn(*column)

			r.Columns = append(r.Columns, &c)
		}

		rows = append(rows, r)
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
