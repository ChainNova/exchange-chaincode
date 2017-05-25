package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

// Asset Asset
type Asset struct {
	Owner     string `json:"owner"`
	Currency  string `json:"currency"`
	Count     int64  `json:"count"`
	LockCount int64  `json:"lockCount"`
}

// Currency Currency
type Currency struct {
	ID         string `json:"id"`
	Count      int64  `json:"count"`
	LeftCount  int64  `json:"leftCount"`
	Creator    string `json:"creator"`
	CreateTime int64  `json:"createTime"`
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

// getOwnerOneAsset
func (c *ExternalityChaincode) getOwnerOneAsset(owner string, currency string) (shim.Row, *Asset, error) {
	var asset *Asset

	row, err := c.stub.GetRow(TableAssets, []shim.Column{
		shim.Column{Value: &shim.Column_String_{String_: owner}},
		shim.Column{Value: &shim.Column_String_{String_: currency}},
	})

	if len(row.Columns) > 0 {
		asset = &Asset{
			Owner:     row.Columns[0].GetString_(),
			Currency:  row.Columns[1].GetString_(),
			Count:     row.Columns[2].GetInt64(),
			LockCount: row.Columns[3].GetInt64(),
		}
	}

	return row, asset, err
}

// saveReleaseLog
func (c *ExternalityChaincode) saveReleaseLog(id string, count, now int64) error {
	ok, err := c.stub.InsertRow(TableCurrencyReleaseLog,
		shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: id}},
				&shim.Column{Value: &shim.Column_Int64{Int64: count}},
				&shim.Column{Value: &shim.Column_Int64{Int64: now}},
			},
		})
	if !ok {
		return errors.New("Currency was already releassed")
	}

	return err
}

// getCurrencyByID
func (c *ExternalityChaincode) getCurrencyByID(id string) (shim.Row, *Currency, error) {
	var currency *Currency

	row, err := c.stub.GetRow(TableCurrency, []shim.Column{
		shim.Column{Value: &shim.Column_String_{String_: id}},
	})

	if len(row.Columns) > 0 {
		currency = &Currency{
			ID:         row.Columns[0].GetString_(),
			Count:      row.Columns[1].GetInt64(),
			LeftCount:  row.Columns[2].GetInt64(),
			Creator:    row.Columns[3].GetString_(),
			CreateTime: row.Columns[4].GetInt64(),
		}
	}
	return row, currency, err
}

// saveAssignLog
func (c *ExternalityChaincode) saveAssignLog(id, reciver string, count int64) error {
	_, err := c.stub.InsertRow(TableCurrencyAssignLog,
		shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: id}},
				&shim.Column{Value: &shim.Column_String_{String_: reciver}},
				&shim.Column{Value: &shim.Column_Int64{Int64: count}},
				&shim.Column{Value: &shim.Column_Int64{Int64: time.Now().Unix()}},
			},
		})

	return err
}

// lockOrUnlockBalance lockOrUnlockBalance
func (c *ExternalityChaincode) lockOrUnlockBalance(owner string, currency, order string, count int64, islock bool) (error, ErrType) {
	row, asset, err := c.getOwnerOneAsset(owner, currency)
	if err != nil {
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", currency, err), CheckErr
	}
	if len(row.Columns) == 0 {
		return fmt.Errorf("The user have not currency [%s]", currency), CheckErr
	}
	if islock && asset.Count < count {
		return fmt.Errorf("Currency [%s] of the user is insufficient", currency), CheckErr
	} else if !islock && asset.LockCount < count {
		return fmt.Errorf("Locked currency [%s] of the user is insufficient", currency), CheckErr
	}

	// check the order is locked/unlocked or not
	lockRow, err := c.getLockLog(owner, currency, order, islock)
	if err != nil {
		return err, CheckErr
	}

	if len(lockRow.Columns) > 0 {
		return ExecedErr, CheckErr
	}

	if islock {
		row.Columns[2].Value = &shim.Column_Int64{Int64: asset.Count - count}
		row.Columns[3].Value = &shim.Column_Int64{Int64: asset.LockCount + count}
	} else {
		row.Columns[2].Value = &shim.Column_Int64{Int64: asset.Count + count}
		row.Columns[3].Value = &shim.Column_Int64{Int64: asset.LockCount - count}
	}

	_, err = c.stub.ReplaceRow(TableAssets, row)
	if err != nil {
		return err, WorldStateErr
	}

	_, err = c.stub.InsertRow(TableAssetLockLog,
		shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: owner}},
				&shim.Column{Value: &shim.Column_String_{String_: currency}},
				&shim.Column{Value: &shim.Column_String_{String_: order}},
				&shim.Column{Value: &shim.Column_Bool{Bool: islock}},
				&shim.Column{Value: &shim.Column_Int64{Int64: count}},
				&shim.Column{Value: &shim.Column_Int64{Int64: time.Now().Unix()}},
			},
		})
	if err != nil {
		return err, WorldStateErr
	}

	return nil, ErrType("")
}

// getLockLog getLockLog
func (c *ExternalityChaincode) getLockLog(owner string, currency, order string, islock bool) (shim.Row, error) {
	return c.stub.GetRow(TableAssetLockLog, []shim.Column{
		shim.Column{Value: &shim.Column_String_{String_: owner}},
		shim.Column{Value: &shim.Column_String_{String_: currency}},
		shim.Column{Value: &shim.Column_String_{String_: order}},
		shim.Column{Value: &shim.Column_Bool{Bool: islock}},
	})
}

// getTxLogByID
func (c *ExternalityChaincode) getTxLogByID(uuid string) (shim.Row, *Order, error) {
	var order Order
	row, err := c.stub.GetRow(TableTxLog2, []shim.Column{
		shim.Column{Value: &shim.Column_String_{String_: uuid}},
	})
	if len(row.Columns) > 0 {
		err = json.Unmarshal(row.Columns[1].GetBytes(), &order)
	}

	return row, &order, err
}

// execTx execTx
func (c *ExternalityChaincode) execTx(buyOrder, sellOrder *Order) (error, ErrType) {
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
	buySrcRow, buySrcAsset, err := c.getOwnerOneAsset(buyOrder.Account, buyOrder.SrcCurrency)
	if err != nil {
		myLogger.Errorf("execTx error3:%s", err)
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", buyOrder.SrcCurrency, err), CheckErr
	}
	if len(buySrcRow.Columns) == 0 {
		return fmt.Errorf("The user have not currency [%s]", buyOrder.SrcCurrency), CheckErr
	}
	buySrcRow.Columns[3].Value = &shim.Column_Int64{Int64: buySrcAsset.LockCount - buyOrder.FinalCost}
	_, err = c.stub.ReplaceRow(TableAssets, buySrcRow)
	if err != nil {
		myLogger.Errorf("execTx error4:%s", err)
		return errors.New("Failed updating row"), WorldStateErr
	}

	// buy order srcCurrency +
	buyDesRow, buyDesAsset, err := c.getOwnerOneAsset(buyOrder.Account, buyOrder.DesCurrency)
	if err != nil {
		myLogger.Errorf("execTx error5:%s", err)
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", buyOrder.DesCurrency, err), CheckErr
	}
	if len(buyDesRow.Columns) == 0 {
		_, err := c.stub.InsertRow(TableAssets,
			shim.Row{
				Columns: []*shim.Column{
					&shim.Column{Value: &shim.Column_String_{String_: buyOrder.Account}},
					&shim.Column{Value: &shim.Column_String_{String_: buyOrder.DesCurrency}},
					&shim.Column{Value: &shim.Column_Int64{Int64: buyOrder.DesCount}},
					&shim.Column{Value: &shim.Column_Int64{Int64: int64(0)}},
				},
			})
		if err != nil {
			myLogger.Errorf("execTx error6:%s", err)
			return errors.New("Failed inserting row"), WorldStateErr
		}
	} else {
		buyDesRow.Columns[2].Value = &shim.Column_Int64{Int64: buyDesAsset.Count + buyOrder.DesCount}
		_, err = c.stub.ReplaceRow(TableAssets, buyDesRow)
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
	sellSrcRow, sellSrcAsset, err := c.getOwnerOneAsset(sellOrder.Account, sellOrder.SrcCurrency)
	if err != nil {
		myLogger.Errorf("execTx error10:%s", err)
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", sellOrder.SrcCurrency, err), CheckErr
	}
	if len(sellSrcRow.Columns) == 0 {
		return fmt.Errorf("The user have not currency [%s]", sellOrder.SrcCurrency), CheckErr
	}
	sellSrcRow.Columns[3].Value = &shim.Column_Int64{Int64: sellSrcAsset.LockCount - sellOrder.FinalCost}
	_, err = c.stub.ReplaceRow(TableAssets, sellSrcRow)
	if err != nil {
		myLogger.Errorf("execTx error11:%s", err)
		return errors.New("Failed updating row"), WorldStateErr
	}

	// sell order desCurrency +
	sellDesRow, sellDesAsset, err := c.getOwnerOneAsset(sellOrder.Account, sellOrder.DesCurrency)
	if err != nil {
		myLogger.Errorf("execTx error12:%s", err)
		return fmt.Errorf("Failed retrieving asset [%s] of the user: [%s]", sellOrder.DesCurrency, err), CheckErr
	}
	if len(sellDesRow.Columns) == 0 {
		_, err = c.stub.InsertRow(TableAssets,
			shim.Row{
				Columns: []*shim.Column{
					&shim.Column{Value: &shim.Column_String_{String_: sellOrder.Account}},
					&shim.Column{Value: &shim.Column_String_{String_: sellOrder.DesCurrency}},
					&shim.Column{Value: &shim.Column_Int64{Int64: sellOrder.DesCount}},
					&shim.Column{Value: &shim.Column_Int64{Int64: 0}},
				},
			})
		if err != nil {
			myLogger.Errorf("execTx error13:%s", err)
			return errors.New("Failed inserting row"), WorldStateErr
		}
	} else {
		sellDesRow.Columns[2].Value = &shim.Column_Int64{Int64: sellDesAsset.Count + sellOrder.DesCount}
		_, err = c.stub.ReplaceRow(TableAssets, sellDesRow)
		if err != nil {
			myLogger.Errorf("execTx error14:%s", err)
			return errors.New("Failed updating row"), WorldStateErr
		}
	}
	return nil, ErrType("")
}

// getTXs
func (c *ExternalityChaincode) getTXs(owner string, srcCurrency, desCurrency, rawOrder string) ([]shim.Row, []*Order, error) {
	rowChannel, err := c.stub.GetRows(TableTxLog, []shim.Column{
		shim.Column{Value: &shim.Column_String_{String_: owner}},
		shim.Column{Value: &shim.Column_String_{String_: srcCurrency}},
		shim.Column{Value: &shim.Column_String_{String_: desCurrency}},
		shim.Column{Value: &shim.Column_String_{String_: rawOrder}},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("getTXs operation failed. %s", err)
	}

	var rows []shim.Row
	var orders []*Order
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)

				var order Order
				err := json.Unmarshal(row.Columns[4].GetBytes(), &order)
				if err != nil {
					return nil, nil, fmt.Errorf("Error unmarshaling JSON: %s", err)
				}

				orders = append(orders, &order)
			}
		}
		if rowChannel == nil {
			break
		}
	}
	return rows, orders, nil
}

// computeBalance
func (c *ExternalityChaincode) computeBalance(owner string, srcCurrency, desCurrency, rawUUID string, currentCost int64) (int64, error) {
	_, txs, err := c.getTXs(owner, srcCurrency, desCurrency, rawUUID)
	if err != nil {
		return 0, err
	}
	row, err := c.getLockLog(owner, srcCurrency, rawUUID, true)
	if err != nil {
		return 0, err
	}
	if len(row.Columns) == 0 {
		return 0, errors.New("can't find lock log")
	}

	lock := row.Columns[4].GetInt64()
	sumCost := int64(0)
	for _, tx := range txs {
		sumCost += tx.FinalCost
	}

	return lock - sumCost - currentCost, nil
}

// saveTxLog
func (c *ExternalityChaincode) saveTxLog(buyOrder, sellOrder *Order) error {
	buyJson, _ := json.Marshal(buyOrder)
	sellJson, _ := json.Marshal(sellOrder)

	_, err := c.stub.InsertRow(TableTxLog, shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: buyOrder.Account}},
			&shim.Column{Value: &shim.Column_String_{String_: buyOrder.SrcCurrency}},
			&shim.Column{Value: &shim.Column_String_{String_: buyOrder.DesCurrency}},
			&shim.Column{Value: &shim.Column_String_{String_: buyOrder.RawUUID}},
			&shim.Column{Value: &shim.Column_Bytes{Bytes: buyJson}},
		},
	})
	if err != nil {
		return err
	}

	_, err = c.stub.InsertRow(TableTxLog2, shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: buyOrder.UUID}},
			&shim.Column{Value: &shim.Column_Bytes{Bytes: buyJson}},
		},
	})
	if err != nil {
		return err
	}

	_, err = c.stub.InsertRow(TableTxLog, shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: sellOrder.Account}},
			&shim.Column{Value: &shim.Column_String_{String_: sellOrder.SrcCurrency}},
			&shim.Column{Value: &shim.Column_String_{String_: sellOrder.DesCurrency}},
			&shim.Column{Value: &shim.Column_String_{String_: sellOrder.RawUUID}},
			&shim.Column{Value: &shim.Column_Bytes{Bytes: sellJson}},
		},
	})
	if err != nil {
		return err
	}

	_, err = c.stub.InsertRow(TableTxLog2, shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: sellOrder.UUID}},
			&shim.Column{Value: &shim.Column_Bytes{Bytes: sellJson}},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

// getOwnerAllAsset
func (c *ExternalityChaincode) getOwnerAllAsset(owner string) ([]shim.Row, []*Asset, error) {
	rowChannel, err := c.stub.GetRows(TableAssets, []shim.Column{
		shim.Column{Value: &shim.Column_String_{String_: owner}},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("getOwnerAllAsset operation failed. %s", err)
	}

	var rows []shim.Row
	var assets []*Asset
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)

				asset := &Asset{
					Owner:     row.Columns[0].GetString_(),
					Currency:  row.Columns[1].GetString_(),
					Count:     row.Columns[2].GetInt64(),
					LockCount: row.Columns[3].GetInt64(),
				}
				assets = append(assets, asset)
			}
		}
		if rowChannel == nil {
			break
		}
	}
	return rows, assets, nil
}

// getMyCurrency
func (c *ExternalityChaincode) getMyCurrency(owner string) ([]*Currency, error) {
	_, infos, err := c.getAllCurrency()
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, NoDataErr
	}

	var currencys []*Currency
	for _, v := range infos {
		if owner == v.Creator {
			currencys = append(currencys, v)
		}
	}

	return currencys, nil
}

// getAllCurrency
func (c *ExternalityChaincode) getAllCurrency() ([]shim.Row, []*Currency, error) {
	rowChannel, err := c.stub.GetRows(TableCurrency, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("getRows operation failed. %s", err)
	}
	var rows []shim.Row
	var infos []*Currency
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)

				info := new(Currency)
				info.ID = row.Columns[0].GetString_()
				info.Count = row.Columns[1].GetInt64()
				info.LeftCount = row.Columns[2].GetInt64()
				info.Creator = row.Columns[3].GetString_()
				info.CreateTime = row.Columns[4].GetInt64()

				infos = append(infos, info)
			}
		}
		if rowChannel == nil {
			break
		}
	}
	return rows, infos, nil
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
