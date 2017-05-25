package main

import (
	"time"
)

const (
	CNY = "CNY"
	USD = "USD"
)

func (c *ExchangeChaincode) initCurrency() error {

	err := c.putCurrency(&Currency{
		Name:       CNY,
		Count:      0,
		LeftCount:  0,
		Creator:    "system",
		CreateTime: time.Now().Unix(),
	})
	if err != nil {
		return err
	}

	err = c.putCurrency(&Currency{
		Name:       USD,
		Count:      0,
		LeftCount:  0,
		Creator:    "system",
		CreateTime: time.Now().Unix(),
	})
	if err != nil {
		return err
	}

	return nil
}
