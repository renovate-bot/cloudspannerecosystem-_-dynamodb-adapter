// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ahmetb/go-linq"
	"github.com/cloudspannerecosystem/dynamodb-adapter/config"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	otelgo "github.com/cloudspannerecosystem/dynamodb-adapter/otel"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/errors"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
	translator "github.com/cloudspannerecosystem/dynamodb-adapter/translator/utils"
	"github.com/cloudspannerecosystem/dynamodb-adapter/utils"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

var base64Regexp = regexp.MustCompile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$")

const (
	SpannerBatchGetAnnotation     = "Calling SpannerBatchGet Method"
	SpannerGetAnnotation          = "Calling SpannerGet Method"
	ExecuteSpannerQueryAnnotation = "Calling ExecuteSpannerQuery Method"
	SpannerPutAnnotation          = "Calling SpannerPut Method"
	SpannerDeleteAnnotation       = "Calling SpannerDelete Method"
	SpannerBatchDeleteAnnotation  = "Calling SpannerBatchDelete Method"
	SpannerAddAnnotation          = "Calling SpannerAdd Method"
	SpannerDelAnnotation          = "Calling SpannerDel Method"
	SpannerRemoveAnnotation       = "Calling SpannerRemove Method"
	SpannerBatchPutAnnotation     = "Calling SpannerBatchPut Method"
)

// SpannerBatchGet - fetch all rows
func (s Storage) SpannerBatchGet(ctx context.Context, tableName string, pKeys, sKeys []interface{}, projectionCols []string) ([]map[string]interface{}, error) {
	otelgo.AddAnnotation(ctx, SpannerBatchGetAnnotation)
	var keySet []spanner.KeySet

	for i := range pKeys {
		if len(sKeys) == 0 || sKeys[i] == nil {
			keySet = append(keySet, spanner.Key{pKeys[i]})
		} else {
			keySet = append(keySet, spanner.Key{pKeys[i], sKeys[i]})
		}
	}
	if len(projectionCols) == 0 {
		var ok bool
		projectionCols, ok = models.TableColumnMap[utils.ChangeTableNameForSpanner(tableName)]
		if !ok {
			return nil, errors.New("ResourceNotFoundException", tableName)
		}
	}
	colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(tableName)]
	if !ok {
		return nil, errors.New("ResourceNotFoundException", tableName)
	}
	tableName = utils.ChangeTableNameForSpanner(tableName)
	client := s.getSpannerClient(tableName)
	itr := client.Single().Read(ctx, tableName, spanner.KeySets(keySet...), projectionCols)
	defer itr.Stop()
	allRows := []map[string]interface{}{}
	for {
		r, err := itr.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, errors.New("ValidationException", err)
		}
		singleRow, _, err := parseRow(r, colDDL)
		if err != nil {
			return nil, err
		}
		if len(singleRow) > 0 {
			allRows = append(allRows, singleRow)
		}
	}
	return allRows, nil
}

// SpannerGet - get with spanner
func (s Storage) SpannerGet(ctx context.Context, tableName string, pKeys, sKeys interface{}, projectionCols []string) (map[string]interface{}, map[string]interface{}, error) {
	otelgo.AddAnnotation(ctx, SpannerGetAnnotation)
	var key spanner.Key
	if sKeys == nil {
		key = spanner.Key{pKeys}
	} else {
		key = spanner.Key{pKeys, sKeys}
	}
	if len(projectionCols) == 0 {
		var ok bool
		projectionCols, ok = models.TableColumnMap[utils.ChangeTableNameForSpanner(tableName)]
		if !ok {
			return nil, nil, errors.New("ResourceNotFoundException", tableName)
		}
	}
	colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(tableName)]
	if !ok {
		return nil, nil, errors.New("ResourceNotFoundException", tableName)
	}
	tableName = utils.ChangeTableNameForSpanner(tableName)
	client := s.getSpannerClient(tableName)
	row, err := client.Single().ReadRow(ctx, tableName, key, projectionCols)
	if err := errors.AssignError(err); err != nil {
		return nil, nil, errors.New("ResourceNotFoundException", tableName, key, err)
	}

	return parseRow(row, colDDL)
}

// ExecuteSpannerQuery - this will execute query on spanner database
func (s Storage) ExecuteSpannerQuery(ctx context.Context, table string, cols []string, isCountQuery bool, stmt spanner.Statement) ([]map[string]interface{}, error) {
	otelgo.AddAnnotation(ctx, ExecuteSpannerQueryAnnotation)
	colDLL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(table)]

	if !ok {
		return nil, errors.New("ResourceNotFoundException", table)
	}

	itr := s.getSpannerClient(table).Single().WithTimestampBound(spanner.ExactStaleness(time.Second*10)).Query(ctx, stmt)

	defer itr.Stop()
	allRows := []map[string]interface{}{}
	for {
		r, err := itr.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.New("ResourceNotFoundException", err)
		}
		if isCountQuery {
			var count int64
			err := r.ColumnByName("count", &count)
			if err != nil {
				return nil, err
			}
			singleRow := map[string]interface{}{"Count": count, "Items": []map[string]interface{}{}, "LastEvaluatedKey": nil}
			allRows = append(allRows, singleRow)
			break
		}
		singleRow, _, err := parseRow(r, colDLL)
		if err != nil {
			return nil, err
		}
		allRows = append(allRows, singleRow)
	}

	return allRows, nil
}

// SpannerPut - Spanner put insert a single object
func (s Storage) SpannerPut(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition, spannerRow map[string]interface{}) (map[string]interface{}, error) {
	otelgo.AddAnnotation(ctx, SpannerPutAnnotation)
	update := map[string]interface{}{}
	_, err := s.getSpannerClient(table).ReadWriteTransaction(ctx, func(ctx context.Context, t *spanner.ReadWriteTransaction) error {
		tmpMap := map[string]interface{}{}
		for k, v := range m {
			switch v := v.(type) {
			case []interface{}:
				// Serialize lists to JSON
				jsonValue, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("failed to serialize column %s to JSON: %v", k, err)
				}
				tmpMap[k] = string(jsonValue)
			default:
				// Assign other types as-is
				tmpMap[k] = v
			}
		}
		if len(eval.Attributes) > 0 || expr != nil {
			status, err := evaluateConditionalExpression(ctx, t, table, tmpMap, eval, expr)
			if err != nil {
				return err
			}
			if !status {
				return errors.New("ConditionalCheckFailedException", eval, expr)
			}
		}
		table = utils.ChangeTableNameForSpanner(table)
		for k, v := range tmpMap {
			update[k] = v
		}
		return s.performPutOperation(ctx, t, table, tmpMap, spannerRow)
	})
	return update, err
}

// SpannerDelete - this will delete the data
func (s Storage) SpannerDelete(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition) error {
	otelgo.AddAnnotation(ctx, SpannerDeleteAnnotation)
	_, err := s.getSpannerClient(table).ReadWriteTransaction(ctx, func(ctx context.Context, t *spanner.ReadWriteTransaction) error {
		tmpMap := map[string]interface{}{}
		for k, v := range m {
			tmpMap[k] = v
		}
		if len(eval.Attributes) > 0 || expr != nil {
			status, err := evaluateConditionalExpression(ctx, t, table, tmpMap, eval, expr)
			if err != nil {
				return err
			}
			if !status {
				return errors.New("ConditionalCheckFailedException", tmpMap, expr)
			}
		}
		tableConf, err := config.GetTableConf(table)
		if err != nil {
			return err
		}
		table = utils.ChangeTableNameForSpanner(table)

		pKey := tableConf.PartitionKey
		pValue, ok := tmpMap[pKey]
		if !ok {
			return errors.New("ResourceNotFoundException", pKey)
		}
		var key spanner.Key
		sKey := tableConf.SortKey
		if sKey != "" {
			sValue, ok := tmpMap[sKey]
			if !ok {
				return errors.New("ResourceNotFoundException", pKey)
			}
			key = spanner.Key{pValue, sValue}

		} else {
			key = spanner.Key{pValue}
		}

		mutation := spanner.Delete(table, key)
		err = t.BufferWrite([]*spanner.Mutation{mutation})
		if e := errors.AssignError(err); e != nil {
			return e
		}
		return nil
	})
	return err
}

// SpannerBatchDelete - this delete the data in batch
func (s Storage) SpannerBatchDelete(ctx context.Context, table string, keys []map[string]interface{}) error {
	otelgo.AddAnnotation(ctx, SpannerBatchDeleteAnnotation)
	tableConf, err := config.GetTableConf(table)
	if err != nil {
		return err
	}
	table = utils.ChangeTableNameForSpanner(table)

	pKey := tableConf.PartitionKey
	ms := make([]*spanner.Mutation, len(keys))
	sKey := tableConf.SortKey
	for i := 0; i < len(keys); i++ {
		m := keys[i]
		pValue, ok := m[pKey]
		if !ok {
			return errors.New("ResourceNotFoundException", pKey)
		}
		var key spanner.Key
		if sKey != "" {
			sValue, ok := m[sKey]
			if !ok {
				return errors.New("ResourceNotFoundException", sKey)
			}
			key = spanner.Key{pValue, sValue}

		} else {
			key = spanner.Key{pValue}
		}
		ms[i] = spanner.Delete(table, key)
	}
	_, err = s.getSpannerClient(table).Apply(ctx, ms)
	if err != nil {
		return errors.New("ResourceNotFoundException", err)
	}
	return nil
}

// SpannerAdd - Spanner Add functionality like update attribute
func (s Storage) SpannerAdd(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition) (map[string]interface{}, error) {
	otelgo.AddAnnotation(ctx, SpannerAddAnnotation)
	tableConf, err := config.GetTableConf(table)
	if err != nil {
		return nil, err
	}
	colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(table)]
	if !ok {
		return nil, errors.New("ResourceNotFoundException", table)
	}
	pKey := tableConf.PartitionKey
	var pValue interface{}
	var sValue interface{}
	sKey := tableConf.SortKey

	cols := []string{}
	var key spanner.Key
	var m1 = make(map[string]interface{})

	for k, v := range m {
		m1[k] = v
		if k == pKey {
			pValue = v
			delete(m, k)
			continue
		}
		if k == sKey {
			delete(m, k)
			sValue = v
			continue
		}
		cols = append(cols, k)
	}
	if sValue != nil {
		key = spanner.Key{pValue, sValue}
	} else {
		key = spanner.Key{pValue}
	}

	updatedObj := map[string]interface{}{}
	_, err = s.getSpannerClient(table).ReadWriteTransaction(ctx, func(ctx context.Context, t *spanner.ReadWriteTransaction) error {
		tmpMap := map[string]interface{}{}
		for k, v := range m1 {
			tmpMap[k] = v
		}

		if len(eval.Attributes) > 0 || expr != nil {
			status, _ := evaluateConditionalExpression(ctx, t, table, tmpMap, eval, expr)
			if !status {
				return errors.New("ConditionalCheckFailedException")
			}
		}
		table = utils.ChangeTableNameForSpanner(table)

		r, err := t.ReadRow(ctx, table, key, cols)
		if err != nil {
			return errors.New("ResourceNotFoundException", err)
		}
		rs, _, err := parseRow(r, colDDL)
		if err != nil {
			return err
		}

		for k, v := range tmpMap {
			if existingVal, ok := rs[k]; ok {
				switch existingVal := existingVal.(type) {
				case int64:
					// Handling int64
					v2, ok := v.(float64)
					if !ok {
						strV, ok := v.(string)
						if !ok {
							return errors.New("ValidationException", reflect.TypeOf(v).String())
						}
						v2, err = strconv.ParseFloat(strV, 64)
						if err != nil {
							return errors.New("ValidationException", reflect.TypeOf(v).String())
						}
					}
					tmpMap[k] = existingVal + int64(v2)

				case float64:
					// Handling float64
					v2, ok := v.(float64)
					if !ok {
						strV, ok := v.(string)
						if !ok {
							return errors.New("ValidationException", reflect.TypeOf(v).String())
						}
						v2, err = strconv.ParseFloat(strV, 64)
						if err != nil {
							return errors.New("ValidationException", reflect.TypeOf(v).String())
						}
					}
					tmpMap[k] = existingVal + v2

				default:
					logger.LogDebug(reflect.TypeOf(v).String())
				}
			}
		}

		// Add partition and sort keys to the updated object
		tmpMap[pKey] = pValue
		if sValue != nil {
			tmpMap[sKey] = sValue
		}

		ddl := models.TableDDL[table]
		for k, v := range tmpMap {
			updatedObj[k] = v
			t, ok := ddl[k]
			if t == "BYTES(MAX)" && ok {
				ba, err := json.Marshal(v)
				if err != nil {
					return errors.New("ValidationException", err)
				}
				tmpMap[k] = ba
			}
			switch v := v.(type) {
			case []interface{}:
				// Serialize lists to JSON
				jsonValue, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("failed to serialize column %s to JSON: %v", k, err)
				}
				tmpMap[k] = string(jsonValue)
			default:
				// Assign other types as-is
				tmpMap[k] = v
			}
		}

		mutation := spanner.InsertOrUpdateMap(table, tmpMap)
		err = t.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return errors.New("ResourceNotFoundException", err)
		}

		return nil
	})

	return updatedObj, err
}

func (s Storage) SpannerDel(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition) error {
	otelgo.AddAnnotation(ctx, SpannerDelAnnotation)
	tableConf, err := config.GetTableConf(table)
	if err != nil {
		return err
	}
	colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(table)]
	if !ok {
		return errors.New("ResourceNotFoundException", table)
	}
	pKey := tableConf.PartitionKey
	var pValue interface{}
	var sValue interface{}
	sKey := tableConf.SortKey

	cols := []string{}
	var key spanner.Key
	var m1 = make(map[string]interface{})

	// Process primary and secondary keys
	for k, v := range m {
		m1[k] = v
		if k == pKey {
			pValue = v
			delete(m, k)
			continue
		}
		if k == sKey {
			delete(m, k)
			sValue = v
			continue
		}
		cols = append(cols, k)
	}
	if sValue != nil {
		key = spanner.Key{pValue, sValue}
	} else {
		key = spanner.Key{pValue}
	}

	_, err = s.getSpannerClient(table).ReadWriteTransaction(ctx, func(ctx context.Context, t *spanner.ReadWriteTransaction) error {
		tmpMap := map[string]interface{}{}
		for k, v := range m {
			tmpMap[k] = v
		}

		// Evaluate conditional expressions
		if len(eval.Attributes) > 0 || expr != nil {
			status, _ := evaluateConditionalExpression(ctx, t, table, m1, eval, expr)
			if !status {
				return errors.New("ConditionalCheckFailedException")
			}
		}

		table = utils.ChangeTableNameForSpanner(table)

		// Read the row
		r, err := t.ReadRow(ctx, table, key, cols)
		if err != nil {
			return errors.New("ResourceNotFoundException", err)
		}
		rs, _, err := parseRow(r, colDDL)
		if err != nil {
			return err
		}

		// Process and merge data for deletion
		for k, v := range tmpMap {
			v1, ok := rs[k]
			if ok {
				switch v1.(type) {
				case []interface{}:
					var ifaces1 []interface{}
					ba, ok := v.([]byte)
					if ok {
						err = json.Unmarshal(ba, &ifaces1)
						if err != nil {
							logger.LogError(err, string(ba))
						}
					} else {
						ifaces1 = v.([]interface{})
					}
					m1 := map[interface{}]struct{}{}
					ifaces := v1.([]interface{})
					for i := 0; i < len(ifaces); i++ {
						m1[reflect.ValueOf(ifaces[i]).Interface()] = struct{}{}
					}
					for i := 0; i < len(ifaces1); i++ {

						delete(m1, reflect.ValueOf(ifaces1[i]).Interface())
					}
					ifaces = []interface{}{}
					for k := range m1 {
						ifaces = append(ifaces, k)
					}
					tmpMap[k] = ifaces
				default:
					logger.LogDebug(reflect.TypeOf(v).String())
				}
			}
		}
		tmpMap[pKey] = pValue
		if sValue != nil {
			tmpMap[sKey] = sValue
		}

		ddl := models.TableDDL[table]

		// Handle special cases like BYTES(MAX) columns
		for k, v := range tmpMap {
			t, ok := ddl[k]
			if t == "BYTES(MAX)" && ok {
				ba, err := json.Marshal(v)
				if err != nil {
					return errors.New("ValidationException", err)
				}
				tmpMap[k] = ba
			}
		}

		// Perform the delete operation by updating the row
		mutation := spanner.InsertOrUpdateMap(table, tmpMap)
		err = t.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return errors.New("ResourceNotFoundException", err)
		}
		return nil
	})
	return err
}

// SpannerRemove - Spanner Remove functionality like update attribute
func (s Storage) SpannerRemove(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition, colsToRemove []string, oldRes map[string]interface{}) error {
	otelgo.AddAnnotation(ctx, SpannerRemoveAnnotation)
	_, err := s.getSpannerClient(table).ReadWriteTransaction(ctx, func(ctx context.Context, t *spanner.ReadWriteTransaction) error {
		tmpMap := map[string]interface{}{}
		for k, v := range m {
			tmpMap[k] = v
		}
		if len(eval.Attributes) > 0 || expr != nil {
			status, _ := evaluateConditionalExpression(ctx, t, table, m, eval, expr)
			if !status {
				return errors.New("ConditionalCheckFailedException")
			}
		}

		// Process each removal target
		for _, target := range colsToRemove {
			if strings.Contains(target, "[") && strings.Contains(target, "]") {
				// Handle list element removal
				listAttr, idx := utils.ParseListRemoveTarget(target)
				if val, ok := oldRes[listAttr]; ok {
					if list, ok := val.([]interface{}); ok {
						oldRes[listAttr] = utils.RemoveListElement(list, idx)
						tmpMap[listAttr] = oldRes[listAttr]
					}
				}
			} else {
				// Direct column removal from oldRes
				delete(oldRes, target)
			}
		}
		// Handle special cases like BYTES(MAX) columns
		for k, v := range tmpMap {
			switch v := v.(type) {
			case []interface{}:
				// Serialize lists to JSON
				jsonValue, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("failed to serialize column %s to JSON: %v", k, err)
				}
				tmpMap[k] = string(jsonValue)
			default:
				// Assign other types as-is
				tmpMap[k] = v
			}
		}

		table = utils.ChangeTableNameForSpanner(table)
		mutation := spanner.InsertOrUpdateMap(table, tmpMap)
		err := t.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return errors.New("ResourceNotFoundException", err)
		}
		return nil
	})
	return err
}

// SpannerBatchPut - this insert or update data in batch
func (s Storage) SpannerBatchPut(ctx context.Context, table string, m []map[string]interface{}, spannerRow []map[string]interface{}) error {
	otelgo.AddAnnotation(ctx, SpannerBatchPutAnnotation)
	mutations := make([]*spanner.Mutation, len(m))
	ddl := models.TableDDL[utils.ChangeTableNameForSpanner(table)]
	table = utils.ChangeTableNameForSpanner(table)
	for i := 0; i < len(m); i++ {
		for k, v := range m[i] {
			// t, ok := ddl[k]
			if strings.Contains(k, ".") {
				pathfeilds := strings.Split(k, ".")
				colName := pathfeilds[0]
				t, ok := ddl[colName]
				if t == "JSON" || t == "M" && ok {

					var err error
					// Store the updated JSON in the map
					m[i][colName], err = updateMapColumnObject(spannerRow[i], colName, k, v)
					if err != nil {
						return errors.New("Error updating the Map object:", err)
					}
					delete(m[i], k)
				}
			} else {
				t, ok := ddl[k]
				if t == "BYTES(MAX)" || t == "B" && ok {
					ba, err := json.Marshal(v)
					if err != nil {
						return errors.New("ValidationException", err)
					}
					m[i][k] = ba
				}
				if t == "M" && ok {
					ba, err := json.MarshalIndent(v, "", "  ")
					if err != nil {
						return errors.New("ValidationException", err)
					}
					m[i][k] = string(ba)
				}
				if t == "L" && ok {
					list, ok := v.([]interface{})
					if !ok {
						return errors.New("invalid list format")
					}

					jsonData, err := json.Marshal(list)
					if err != nil {
						return fmt.Errorf("error marshaling list to JSON: %w", err)
					}

					m[i][k] = string(jsonData)
				}

			}
		}
		mutations[i] = spanner.InsertOrUpdateMap(table, m[i])
	}
	_, err := s.getSpannerClient(table).Apply(ctx, mutations)
	if err != nil {
		return errors.New("ResourceNotFoundException", err.Error())
	}
	return nil
}

// performPutOperation handles the insertion or update of data in a specified Spanner table.
// It processes the provided mapping to account for JSON fields and handles it accordingly.
//
// Parameters:
// - ctx: The context for managing timeouts and cancellation signals.
// - t: A pointer to a ReadWriteTransaction that allows for transaction operations.
// - table: The name of the table where the data will be inserted or updated.
// - m: A map containing field name-value pairs to be written to the database.
// - spannerRow: A map representing the current state of the row in the database, used for reading nested JSON fields.
//
// Returns:
// - An error if the operation fails or nil if the operation succeeds.
func (s Storage) performPutOperation(ctx context.Context, t *spanner.ReadWriteTransaction, table string, m map[string]interface{}, spannerRow map[string]interface{}) error {
	ddl := models.TableDDL[table]
	newMap := m
	for k, v := range m {
		if strings.Contains(k, ".") {
			pathfeilds := strings.Split(k, ".")
			colName := pathfeilds[0]
			t, ok := ddl[colName]
			if t == "M" && ok {
				var err error
				newMap[colName], err = updateMapColumnObject(spannerRow, colName, k, v)
				if err != nil {
					return errors.New("Error updating the Map:", err)
				}
				delete(newMap, k)
			}
		} else {
			t, ok := ddl[k]
			if t == "B" && ok {
				ba, err := json.Marshal(v)
				if err != nil {
					return errors.New("ValidationException", err)
				}
				newMap[k] = ba
			}
			if t == "M" && ok {
				if v == nil {
					continue
				}
				ba, err := json.MarshalIndent(v, "", "  ")
				if err != nil {
					return errors.New("ValidationException", err)
				}
				newMap[k] = string(ba)
			}
		}
	}
	mutation := spanner.InsertOrUpdateMap(table, newMap)

	mutations := []*spanner.Mutation{mutation}

	err := t.BufferWrite(mutations)
	if e := errors.AssignError(err); e != nil {
		return e
	}
	return nil
}

// updateMapColumnObject updates the fields in a given JSON object for the Map Datatype
func updateMapColumnObject(spannerRow map[string]interface{}, colName string, k string, v interface{}) (map[string]interface{}, error) {
	var data map[string]interface{}
	jsonData := spannerRow[colName]

	// jsonData should be assumed to be a JSON object. If it's already marshaled, just convert it to a string.
	jsonBytes, err := json.Marshal(jsonData) // Only if jsonData needs to be marshaled
	if err != nil {
		log.Fatalf("error marshalling JSON: %v", err)
	}

	// Unmarshal into a map for manipulation
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	// Updating the field
	if updated := utils.UpdateFieldByPath(data, k, v); updated {
		log.Println("Update successful")
	} else {
		log.Println("Update failed: path not found")
	}

	return data, nil
}

func evaluateConditionalExpression(ctx context.Context, t *spanner.ReadWriteTransaction, table string, m map[string]interface{}, e *models.Eval, expr *models.UpdateExpressionCondition) (bool, error) {
	colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(table)]
	if !ok {
		return false, errors.New("ResourceNotFoundException", table)
	}
	tableConf, err := config.GetTableConf(table)
	if err != nil {
		return false, err
	}

	pKey := tableConf.PartitionKey
	pValue, ok := m[pKey]
	if !ok {
		return false, errors.New("ValidationException", pKey)
	}
	var key spanner.Key
	sKey := tableConf.SortKey
	if sKey != "" {
		sValue, ok := m[sKey]
		if !ok {
			return false, errors.New("ValidationException", sKey)
		}
		key = spanner.Key{pValue, sValue}

	} else {
		key = spanner.Key{pValue}
	}
	var cols []string
	if expr != nil {
		cols = append(e.Cols, expr.Field...)
		for k := range expr.AddValues {
			cols = append(e.Cols, k)
		}
	} else {
		cols = e.Cols
	}

	linq.From(cols).IntersectByT(linq.From(models.TableColumnMap[utils.ChangeTableNameForSpanner(table)]), func(str string) string {
		return str
	}).ToSlice(&cols)
	r, err := t.ReadRow(ctx, utils.ChangeTableNameForSpanner(table), key, cols)
	if e := errors.AssignError(err); e != nil {
		return false, e
	}
	rowMap, _, err := parseRow(r, colDDL)
	if err != nil {
		return false, err
	}
	if expr != nil {
		for index := 0; index < len(expr.Field); index++ {
			colName := expr.Field[index]
			if strings.HasPrefix(colName, "size(") {
				// Extract attribute name from size function
				sizeRegex := regexp.MustCompile(`size\((\w+)\)`)
				matches := sizeRegex.FindStringSubmatch(colName)
				if len(matches) == 2 {
					colName = matches[1] // Extracted column name
				}
			}
			status := evaluateStatementFromRowMap(expr.Condition[index], colName, rowMap)
			tmp, ok := status.(bool)
			if !ok || !tmp {
				if v1, ok := expr.AddValues[expr.Field[index]]; ok {

					tmp, ok := rowMap[expr.Field[index]].(float64)
					if ok {
						m[expr.Field[index]] = tmp + v1
						err = checkInifinty(m[expr.Field[index]].(float64), expr)
						if err != nil {
							return false, err
						}
					}
				} else {
					delete(m, expr.Field[index])
				}
			} else {
				if v1, ok := expr.AddValues[expr.Field[index]]; ok {
					tmp, ok := m[expr.Field[index]].(float64)
					if ok {
						m[expr.Field[index]] = tmp + v1
						err = checkInifinty(m[expr.Field[index]].(float64), expr)
						if err != nil {
							return false, err
						}
					}
				}
			}
			delete(expr.AddValues, expr.Field[index])
		}
		for k, v := range expr.AddValues {
			val, ok := rowMap[k].(float64)
			if ok {
				m[k] = val + v
				err = checkInifinty(m[k].(float64), expr)
				if err != nil {
					return false, err
				}

			} else {
				m[k] = v
			}
		}
	}
	for i := 0; i < len(e.Attributes); i++ {
		e.ValueMap[e.Tokens[i]] = evaluateStatementFromRowMap(e.Attributes[i], e.Cols[i], rowMap)
	}

	status, err := utils.EvaluateExpression(e)
	if err != nil {
		return false, err
	}

	return status, nil
}

func evaluateStatementFromRowMap(conditionalExpression, colName string, rowMap map[string]interface{}) interface{} {
	if strings.HasPrefix(conditionalExpression, "attribute_not_exists") || strings.HasPrefix(conditionalExpression, "if_not_exists") {
		if len(rowMap) == 0 {
			return true
		}
		_, ok := rowMap[colName]
		return !ok
	}
	if strings.HasPrefix(conditionalExpression, "attribute_exists") || strings.HasPrefix(conditionalExpression, "if_exists") {
		if len(rowMap) == 0 {
			return false
		}
		_, ok := rowMap[colName]
		return ok
	}
	// Handle size() function
	if strings.HasPrefix(conditionalExpression, "size(") {
		sizeRegex := regexp.MustCompile(`size\((\w+)\)`)
		matches := sizeRegex.FindStringSubmatch(conditionalExpression)
		if len(matches) == 2 {
			attributeName := matches[1]

			// Check if the attribute exists in rowMap
			val, ok := rowMap[attributeName]
			if !ok {
				return errors.New("Attribute not found in row")
			}

			// Ensure the attribute is a list and calculate its size
			switch v := val.(type) {
			case []interface{}:
				return len(v) // Return the size of the list
			default:
				return errors.New("size() function is only valid for list attributes")
			}
		} else {
			return errors.New("Invalid size() function syntax")
		}
	}
	return rowMap[conditionalExpression]
}

// parseRow parses a single Spanner row into a map of column name to value.
// It uses a column DDL map to determine the data type of each column and
// parse it accordingly.
//
// Args:
//
//	r: The Spanner row to parse.
//	colDDL: A map of column name to data type (e.g., "S", "B", "N", "BOOL", "SS", "BS", "NS").
//
// Returns:
//
//	A map of column name to value (map[string]interface{}), or an error if any occurs during parsing.
//	Returns an empty map and nil error if the input row `r` is nil.
func parseRow(r *spanner.Row, colDDL map[string]string) (map[string]interface{}, map[string]interface{}, error) {
	singleRow := make(map[string]interface{})
	if r == nil {
		return singleRow, nil, nil
	}
	spannerRow := make(map[string]interface{})

	cols := r.ColumnNames()
	for i, k := range cols {
		if k == "" || k == "commit_timestamp" {
			continue
		}
		v, ok := colDDL[k]
		if !ok {
			return nil, nil, errors.New("ResourceNotFoundException", k)
		}

		var err error
		switch v {
		case "S":
			err = parseStringColumn(r, i, k, singleRow)
		case "B":
			err = parseBytesColumn(r, i, k, singleRow)
		case "N":
			err = parseNumericColumn(r, i, k, singleRow)
		case "BOOL":
			err = parseBoolColumn(r, i, k, singleRow)
		case "SS":
			err = parseStringArrayColumn(r, i, k, singleRow)
		case "BS":
			err = parseByteArrayColumn(r, i, k, singleRow)
		case "NS":
			err = parseNumberArrayColumn(r, i, k, singleRow)
		case "NULL":
			err = parseNullColumn(r, i, k, singleRow)
		case "L":
			err = parseListColumn(r, i, k, singleRow)
		case "M":
			err = parseMapColumn(r, i, k, singleRow, spannerRow)
		default:
			return nil, nil, errors.New("TypeNotFound", err, k)
		}
		if err != nil {
			return nil, nil, errors.New("ValidationException", err, k)
		}
	}
	return singleRow, spannerRow, nil
}

// parseStringColumn parses a string column from a Spanner row.
//
// Args:
//
//	r: The Spanner row.
//	idx: The column index.
//	col: The column name.
//	row: The map to store the parsed value.
//
// Returns:
//
//	An error if any occurs during column retrieval.
func parseStringColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var s spanner.NullString
	err := r.Column(idx, &s)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}
	if s.IsNull() {
		row[col] = nil
		return nil
	} else {
		row[col] = s.StringVal
		if strings.HasSuffix(s.StringVal, "=") && utils.IsValidBase64(s.StringVal) {
			res, err := utils.ParseBytes(r, idx, col)
			if err != nil {
				return err
			}
			row[col] = res[col]
		} else {
			row[col] = s.StringVal
		}
	}
	return nil
}

// parseBytesColumn parses a bytes column from a Spanner row.  It attempts to
// unmarshal the bytes as JSON. If unmarshalling fails, it stores the raw
// byte string as a string.
//
// Args:
//
//	r: The Spanner row.
//	idx: The column index.
//	col: The column name.
//	row: The map to store the parsed value.
//
// Returns:
//
//	An error if any occurs during column retrieval.
func parseBytesColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var s []byte
	err := r.Column(idx, &s)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}

	if len(s) > 0 {
		var m interface{}
		if err := json.Unmarshal(s, &m); err != nil {
			// Instead of an error while unmarshalling fall back to the raw string.
			row[col] = string(s)
			return nil
		}
		m = processDecodedData(m)
		row[col] = m
	}
	return nil
}

// parseNumericColumn parses a numeric (float64) column from a Spanner row.
//
// Args:
//   r: The Spanner row.
//   idx: The column index.
//   col: The column name.
//   row: The map to store the parsed value.
//
// Returns:
//   An error if any occurs during column retrieval.

func parseNumericColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var s spanner.NullFloat64
	err := r.Column(idx, &s)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}
	if s.IsNull() {
		row[col] = nil
		return nil
	} else {
		row[col] = s.Float64
	}
	return nil
}

// parseBoolColumn parses a boolean column from a Spanner row.
//
// Args:
//
//	r: The Spanner row.
//	idx: The column index.
//	col: The column name.
//	row: The map to store the parsed value.
//
// Returns:
//
//	An error if any occurs during column retrieval.
func parseBoolColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var s spanner.NullBool
	err := r.Column(idx, &s)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}
	if s.IsNull() {
		row[col] = nil
	} else {
		row[col] = s.Bool
	}
	return nil
}

// parseStringArrayColumn parses a string array column from a Spanner row.
//
// Args:
//
//	r: The Spanner row.
//	idx: The column index.
//	col: The column name.
//	row: The map to store the parsed value.
//
// Returns:
//
//	An error if any occurs during column retrieval.
func parseStringArrayColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var s []spanner.NullString
	err := r.Column(idx, &s)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}
	var temp []string
	for _, val := range s {
		temp = append(temp, val.StringVal)
	}
	if len(s) > 0 {
		row[col] = temp
	}
	return nil
}

// parseByteArrayColumn parses a byte array column from a Spanner row.
//
// Args:
//
//	r: The Spanner row.
//	idx: The column index.
//	col: The column name.
//	row: The map to store the parsed value.
//
// Returns:
//
//	An error if any occurs during column retrieval.
func parseByteArrayColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var b [][]byte
	err := r.Column(idx, &b)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}
	if len(b) > 0 {
		row[col] = b
	}
	return nil
}

// parseNumberArrayColumn parses a numeric (float64) array column from a Spanner row.
//
// Args:
//
//	r: The Spanner row.
//	idx: The column index.
//	col: The column name.
//	row: The map to store the parsed value.
//
// Returns:
//
//	An error if any occurs during column retrieval.
func parseNumberArrayColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var nums []spanner.NullFloat64
	err := r.Column(idx, &nums)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}
	var temp []float64
	for _, val := range nums {
		if val.Valid {
			temp = append(temp, val.Float64)
		}
	}
	if len(nums) > 0 {
		row[col] = temp
	}
	return nil
}

// parseMapColumn parses a column of type for JSON data
func parseMapColumn(r *spanner.Row, idx int, col string, row map[string]interface{}, spannerRow map[string]interface{}) error {
	var s spanner.NullJSON
	err := r.Column(idx, &s)
	if err != nil {
		return errors.New("ValidationException", err, col)
	}

	if !s.IsNull() {
		var decodedData interface{}
		if err = json.Unmarshal([]byte(s.String()), &decodedData); err != nil {
			return errors.New("JSONParseException", err)
		}
		row[col] = utils.ParseNestedJSON(decodedData)
		spannerRow[col] = decodedData
	}
	return err
}

// parseListColumn parses a list column from a Spanner row.
//
// Args:
//
//	r: The Spanner row.
//	idx: The column index.
//	col: The column name.
//	row: The map to store the parsed value.
//
// Returns:
//
//	An error if any occurs during column retrieval.
func parseListColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var jsonValue spanner.NullJSON
	err := r.Column(idx, &jsonValue)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}
	if !jsonValue.IsNull() {
		parsed := parseDynamoDBJSON(jsonValue.Value)
		row[col] = parsed
	}
	return nil
}

// parseDynamoDBJSON parses a DynamoDB JSON structure into a Go native type.
//
// Args:
//
//	value: The value to parse, which can be a map, list, or primitive type.
//
// Returns:
//
//	The parsed Go native type (interface{}).
func parseDynamoDBJSON(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case map[string]interface{}:
		for key, val := range v {
			switch key {
			case "S": // String
				return val.(string)
			case "N": // Number
				num, _ := strconv.ParseFloat(val.(string), 64)
				return num
			case "BOOL": // Boolean
				return val.(bool)
			case "NULL": // Null
				if val.(bool) {
					return nil
				}
				return value
			case "M": // Map (nested object)
				result := make(map[string]interface{})
				for k, nestedVal := range val.(map[string]interface{}) {
					result[k] = parseDynamoDBJSON(nestedVal)
				}
				return result
			case "L": // List
				list := val.([]interface{})
				result := make([]interface{}, len(list))
				for i, item := range list {
					result[i] = parseDynamoDBJSON(item) // Recursively parse each list item
				}
				return result
			}
		}
	case []interface{}: // Handle direct list structures
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = parseDynamoDBJSON(item)
		}
		return result
	}

	return value // Return as-is for unsupported types
}

// processDecodedData attempts to decode base64 encoded strings within the
// given data structure.  It handles both strings and maps of strings.  This
// function is used to handle cases where JSON data is stored as a base64
// encoded string within a Spanner column.
//
// Args:
//
//	m: The data structure to process (interface{}).  Can be a string or a
//	   map[string]interface{}.
//
// Returns:
//
//	The processed data structure (interface{}).  If base64 decoding and JSON
//	unmarshalling are successful, the decoded JSON will be returned. Otherwise,
//	the original data structure is returned.
func processDecodedData(m interface{}) interface{} {
	if val, ok := m.(string); ok && base64Regexp.MatchString(val) {
		if ba, err := base64.StdEncoding.DecodeString(val); err == nil {
			var sample interface{}
			if err := json.Unmarshal(ba, &sample); err == nil {
				return sample
			}
		}
	}
	if mp, ok := m.(map[string]interface{}); ok {
		for k, v := range mp {
			if val, ok := v.(string); ok && base64Regexp.MatchString(val) {
				if ba, err := base64.StdEncoding.DecodeString(val); err == nil {
					var sample interface{}
					if err := json.Unmarshal(ba, &sample); err == nil {
						mp[k] = sample
					}
				}
			}
		}
	}
	return m
}

// parseNullColumn handles NULL values for any column type.
//
// Args:
//
//	r: The Spanner row.
//	idx: The column index.
//	col: The column name.
//	row: The map to store the parsed value.
//
// Returns:
//
//	An error if any occurs during column retrieval.
func parseNullColumn(r *spanner.Row, idx int, col string, row map[string]interface{}) error {
	var s spanner.NullString
	err := r.Column(idx, &s)
	if err != nil && !strings.Contains(err.Error(), "ambiguous column name") {
		return err
	}
	if s.IsNull() {
		row[col] = nil
		return nil
	}
	return nil
}

func checkInifinty(value float64, logData interface{}) error {
	if math.IsInf(value, 1) {
		return errors.New("ValidationException", "value found is infinity", logData)
	}
	if math.IsInf(value, -1) {
		return errors.New("ValidationException", "value found is infinity", logData)
	}

	return nil
}

// SpannerTransactGetItems is a utility function to fetch data for a single TransactGetItems operation.
// It takes a context, a table name, a map of projection columns, a map of primary keys, and a map of secondary keys.
// It returns a slice of maps and an error.
// The function first gets a Spanner client and then performs a transaction read operation.
// It then iterates over the results and parses the Spanner rows into DynamoDB-style rows.
// Finally, it returns the parsed rows.
func (s Storage) SpannerTransactGetItems(ctx context.Context, tableProjectionCols map[string][]string, pValues map[string]interface{}, sValues map[string]interface{}) ([]map[string]interface{}, error) {
	client := s.getSpannerClient("") // Get a generic client
	txn := client.ReadOnlyTransaction()
	defer txn.Close()

	// Initialize the result slice
	allRows := []map[string]interface{}{}
	// Iterate over the tables
	for tableName, projectionCols := range tableProjectionCols {
		// Get the column definitions for the table
		colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(tableName)]
		if !ok {
			return nil, errors.New("ResourceNotFoundException", tableName)
		}
		// Get the primary keys, secondary keys, and construct the key set
		pKeys := pValues[tableName].([]interface{})
		sKeys := sValues[tableName].([]interface{})
		var keySet []spanner.KeySet

		for i := range pKeys {
			if len(sKeys) == 0 || sKeys[i] == nil {
				keySet = append(keySet, spanner.Key{pKeys[i]})
			} else {
				keySet = append(keySet, spanner.Key{pKeys[i], sKeys[i]})
			}
		}
		// If no projection columns are specified, then get all columns
		if len(projectionCols) == 0 {
			var ok bool
			projectionCols, ok = models.TableColumnMap[utils.ChangeTableNameForSpanner(tableName)]
			if !ok {
				return nil, errors.New("ResourceNotFoundException", tableName)
			}
		}
		// Perform the transaction read operation
		itr := txn.Read(ctx, tableName, spanner.KeySets(keySet...), projectionCols)
		defer itr.Stop()
		// Iterate over the results
		for {
			r, err := itr.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				return nil, errors.New("ValidationException", err)
			}
			// Parse the Spanner row into a DynamoDB-style row
			singleRow, _, err := parseRow(r, colDDL)
			if err != nil {
				return nil, err
			}
			// If the row is not empty, add it to the result slice
			if len(singleRow) > 0 {
				rowWithTable := map[string]interface{}{
					"Item":      singleRow,
					"TableName": tableName,
				}
				allRows = append(allRows, rowWithTable)
			}
		}
	}
	return allRows, nil
}

// SpannerTransactWritePut performs a transactional write operation in Spanner.
// It checks for conditional expressions and updates the database accordingly.
//
// Args:
//
//	ctx: The context for managing request deadlines and cancellations.
//	table: The name of the table to update.
//	m: The map containing the data to be inserted or updated.
//	eval: The evaluation criteria for processing conditional expressions.
//	expr: The UpdateExpressionCondition to be checked before writing.
//	txn: The Spanner ReadWriteTransaction.
//
// Returns:
//
//	A map of updated data, a Spanner mutation, and an error if any occurs.
func (s Storage) SpannerTransactWritePut(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition, txn *spanner.ReadWriteTransaction, oldRes map[string]interface{}) (map[string]interface{}, *spanner.Mutation, error) {
	// Initialize the update map and mutation
	update := map[string]interface{}{}
	var mutation *spanner.Mutation

	// Copy input map to a temporary map for processing
	tmpMap := map[string]interface{}{}
	for k, v := range m {
		tmpMap[k] = v
	}

	// Evaluate conditional expressions if present
	if len(eval.Attributes) > 0 || expr != nil {
		status, err := evaluateConditionalExpression(ctx, txn, table, tmpMap, eval, expr)
		if err != nil {
			return m, nil, err
		}
		if !status {
			return m, nil, errors.New("ConditionalCheckFailedException", eval, expr)
		}
	}

	// Update table name to match Spanner's naming convention
	table = utils.ChangeTableNameForSpanner(table)

	// Update the map with processed data
	for k, v := range tmpMap {
		update[k] = v
	}

	// Perform the transactional put operation
	mutation, err := s.performTransactPutOperation(table, tmpMap, oldRes)
	return update, mutation, err
}

// performTransactPutOperation performs a transactional put operation in Spanner.
//
// ctx: The context of the transaction.
//
// txn: The Spanner ReadWriteTransaction.
//
// table: The name of the Spanner table.
//
// m: The input map of data to be inserted or updated.
//
// Returns:
//
//	A Spanner mutation and an error if any occurs.
func (s Storage) performTransactPutOperation(table string, m map[string]interface{}, oldRes map[string]interface{}) (*spanner.Mutation, error) {
	ddl := models.TableDDL[table]
	newMap := m
	for k, v := range m {
		if strings.Contains(k, ".") {
			pathfeilds := strings.Split(k, ".")
			colName := pathfeilds[0]
			t, ok := ddl[colName]
			if t == "M" && ok {
				var err error
				newMap[colName], err = updateMapColumnObject(oldRes, colName, k, v)
				if err != nil {
					return nil, errors.New("Error updating the Map:", err)
				}
				delete(newMap, k)
			}
		} else {
			t, ok := ddl[k]
			if t == "B" && ok {
				ba, err := json.Marshal(v)
				if err != nil {
					return nil, errors.New("ValidationException", err)
				}
				newMap[k] = ba
			}
			if t == "M" && ok {
				if v == nil {
					continue
				}
				ba, err := json.MarshalIndent(v, "", "  ")
				if err != nil {
					return nil, errors.New("ValidationException", err)
				}
				newMap[k] = string(ba)
			}
			if t == "L" && ok {
				list, ok := v.([]interface{})
				if !ok {
					return nil, errors.New("invalid list format")
				}
				jsonData, err := json.Marshal(list)
				if err != nil {
					return nil, fmt.Errorf("error marshaling list to JSON: %w", err)
				}
				newMap[k] = string(jsonData)
			}
		}
	}
	// Create a Spanner mutation for the InsertOrUpdateMap operation
	mutation := spanner.InsertOrUpdateMap(table, newMap)
	return mutation, nil
}

func (s Storage) TransactWriteSpannerDel(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition, txn *spanner.ReadWriteTransaction) (*spanner.Mutation, error) {
	tableConf, err := config.GetTableConf(table)
	if err != nil {
		return nil, err
	}
	colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(table)]
	if !ok {
		return nil, errors.New("ResourceNotFoundException", table)
	}
	pKey := tableConf.PartitionKey
	var pValue interface{}
	var sValue interface{}
	sKey := tableConf.SortKey

	cols := []string{}
	var key spanner.Key
	var m1 = make(map[string]interface{})

	for k, v := range m {
		m1[k] = v
		if k == pKey {
			pValue = v
			delete(m, k)
			continue
		}
		if k == sKey {
			delete(m, k)
			sValue = v
			continue
		}
		cols = append(cols, k)
	}
	if sValue != nil {
		key = spanner.Key{pValue, sValue}
	} else {
		key = spanner.Key{pValue}
	}
	tmpMap := map[string]interface{}{}
	for k, v := range m {
		tmpMap[k] = v
	}
	if len(eval.Attributes) > 0 || expr != nil {
		status, _ := evaluateConditionalExpression(ctx, txn, table, m1, eval, expr)
		if !status {
			return nil, errors.New("ConditionalCheckFailedException")
		}
	}
	table = utils.ChangeTableNameForSpanner(table)

	r, err := txn.ReadRow(ctx, table, key, cols)
	if err != nil {
		return nil, errors.New("ResourceNotFoundException", err)
	}
	rs, _, err := parseRow(r, colDDL)
	if err != nil {
		return nil, err
	}
	for k, v := range tmpMap {
		v1, ok := rs[k]
		if ok {
			switch v1.(type) {
			case []interface{}:
				var ifaces1 []interface{}
				ba, ok := v.([]byte)
				if ok {
					err = json.Unmarshal(ba, &ifaces1)
					if err != nil {
						logger.LogError(err, string(ba))
					}
				} else {
					ifaces1 = v.([]interface{})
				}
				m1 := map[interface{}]struct{}{}
				ifaces := v1.([]interface{})
				for i := 0; i < len(ifaces); i++ {
					m1[reflect.ValueOf(ifaces[i]).Interface()] = struct{}{}
				}
				for i := 0; i < len(ifaces1); i++ {

					delete(m1, reflect.ValueOf(ifaces1[i]).Interface())
				}
				ifaces = []interface{}{}
				for k := range m1 {
					ifaces = append(ifaces, k)
				}
				tmpMap[k] = ifaces
			default:
				logger.LogDebug(reflect.TypeOf(v).String())
			}
		}
	}
	tmpMap[pKey] = pValue
	if sValue != nil {
		tmpMap[sKey] = sValue
	}
	ddl := models.TableDDL[table]

	for k, v := range tmpMap {
		t, ok := ddl[k]
		if t == "BYTES(MAX)" && ok {
			ba, err := json.Marshal(v)
			if err != nil {
				return nil, errors.New("ValidationException", err)
			}
			tmpMap[k] = ba
		}
	}
	mutation := spanner.InsertOrUpdateMap(table, tmpMap)

	return mutation, err
}

func (s Storage) TransactWriteSpannerAdd(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition, txn *spanner.ReadWriteTransaction) (map[string]interface{}, *spanner.Mutation, error) {
	tableConf, err := config.GetTableConf(table)
	if err != nil {
		return nil, nil, err
	}
	colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(table)]
	if !ok {
		return nil, nil, errors.New("ResourceNotFoundException", table)
	}
	pKey := tableConf.PartitionKey
	var pValue interface{}
	var sValue interface{}
	sKey := tableConf.SortKey

	cols := []string{}
	var key spanner.Key
	var m1 = make(map[string]interface{})

	for k, v := range m {
		m1[k] = v
		if k == pKey {
			pValue = v
			delete(m, k)
			continue
		}
		if k == sKey {
			delete(m, k)
			sValue = v
			continue
		}
		cols = append(cols, k)
	}
	if sValue != nil {
		key = spanner.Key{pValue, sValue}
	} else {
		key = spanner.Key{pValue}
	}
	updatedObj := map[string]interface{}{}

	tmpMap := map[string]interface{}{}
	for k, v := range m1 {
		tmpMap[k] = v
	}
	if len(eval.Attributes) > 0 || expr != nil {
		status, _ := evaluateConditionalExpression(ctx, txn, table, tmpMap, eval, expr)
		if !status {
			return nil, nil, errors.New("ConditionalCheckFailedException")
		}
	}
	table = utils.ChangeTableNameForSpanner(table)

	r, err := txn.ReadRow(ctx, table, key, cols)
	if err != nil {
		return nil, nil, errors.New("ResourceNotFoundException", err)
	}
	rs, _, err := parseRow(r, colDDL)
	if err != nil {
		return nil, nil, err
	}
	for k, v := range tmpMap {
		v1, ok := rs[k]
		if ok {
			switch v1.(type) {
			case int64:
				v2, ok := v.(float64)
				if !ok {
					strV, ok := v.(string)
					if !ok {
						return nil, nil, errors.New("ValidationException", reflect.TypeOf(v).String())
					}
					v2, err = strconv.ParseFloat(strV, 64)
					if err != nil {
						return nil, nil, errors.New("ValidationException", reflect.TypeOf(v).String())
					}
					err = checkInifinty(v2, strV)
					if err != nil {
						return nil, nil, err
					}
				}
				tmpMap[k] = v1.(int64) + int64(v2)
				err = checkInifinty(float64(m[k].(int64)), m)
				if err != nil {
					return nil, nil, err
				}
			case float64:
				v2, ok := v.(float64)
				if !ok {
					strV, ok := v.(string)
					if !ok {
						return nil, nil, errors.New("ValidationException", reflect.TypeOf(v).String())
					}
					v2, err = strconv.ParseFloat(strV, 64)
					if err != nil {
						return nil, nil, errors.New("ValidationException", reflect.TypeOf(v).String())
					}
					err = checkInifinty(v2, strV)
					if err != nil {
						return nil, nil, err
					}
				}
				tmpMap[k] = v1.(float64) + v2
				err = checkInifinty(m[k].(float64), m)
				if err != nil {
					return nil, nil, err
				}

			case []interface{}:
				var ifaces1 []interface{}
				ba, ok := v.([]byte)
				if ok {
					err = json.Unmarshal(ba, &ifaces1)
					if err != nil {
						logger.LogError(err, string(ba))
					}
				} else {
					ifaces1 = v.([]interface{})
				}
				m1 := map[interface{}]struct{}{}
				ifaces := v1.([]interface{})
				for i := 0; i < len(ifaces); i++ {
					m1[ifaces[i]] = struct{}{}
				}
				for i := 0; i < len(ifaces1); i++ {
					m1[ifaces1[i]] = struct{}{}
				}
				ifaces = []interface{}{}
				for k := range m1 {
					ifaces = append(ifaces, k)
				}
				tmpMap[k] = ifaces
			default:
				logger.LogDebug(reflect.TypeOf(v).String())
			}
		}
	}
	tmpMap[pKey] = pValue
	if sValue != nil {
		tmpMap[sKey] = sValue
	}
	ddl := models.TableDDL[table]

	for k, v := range tmpMap {
		updatedObj[k] = v
		t, ok := ddl[k]
		if t == "BYTES(MAX)" && ok {
			ba, err := json.Marshal(v)
			if err != nil {
				return nil, nil, errors.New("ValidationException", err)
			}
			tmpMap[k] = ba
		}
	}

	mutation := spanner.InsertOrUpdateMap(table, tmpMap)

	return updatedObj, mutation, err
}

// TransactWriteSpannerRemove - Spanner Remove functionality like update attribute inside a transaction
//
// This is used in the context of a transaction, and it will remove the given columns from the given
// table. The condition expression in the eval and expr parameters will be evaluated and if it fails,
// this will return an error.
//
// The colsToRemove parameter should contain the names of the columns to be removed.
func (s Storage) TransactWriteSpannerRemove(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition, colsToRemove []string, txn *spanner.ReadWriteTransaction) (*spanner.Mutation, error) {

	tmpMap := map[string]interface{}{}
	for k, v := range m {
		tmpMap[k] = v
	}
	if len(eval.Attributes) > 0 || expr != nil {
		status, _ := evaluateConditionalExpression(ctx, txn, table, m, eval, expr)
		if !status {
			return nil, errors.New("ConditionalCheckFailedException")
		}
	}
	var null spanner.NullableValue
	for _, col := range colsToRemove {
		tmpMap[col] = null
	}
	table = utils.ChangeTableNameForSpanner(table)
	mutation := spanner.InsertOrUpdateMap(table, tmpMap)

	return mutation, nil
}

func (s Storage) TransactWriteSpannerDelete(ctx context.Context, table string, m map[string]interface{}, eval *models.Eval, expr *models.UpdateExpressionCondition, txn *spanner.ReadWriteTransaction) (*spanner.Mutation, error) {

	tmpMap := map[string]interface{}{}
	for k, v := range m {
		tmpMap[k] = v
	}
	if len(eval.Attributes) > 0 || expr != nil {
		status, err := evaluateConditionalExpression(ctx, txn, table, tmpMap, eval, expr)
		if err != nil {
			return nil, err
		}
		if !status {
			return nil, errors.New("ConditionalCheckFailedException", tmpMap, expr)
		}
	}
	tableConf, err := config.GetTableConf(table)
	if err != nil {
		return nil, err
	}
	table = utils.ChangeTableNameForSpanner(table)

	pKey := tableConf.PartitionKey
	pValue, ok := tmpMap[pKey]
	if !ok {
		return nil, errors.New("ResourceNotFoundException", pKey)
	}
	var key spanner.Key
	sKey := tableConf.SortKey
	if sKey != "" {
		sValue, ok := tmpMap[sKey]
		if !ok {
			return nil, errors.New("ResourceNotFoundException", pKey)
		}
		key = spanner.Key{pValue, sValue}

	} else {
		key = spanner.Key{pValue}
	}

	mutation := spanner.Delete(table, key)

	return mutation, nil
}

// EvaluateConditionalExpression evaluates a conditional expression for a given Spanner transaction.
// It checks for the presence of necessary table schema and configuration, handles conditional fields,
// and updates the map with computed values if conditions are met. It returns a boolean status indicating
// whether the condition was satisfied and an error if any occurs during processing.

func EvaluateConditionalExpression(ctx context.Context, t *spanner.ReadWriteTransaction, table string, m map[string]interface{}, e *models.Eval, expr *models.UpdateExpressionCondition) (bool, error) {
	// Retrieve table schema DDL
	colDDL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(table)]
	if !ok {
		return false, errors.New("ResourceNotFoundException", table)
	}

	// Get table configuration
	tableConf, err := config.GetTableConf(table)
	if err != nil {
		return false, err
	}

	// Validate primary key presence
	pKey := tableConf.PartitionKey
	pValue, ok := m[pKey]
	if !ok {
		return false, errors.New("ValidationException", pKey)
	}

	// Construct Spanner key based on primary and sort keys
	var key spanner.Key
	sKey := tableConf.SortKey
	if sKey != "" {
		sValue, ok := m[sKey]
		if !ok {
			return false, errors.New("ValidationException", sKey)
		}
		key = spanner.Key{pValue, sValue}
	} else {
		key = spanner.Key{pValue}
	}

	// Determine columns to read
	var cols []string
	if expr != nil {
		cols = append(e.Cols, expr.Field...)
		for k := range expr.AddValues {
			cols = append(e.Cols, k)
		}
	} else {
		cols = e.Cols
	}

	// Filter columns based on table schema
	linq.From(cols).IntersectByT(linq.From(models.TableColumnMap[utils.ChangeTableNameForSpanner(table)]), func(str string) string {
		return str
	}).ToSlice(&cols)

	// Read row from Spanner
	r, err := t.ReadRow(ctx, utils.ChangeTableNameForSpanner(table), key, cols)
	if e := errors.AssignError(err); e != nil {
		return false, e
	}

	// Parse row into a map
	rowMap, _, err := parseRow(r, colDDL)
	if err != nil {
		return false, err
	}

	// Evaluate conditions
	if expr != nil {
		for index := 0; index < len(expr.Field); index++ {
			colName := expr.Field[index]
			if strings.HasPrefix(colName, "size(") {
				// Extract attribute name from size function
				sizeRegex := regexp.MustCompile(`size\((\w+)\)`)
				matches := sizeRegex.FindStringSubmatch(colName)
				if len(matches) == 2 {
					colName = matches[1] // Extracted column name
				}
			}
			status := evaluateStatementFromRowMap(expr.Condition[index], colName, rowMap)
			tmp, ok := status.(bool)
			if !ok || !tmp {
				if v1, ok := expr.AddValues[expr.Field[index]]; ok {
					tmp, ok := rowMap[expr.Field[index]].(float64)
					if ok {
						m[expr.Field[index]] = tmp + v1
						err = checkInifinty(m[expr.Field[index]].(float64), expr)
						if err != nil {
							return false, err
						}
					}
				} else {
					delete(m, expr.Field[index])
				}
			} else {
				if v1, ok := expr.AddValues[expr.Field[index]]; ok {
					tmp, ok := m[expr.Field[index]].(float64)
					if ok {
						m[expr.Field[index]] = tmp + v1
						err = checkInifinty(m[expr.Field[index]].(float64), expr)
						if err != nil {
							return false, err
						}
					}
				}
			}
			delete(expr.AddValues, expr.Field[index])
		}

		// Apply additional values
		for k, v := range expr.AddValues {
			val, ok := rowMap[k].(float64)
			if ok {
				m[k] = val + v
				err = checkInifinty(m[k].(float64), expr)
				if err != nil {
					return false, err
				}
			} else {
				m[k] = v
			}
		}
	}

	// Evaluate main attributes
	for i := 0; i < len(e.Attributes); i++ {
		e.ValueMap[e.Tokens[i]] = evaluateStatementFromRowMap(e.Attributes[i], e.Cols[i], rowMap)
	}

	// Execute the expression evaluation
	status, err := utils.EvaluateExpression(e)
	if err != nil {
		return false, err
	}

	return status, nil
}

// InsertUpdateOrDeleteStatement performs insert, update, or delete operations on a Spanner database table
// based on the provided query map.
//
// Parameters:
// - ctx: The context for managing request-scoped values, cancelations, and timeouts.
// - query: A pointer to DeleteUpdateQueryMap, which holds the table name and query details for execution.
//
// Returns:
// - map[string]interface{}: A map that could potentially hold results for further processing (currently returns nil).
// - error: An error object, if any error occurs during the transaction execution.
func (s *Storage) InsertUpdateOrDeleteStatement(ctx context.Context, query *translator.DeleteUpdateQueryMap) (map[string]interface{}, error) {
	_, err := s.getSpannerClient(query.Table).ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, *buildStmt(query))
		if err != nil {
			return err
		}
		return nil
	}, spanner.TransactionOptions{CommitOptions: s.BuildCommitOptions()})

	return nil, err
}

// buildStmt returns a Statement with the given SQL and Params.
func buildStmt(query *translator.DeleteUpdateQueryMap) *spanner.Statement {
	return &spanner.Statement{
		SQL:    query.SpannerQuery,
		Params: query.Params,
	}
}

var defaultCommitDelay = time.Duration(0) * time.Millisecond

// BuildCommitOptions returns the commit options for Spanner transactions.
func (s Storage) BuildCommitOptions() spanner.CommitOptions {
	commitDelay := defaultCommitDelay
	return spanner.CommitOptions{
		MaxCommitDelay: &commitDelay,
	}
}
