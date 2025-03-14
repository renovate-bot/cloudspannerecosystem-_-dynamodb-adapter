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

package v1

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"github.com/cloudspannerecosystem/dynamodb-adapter/storage"
	"github.com/stretchr/testify/mock"
	"gopkg.in/go-playground/assert.v1"
)

func TestBetween(t *testing.T) {
	tests := []struct {
		testName, strValue, firstStr, secondStr, want string
	}{
		{"Empty Value", "", "s", "l", ""},
		{"Correct Values", "school", "sc", "ol", "ho"},
		{"Correct Values with 2 similar words", "stool", "o", "l", "o"},
		{"Empty first string", "school", "", "l", "schoo"},
		{"Empty second string", "school", "o", "", ""},
	}

	for _, tc := range tests {
		got := between(tc.strValue, tc.firstStr, tc.secondStr)
		assert.Equal(t, got, tc.want)
	}
}

func TestBefore(t *testing.T) {
	tests := []struct {
		testName, valueStr, searchStr, want string
	}{
		{"Empty Value", "", "s", ""},
		{"Correct Values", "school", "ho", "sc"},
		{"Empty string values", "school", "", ""},
	}

	for _, tc := range tests {
		got := before(tc.valueStr, tc.searchStr)
		assert.Equal(t, got, tc.want)
	}
}

func TestAfter(t *testing.T) {
	tests := []struct {
		testName, valueStr, searchStr, want string
	}{
		{"Empty Value", "", "s", ""},
		{"Correct Values", "school", "ho", "ol"},
		{"Empty string values", "school", "", ""},
	}

	for _, tc := range tests {
		got := after(tc.valueStr, tc.searchStr)
		assert.Equal(t, got, tc.want)

	}
}

func TestDeleteEmpty(t *testing.T) {
	tests := []struct {
		testName string
		inputArr []string
		want     []string
	}{
		{
			"Empty array",
			[]string{},
			nil,
		},
		{
			"Only spaces present in array",
			[]string{"", "", ""},
			nil,
		},
		{
			"Spaces present in the middle",
			[]string{"frist", "", "second", "", "third", "fourth"},
			[]string{"frist", "second", "third", "fourth"},
		},
		{
			"Spaces present in initial position",
			[]string{"", "", "", "frist", "second", "third", "fourth"},
			[]string{"frist", "second", "third", "fourth"},
		},
		{
			"Spaces present in end position",
			[]string{"frist", "second", "third", "fourth", "", "", ""},
			[]string{"frist", "second", "third", "fourth"},
		},
	}

	for _, tc := range tests {
		got := deleteEmpty(tc.inputArr)
		assert.Equal(t, got, tc.want)
	}
}

func Test_parseUpdateExpresstion(t *testing.T) {
	tests := []struct {
		testName string
		input    string
		want     *models.UpdateExpressionCondition
	}{
		{
			"empty input value",
			"",
			nil,
		},
		{
			"wrong syntax",
			"if_exists(name, :val2",
			nil,
		},
		{
			"if_exists only",
			"if_exists(name, :val2)",
			&models.UpdateExpressionCondition{
				Field:     []string{"name"},
				Value:     []string{":val2"},
				Condition: []string{"if_exists"},
				ActionVal: "%:val2%",
			},
		},
		{
			"if_not_exists only",
			"if_not_exists(name, :anyVal)",
			&models.UpdateExpressionCondition{
				Field:     []string{"name"},
				Value:     []string{":anyVal"},
				Condition: []string{"if_not_exists"},
				ActionVal: "%:anyVal%",
			},
		},
		{
			"if_exists and if_not_exists both",
			"if_exists(id, :val1) && if_not_exists(name, :val2)",
			&models.UpdateExpressionCondition{
				Field:     []string{"name", "id"},
				Value:     []string{":val2", ":val1"},
				Condition: []string{"if_not_exists", "if_exists"},
				ActionVal: "%:val1% && %:val2%",
			},
		},
		{
			"if_exists, if_not_exists and other values",
			"age >:ag && if_exists(id, :val1) && if_not_exists(name, :val2)",
			&models.UpdateExpressionCondition{
				Field:     []string{"name", "id"},
				Value:     []string{":val2", ":val1"},
				Condition: []string{"if_not_exists", "if_exists"},
				ActionVal: "age >:ag && %:val1% && %:val2%",
			},
		},
	}

	for _, tc := range tests {
		got := parseUpdateExpresstion(tc.input)
		assert.Equal(t, got, tc.want)
	}
}

func Test_extractOperations(t *testing.T) {
	tests := []struct {
		testName    string
		inputString string
		want        map[string]string
	}{
		{
			"Empty input string",
			"",
			nil,
		},
		{
			"Only SET Operation",
			"SET name = :val1, age = :val2",
			map[string]string{
				"SET": "name = :val1, age = :val2",
			},
		},
		{
			"Lowercase set Operation",
			"set name = :val1, age = :val2",
			map[string]string{
				"SET": "name = :val1, age = :val2",
			},
		},
		{
			"Only ADD Operation",
			"ADD age :val1",
			map[string]string{
				"ADD": "age :val1",
			},
		},
		{
			"Title case Add Operation",
			"Add age :val1",
			map[string]string{
				"ADD": "age :val1",
			},
		},
		{
			"Only REMOVE Operation",
			"REMOVE address",
			map[string]string{
				"REMOVE": "address",
			},
		},
		{
			"Mixed case ReMoVe Operation",
			"ReMoVe address",
			map[string]string{
				"REMOVE": "address",
			},
		},
		{
			"Only DELETE Operation",
			"DELETE Color :p",
			map[string]string{
				"DELETE": "Color :p",
			},
		},
		{
			"Lower case delete Operation",
			"delete Color :p",
			map[string]string{
				"DELETE": "Color :p",
			},
		},
	}

	for _, tc := range tests {
		got := extractOperations(tc.inputString)
		assert.Equal(t, got, tc.want)
	}
}

func TestReplaceHashRangeExpr(t *testing.T) {
	tests := []struct {
		testName string
		input    models.Query
		want     models.Query
	}{
		{
			"empty input ",
			models.Query{},
			models.Query{},
		},
		{
			"empty ExpressionAttributeNames ",
			models.Query{
				ExpressionAttributeNames: nil,
				RangeExp:                 "#e = :val1",
				FilterExp:                "#ag > :val2",
			},
			models.Query{
				ExpressionAttributeNames: nil,
				RangeExp:                 "#e = :val1",
				FilterExp:                "#ag > :val2",
			},
		},
		{
			"Correct Input",
			models.Query{
				ExpressionAttributeNames: map[string]string{
					"#e":  "emp_id",
					"#ag": "age",
				},
				RangeExp:  "#e = :val1",
				FilterExp: "#ag > :val2",
			},
			models.Query{
				ExpressionAttributeNames: map[string]string{
					"#e":  "emp_id",
					"#ag": "age",
				},
				RangeExp:  "emp_id = :val1",
				FilterExp: "age > :val2",
			},
		},
	}

	for _, tc := range tests {
		got := ReplaceHashRangeExpr(tc.input)
		assert.Equal(t, got, tc.want)
	}
}

func TestConvertDynamoToMap(t *testing.T) {
	tests := []struct {
		testName       string
		dynamodbObject map[string]*dynamodb.AttributeValue
		want           map[string]interface{}
	}{
		{
			"empty dynamodbObject",
			nil,
			nil,
		},
		{
			"dynamodbObject with String present",
			map[string]*dynamodb.AttributeValue{
				"address":    {S: aws.String("Ney York")},
				"first_name": {S: aws.String("Catalina")},
				"last_name":  {S: aws.String("Smith")},
				"titles":     {SS: aws.StringSlice([]string{"Mr", "Dr"})},
			},
			map[string]interface{}{
				"address":    "Ney York",
				"first_name": "Catalina",
				"last_name":  "Smith",
				"titles":     []string{"Mr", "Dr"},
			},
		},
		{
			"dynamodbObject with diffent type of params",
			map[string]*dynamodb.AttributeValue{
				"emp_id":     {N: aws.String("2")},
				"age":        {N: aws.String("20")},
				"address":    {S: aws.String("Ney York")},
				"first_name": {S: aws.String("Catalina")},
				"last_name":  {S: aws.String("Smith")},
				"subjects": {L: []*dynamodb.AttributeValue{
					{S: aws.String("Maths")},
					{S: aws.String("Physics")},
					{S: aws.String("Chemistry")},
				}},
			},
			map[string]interface{}{
				"emp_id":     float64(2),
				"age":        float64(20),
				"address":    "Ney York",
				"first_name": "Catalina",
				"last_name":  "Smith",
				"subjects":   []interface{}{"Maths", "Physics", "Chemistry"},
			},
		},
	}

	for _, tc := range tests {
		got, _ := ConvertDynamoToMap("", tc.dynamodbObject)
		for key, value := range got {
			if gotSlice, ok := value.([]string); ok {
				sort.Strings(gotSlice)
				got[key] = gotSlice
			}
		}

		for key, value := range tc.want {
			if wantSlice, ok := value.([]string); ok {
				sort.Strings(wantSlice)
				tc.want[key] = wantSlice
			}
		}
		assert.Equal(t, got, tc.want)
	}
}

func TestChangeMaptoDynamoMap(t *testing.T) {
	tests := []struct {
		testName string
		input    interface{}
		want     map[string]interface{}
	}{
		{
			"empty input",
			nil,
			nil,
		},
		{
			"Only String Values for input",
			map[string]interface{}{
				"address": "London",
				"name":    "Richard",
			},
			map[string]interface{}{
				"address": map[string]interface{}{"S": "London"},
				"name":    map[string]interface{}{"S": "Richard"},
			},
		},
		{
			"Mixed data types for input",
			map[string]interface{}{
				"address": "London",
				"name":    "Richard",
				"age":     20,
				"value":   float64(10),
				"array":   []string{"first", "second", "third"},
			},
			map[string]interface{}{
				"address": map[string]interface{}{"S": "London"},
				"name":    map[string]interface{}{"S": "Richard"},
				"age":     map[string]interface{}{"N": "20"},
				"value":   map[string]interface{}{"N": "10"},
				"array": map[string]interface{}{
					"SS": []string{"first", "second", "third"},
				},
			},
		},
	}

	for _, tc := range tests {
		got, _ := ChangeMaptoDynamoMap(tc.input)
		assert.Equal(t, got, tc.want)
	}
}

func TestParseActionValue(t *testing.T) {
	tests := []struct {
		name           string
		updateAttr     models.UpdateAttr
		oldRes         map[string]interface{}
		expectedResult map[string]interface{}
		actionValue    string
	}{
		{
			name: "Simple key-value assignment",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "SET count = :countVal",
				ExpressionAttributeMap: map[string]interface{}{
					":countVal": 10,
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{},
			expectedResult: map[string]interface{}{
				"id":    "1",
				"count": 10,
			},
			actionValue: "count :countVal",
		},
		{
			name: "Addition operation",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "SET count = count + :incr",
				ExpressionAttributeMap: map[string]interface{}{
					":incr": 1,
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			expectedResult: map[string]interface{}{
				"id": "1",
			},
			actionValue: "count = count + :incr",
		},
		{
			name: "Subtraction operation",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "SET count = count - :decr",
				ExpressionAttributeMap: map[string]interface{}{
					":decr": 2,
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{},
			expectedResult: map[string]interface{}{
				"id": "1",
			},
			actionValue: "count = count - :decr",
		},
		{
			name: "String set append with ADD",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "ADD tags :newTags",
				ExpressionAttributeMap: map[string]interface{}{
					":newTags": []string{"newTag"},
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{
				"tags": []string{"oldTag"},
			},
			expectedResult: map[string]interface{}{
				"id":   "1",
				"tags": []string{"oldTag", "newTag"},
			},
			actionValue: "tags :newTags",
		},
		{
			name: "String set removal with DELETE",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "DELETE tags :removeTags",
				ExpressionAttributeMap: map[string]interface{}{
					":removeTags": []string{"oldTag"},
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{
				"tags": []string{"oldTag", "newTag"},
			},
			expectedResult: map[string]interface{}{
				"id":   "1",
				"tags": []string{"newTag"},
			},
			actionValue: "tags :removeTags",
		},
		{
			name: "Number set append with ADD",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "ADD tags :newTags",
				ExpressionAttributeMap: map[string]interface{}{
					":newTags": []float64{10},
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{
				"tags": []float64{20},
			},
			expectedResult: map[string]interface{}{
				"id":   "1",
				"tags": []float64{20, 10},
			},
			actionValue: "tags :newTags",
		},
		{
			name: "Number set removal with DELETE",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "DELETE tags :removeTags",
				ExpressionAttributeMap: map[string]interface{}{
					":removeTags": []float64{10},
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{
				"tags": []float64{20, 10},
			},
			expectedResult: map[string]interface{}{
				"id":   "1",
				"tags": []float64{20},
			},
			actionValue: "tags :removeTags",
		},
		{
			name: "Binary set append with ADD",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "ADD binaryData :newBinary",
				ExpressionAttributeMap: map[string]interface{}{
					":newBinary": [][]byte{[]byte("newData")},
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{
				"binaryData": [][]byte{[]byte("oldData")},
			},
			expectedResult: map[string]interface{}{
				"id":         "1",
				"binaryData": [][]byte{[]byte("oldData"), []byte("newData")},
			},
			actionValue: "binaryData :newBinary",
		},
		{
			name: "Binary set removal with DELETE",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "DELETE binaryData :removeBinary",
				ExpressionAttributeMap: map[string]interface{}{
					":removeBinary": [][]byte{[]byte("oldData")},
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{
				"binaryData": [][]byte{[]byte("oldData"), []byte("newData")},
			},
			expectedResult: map[string]interface{}{
				"id":         "1",
				"binaryData": [][]byte{[]byte("newData")},
			},
			actionValue: "binaryData :removeBinary",
		},
		{
			name: "List append operation",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "SET list_type = list_append(list_type, :newValue)",
				ExpressionAttributeMap: map[string]interface{}{
					":newValue": []interface{}{"John"},
				},
				ExpressionAttributeNames: map[string]string{},
				PrimaryKeyMap: map[string]interface{}{
					"rank_list": "rank_list",
				},
			},
			oldRes: map[string]interface{}{
				"list_type": []interface{}{"test"},
			},
			expectedResult: map[string]interface{}{
				"rank_list": "rank_list",
				"list_type": []interface{}{"test", "John"},
			},
			actionValue: "list_type list_append(list_type, :newValue)",
		},
		{
			name: "List item update by index",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "SET list_type[1] = :newValue",
				ExpressionAttributeMap: map[string]interface{}{
					":newValue": "Jacob",
				},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{
				"list_type": []interface{}{"John", "Doe"},
			},
			expectedResult: map[string]interface{}{
				"id":        "1",
				"list_type": []interface{}{"John", "Jacob"},
			},
			actionValue: "list_type[1] = :newValue",
		},
		{
			name: "List item update by index",
			updateAttr: models.UpdateAttr{
				UpdateExpression: "SET list_type[2] = :newValue",
				ExpressionAttributeMap: map[string]interface{}{
					":newValue": "newData",
				},
				PrimaryKeyMap: map[string]interface{}{
					"id": "1",
				},
			},
			oldRes: map[string]interface{}{
				"list_type": []interface{}{"John", "Doe"},
			},
			expectedResult: map[string]interface{}{
				"id":        "1",
				"list_type": []interface{}{"John", "Doe", "newData"},
			},
			actionValue: "list_type[2] =  :newValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := parseActionValue(tt.actionValue, tt.updateAttr, true, tt.oldRes)
			if !reflect.DeepEqual(result, tt.expectedResult) {
				t.Errorf("Test %s failed: expected %v, got %v", tt.name, tt.expectedResult, result)
			}
		})
	}
}

type MockConfig struct{}

// MayIReadOrWrite implements services.Service.
func (m *MockService) MayIReadOrWrite(tableName string, isWrite bool, user string) bool {
	args := m.Called(tableName, isWrite, user)
	return args.Bool(0)
}

func (m *MockService) GetWithProjection(ctx context.Context, tableName string, primaryKeyMap map[string]interface{}, projectionExpression string, expressionAttributeNames map[string]string) (map[string]interface{}, map[string]interface{}, error) {
	args := m.Called(ctx, tableName, primaryKeyMap, projectionExpression, expressionAttributeNames)
	return args.Get(0).(map[string]interface{}), args.Get(1).(map[string]interface{}), args.Error(2)
}
func (m *MockConfig) GetTableConf(tableName string) (*models.TableConfig, error) {
	return &models.TableConfig{ActualTable: tableName}, nil
}

func (m *MockService) TransactWritePut(ctx context.Context, tableName string, putObj map[string]interface{}, expr *models.UpdateExpressionCondition, conditionExp string, expressionAttr, oldRes map[string]interface{}, txn *spanner.ReadWriteTransaction) (map[string]interface{}, *spanner.Mutation, error) {
	args := m.Called(ctx, tableName, putObj, expr, conditionExp, expressionAttr, oldRes, txn)
	return args.Get(0).(map[string]interface{}), args.Get(1).(*spanner.Mutation), args.Error(2)
}

func (m *MockService) TransactWriteAdd(ctx context.Context, tableName string, primaryKeyMap map[string]interface{}, conditionExpression string, mAttributes map[string]interface{}, expressionAttributeMap map[string]interface{}, expr *models.UpdateExpressionCondition, oldRes map[string]interface{}, txn *spanner.ReadWriteTransaction) (map[string]interface{}, *spanner.Mutation, error) {
	args := m.Called(ctx, tableName, primaryKeyMap, conditionExpression, mAttributes, expressionAttributeMap, expr, oldRes, txn)
	return args.Get(0).(map[string]interface{}), args.Get(1).(*spanner.Mutation), args.Error(2)
}

func (m *MockService) TransactWriteRemove(ctx context.Context, tableName string, updateAttr models.UpdateAttr, actionValue string, expr *models.UpdateExpressionCondition, oldRes map[string]interface{}, txn *spanner.ReadWriteTransaction) (map[string]interface{}, *spanner.Mutation, error) {
	args := m.Called(ctx, tableName, updateAttr, actionValue, expr, oldRes, txn)
	return args.Get(0).(map[string]interface{}), args.Get(1).(*spanner.Mutation), args.Error(2)
}

func (m *MockService) TransactWriteDel(ctx context.Context, tableName string, primaryKeyMap map[string]interface{}, conditionExpression string, mAttributes map[string]interface{}, expr *models.UpdateExpressionCondition, txn *spanner.ReadWriteTransaction) (map[string]interface{}, *spanner.Mutation, error) {
	args := m.Called(ctx, tableName, primaryKeyMap, conditionExpression, mAttributes, expr, txn)
	return args.Get(0).(map[string]interface{}), args.Get(1).(*spanner.Mutation), args.Error(2)
}
func (m *MockStorage) SpannerTransactWritePut(ctx context.Context, tableName string, putObj map[string]interface{}, e *models.Eval, expr *models.UpdateExpressionCondition, txn *spanner.ReadWriteTransaction) (map[string]interface{}, *spanner.Mutation, error) {
	return map[string]interface{}{"Name": "John"}, &spanner.Mutation{}, nil
}

func TestTransactWriteUpdateExpression(t *testing.T) {
	ctx := context.Background()
	mockTxn := &spanner.ReadWriteTransaction{} // Mock transaction
	updateAttr := models.UpdateAttr{
		TableName:                "TestTable",
		PrimaryKeyMap:            map[string]interface{}{"id": 1},
		UpdateExpression:         "SET #name = :newName",
		ConditionExpression:      "#age > :minAge",
		ExpressionAttributeNames: map[string]string{"#name": "Name", "#age": "Age"},
		ExpressionAttributeMap:   map[string]interface{}{":newName": "John", ":minAge": 18},
		ReturnValues:             "ALL_NEW",
	}
	mockStorageInstance := &storage.Storage{}
	storage.SetStorageInstance(mockStorageInstance)
	mockSvc := new(MockService)

	mockSvc.On("GetWithProjection", ctx, updateAttr.TableName, updateAttr.PrimaryKeyMap, "", mock.Anything).
		Return(map[string]interface{}{"Name": "Doe", "Age": 20}, map[string]interface{}{}, nil)

	mockSvc.On("TransactWritePut", ctx, updateAttr.TableName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mockTxn).
		Return(map[string]interface{}{
			"Name": map[string]interface{}{
				"S": "John",
			},
		}, &spanner.Mutation{}, nil)

	result, mut, err := TransactWriteUpdateExpression(ctx, updateAttr, mockTxn, mockSvc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if mut == nil {
		t.Fatalf("Expected mutation, got nil")
	}

	expected := map[string]interface{}{
		"Attributes": map[string]interface{}{
			"Name": map[string]interface{}{
				"S": map[string]interface{}{
					"S": "John",
				},
			},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected result %+v, got %+v", expected, result)
	}

	mockSvc.AssertExpectations(t)
}

func TestTransactWriteUpdateAddExpression(t *testing.T) {
	ctx := context.Background()
	mockTxn := &spanner.ReadWriteTransaction{} // Mock transaction
	updateAttr := models.UpdateAttr{
		TableName:                "TestTable",
		PrimaryKeyMap:            map[string]interface{}{"id": 1},
		UpdateExpression:         "ADD #name = :newName",
		ConditionExpression:      "#age > :minAge",
		ExpressionAttributeNames: map[string]string{"#name": "Name", "#age": "Age"},
		ExpressionAttributeMap:   map[string]interface{}{":newName": "John", ":minAge": 18},
		ReturnValues:             "ALL_NEW",
	}
	mockStorageInstance := &storage.Storage{}
	storage.SetStorageInstance(mockStorageInstance)
	mockSvc := new(MockService)

	mockSvc.On("GetWithProjection", ctx, updateAttr.TableName, updateAttr.PrimaryKeyMap, "", mock.Anything).
		Return(map[string]interface{}{"Name": "Doe", "Age": 20}, map[string]interface{}{}, nil)

	mockSvc.On("TransactWriteAdd", ctx, updateAttr.TableName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mockTxn).
		Return(map[string]interface{}{"Age": 21}, &spanner.Mutation{}, nil)

	result, mut, err := TransactWriteUpdateExpression(ctx, updateAttr, mockTxn, mockSvc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if mut == nil {
		t.Fatalf("Expected mutation, got nil")
	}

	expected := map[string]interface{}{
		"Attributes": map[string]interface{}{
			"Age": map[string]interface{}{
				"N": "21",
			},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected result %+v, got %+v", expected, result)
	}

	mockSvc.AssertExpectations(t)
}

func TestTransactWriteUpdateRemoveExpression(t *testing.T) {
	ctx := context.Background()
	mockTxn := &spanner.ReadWriteTransaction{} // Mock transaction
	updateAttr := models.UpdateAttr{
		TableName:                "TestTable",
		PrimaryKeyMap:            map[string]interface{}{"id": 1},
		UpdateExpression:         "Remove #name = :newName",
		ConditionExpression:      "#age > :minAge",
		ExpressionAttributeNames: map[string]string{"#name": "Name", "#age": "Age"},
		ExpressionAttributeMap:   map[string]interface{}{":newName": "John", ":minAge": 18},
		ReturnValues:             "ALL_NEW",
	}
	mockStorageInstance := &storage.Storage{}
	storage.SetStorageInstance(mockStorageInstance)
	mockSvc := new(MockService)

	mockSvc.On("GetWithProjection", ctx, updateAttr.TableName, updateAttr.PrimaryKeyMap, "", mock.Anything).
		Return(map[string]interface{}{"Name": "Doe", "Age": 20}, map[string]interface{}{}, nil)

	mockSvc.On("TransactWriteRemove", ctx, updateAttr.TableName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mockTxn).
		Return(map[string]interface{}{"Name": nil}, &spanner.Mutation{}, nil)

	result, mut, err := TransactWriteUpdateExpression(ctx, updateAttr, mockTxn, mockSvc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if mut == nil {
		t.Fatalf("Expected mutation, got nil")
	}

	expected := map[string]interface{}{
		"Attributes": map[string]interface{}{
			"Name": map[string]interface{}{
				"NULL": true,
			},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected result %+v, got %+v", expected, result)
	}

	mockSvc.AssertExpectations(t)
}

func TestTransactWriteUpdateDeleteExpression(t *testing.T) {
	ctx := context.Background()
	mockTxn := &spanner.ReadWriteTransaction{} // Mock transaction
	updateAttr := models.UpdateAttr{
		TableName:                "TestTable",
		PrimaryKeyMap:            map[string]interface{}{"id": 1},
		UpdateExpression:         "Delete #name = :newName",
		ConditionExpression:      "#age > :minAge",
		ExpressionAttributeNames: map[string]string{"#name": "Name", "#age": "Age"},
		ExpressionAttributeMap:   map[string]interface{}{":newName": "John", ":minAge": 18},
		ReturnValues:             "ALL_OLD",
	}
	mockStorageInstance := &storage.Storage{}
	storage.SetStorageInstance(mockStorageInstance)
	mockSvc := new(MockService)

	mockSvc.On("GetWithProjection", ctx, updateAttr.TableName, updateAttr.PrimaryKeyMap, "", mock.Anything).
		Return(map[string]interface{}{"Name": "Doe", "Age": 20}, map[string]interface{}{}, nil)

	mockSvc.On("TransactWriteDel", ctx, updateAttr.TableName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mockTxn).
		Return(map[string]interface{}{"Deleted": true}, &spanner.Mutation{}, nil)

	result, mut, err := TransactWriteUpdateExpression(ctx, updateAttr, mockTxn, mockSvc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if mut == nil {
		t.Fatalf("Expected mutation, got nil")
	}

	expected := map[string]interface{}{
		"Attributes": map[string]interface{}{ // Added "Attributes" level
			"Name": map[string]interface{}{
				"S": "Doe",
			},
			"Age": map[string]interface{}{
				"N": "20",
			},
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected result %+v, got %+v", expected, result)
	}

	mockSvc.AssertExpectations(t)
}
