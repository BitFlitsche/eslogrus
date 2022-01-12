package eslogrus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/sirupsen/logrus"
)

var (
	ErrCannotCreateIndex = fmt.Errorf("cannot create index")
)

type IndexNameFunc func() string

type fireFunc func(entry *logrus.Entry, hook *ElasticHook) error

type ElasticHook struct {
	client    *elasticsearch.Client
	host      string
	index     IndexNameFunc
	levels    []logrus.Level
	ctx       context.Context
	ctxCancel context.CancelFunc
	fireFunc  fireFunc
}

type message struct {
	Host      string        `json:"host"`
	Timestamp string        `json:"@timestamp"`
	Message   string        `json:"message"`
	Data      logrus.Fields `json:"data"`
	Level     string        `json:"level"`
}

// NewElasticHook creates new hook.
// client - ElasticSearch client
// host - host of system
// levels - log levels to log
// index - name of the index in ElasticSearch
func NewElasticHook(client *elasticsearch.Client, host string, levels []logrus.Level, index string) (*ElasticHook, error) {
	return newHookFuncAndFireFunc(client, host, levels, func() string { return index }, syncFireFunc)
}

func newHookFuncAndFireFunc(client *elasticsearch.Client, host string, levels []logrus.Level, indexFunc IndexNameFunc, fireFunc fireFunc) (*ElasticHook, error) {
	ctx, cancel := context.WithCancel(context.TODO())

	indexExistsResp, err := client.Indices.Exists([]string{indexFunc()})
	if err != nil {
		cancel()
		return nil, err
	}
	if indexExistsResp.StatusCode == http.StatusNotFound {
		createIndexResp, err := client.Indices.Create(indexFunc())
		if err != nil || createIndexResp.IsError() {
			cancel()
			return nil, ErrCannotCreateIndex
		}
	}

	return &ElasticHook{
		client:    client,
		host:      host,
		index:     indexFunc,
		levels:    levels,
		ctx:       ctx,
		ctxCancel: cancel,
		fireFunc:  fireFunc,
	}, nil
}

func (hook *ElasticHook) Fire(entry *logrus.Entry) error {
	return hook.fireFunc(entry, hook)
}

func createMessage(entry *logrus.Entry, hook *ElasticHook) *message {
	level := entry.Level.String()

	if e, ok := entry.Data[logrus.ErrorKey]; ok && e != nil {
		if err, ok := e.(error); ok {
			entry.Data[logrus.ErrorKey] = err.Error()
		}
	}

	return &message{
		Host:      hook.host,
		Timestamp: entry.Time.UTC().Format(time.RFC3339Nano),
		Message:   entry.Message,
		Data:      entry.Data,
		Level:     strings.ToUpper(level),
	}
}

func syncFireFunc(entry *logrus.Entry, hook *ElasticHook) error {
	data, err := json.Marshal(createMessage(entry, hook))
	if err != nil {
		return err
	}
	req := esapi.IndexRequest{
		Index: hook.index(),
		Body:  bytes.NewReader(data),
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), hook.client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return err
}

// Levels Required for logrus hook implementation
func (hook *ElasticHook) Levels() []logrus.Level {
	return hook.levels
}

// Cancel all calls to elastic
func (hook *ElasticHook) Cancel() {
	hook.ctxCancel()
}
