/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package reversewrapper is used to reverse logic from mobile wrapper.
package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/hyperledger/aries-framework-go/cmd/aries-agent-mobile/pkg/api"
	spi "github.com/hyperledger/aries-framework-go/spi/storage"
)

// Provider represents a storage provider reverse wrapper that allows for conversion between the
// spi/provider storage interfaces and the mobile-bindings-compatible interface in
// aries-framework-go/cmd/aries-agent-mobile/pkg/api/storage.go.
type Provider struct {
	commonProvider spi.Provider
	openStores     map[string]*store
	lock           sync.RWMutex
}

type closer func(name string)

// New returns a new storage provider reverse wrapper.
func New(provider spi.Provider) api.Provider {
	return &Provider{commonProvider: provider, openStores: make(map[string]*store)}
}

// OpenStore opens a store with the given name and returns a handle.
// If the store has never been opened before, then it is created.
// Store names are not case-sensitive. If name is blank, then an error will be returned.
func (p *Provider) OpenStore(name string) (api.Store, error) {
	name = strings.ToLower(name)

	p.lock.Lock()
	defer p.lock.Unlock()

	openStore := p.openStores[name]
	if openStore == nil {
		commonStore, err := p.commonProvider.OpenStore(name)
		if err != nil {
			return nil, fmt.Errorf("failed to open common store: %w", err)
		}

		storeWrapper := &store{commonStore: commonStore, name: name, close: p.removeStore}

		p.openStores[name] = storeWrapper

		return storeWrapper, nil
	}

	return openStore, nil
}

// SetStoreConfig sets the configuration on a store.
// The store must be created prior to calling this method.
// If the store cannot be found, then an error wrapping ErrStoreNotFound will be returned.
// If name is blank, then an error will be returned.
func (p *Provider) SetStoreConfig(name string, storeConfigBytes []byte) error {
	storeConfig := spi.StoreConfiguration{}
	err := json.Unmarshal(storeConfigBytes, &storeConfig)
	if err != nil {
		return fmt.Errorf("failed to unmarshal store config: %w", err)
	}

	err = p.commonProvider.SetStoreConfig(name, storeConfig)
	if err != nil {
		// The errors returned from the common provider will just have the error text without any distinct
		// error types like the ones defined in aries-framework-go/spi/storage. In order to still allow for the use of
		// errors.Is(err, spi.ErrStoreNotFound) in higher level functions,
		// we need to wrap spi.ErrStoreNotFound back in if we detect it.
		if strings.Contains(err.Error(), spi.ErrStoreNotFound.Error()) {
			return fmt.Errorf("failed to set store config in common provider: %s: %w", err.Error(),
				spi.ErrStoreNotFound)
		}

		return fmt.Errorf("failed to set store config in common provider: %w", err)
	}

	return nil
}

// GetStoreConfig gets the current store configuration.
// The store must be created prior to calling this method.
// If the store cannot be found, then an error wrapping ErrStoreNotFound will be returned.
// If name is blank, then an error will be returned.
func (p *Provider) GetStoreConfig(name string) ([]byte, error) {
	config, err := p.commonProvider.GetStoreConfig(name)

	if err != nil {
		// The errors returned from the common provider will just have the error text without any distinct
		// error types like the ones defined in aries-framework-go/spi/storage. In order to still allow for the use of
		// errors.Is(err, spi.ErrStoreNotFound) in higher level functions,
		// we need to wrap spi.ErrStoreNotFound back in if we detect it.
		if strings.Contains(err.Error(), spi.ErrStoreNotFound.Error()) {
			return []byte{},
				fmt.Errorf("failed to get store config from common provider: %s: %w", err.Error(),
					spi.ErrStoreNotFound)
		}

		return []byte{},
			fmt.Errorf("failed to get store config from common provider: %w", err)
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		return []byte{}, fmt.Errorf("failed to marshal store configuration: %w", err)
	}

	return configBytes, nil
}

// GetOpenStores returns all currently open stores.
// No está en el otro lado
func (p *Provider) GetOpenStores() []api.Store {
	p.lock.RLock()
	defer p.lock.RUnlock()

	openSPIStores := make([]api.Store, len(p.openStores))

	var counter int

	for _, openStoreWrapper := range p.openStores {
		openSPIStores[counter] = openStoreWrapper
		counter++
	}

	return openSPIStores
}

// Close closes all stores created under this store provider.
// For persistent common store implementations, this does not delete any data in the underlying databases.
func (p *Provider) Close() error {
	p.lock.RLock()

	openStoresSnapshot := make([]*store, len(p.openStores))

	var counter int

	for _, openStore := range p.openStores {
		openStoresSnapshot[counter] = openStore
		counter++
	}
	p.lock.RUnlock()

	for _, openStore := range openStoresSnapshot {
		err := openStore.Close()
		if err != nil {
			return fmt.Errorf("failed to close store: %w", err)
		}
	}

	return nil
}

func (p *Provider) removeStore(name string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.openStores, name)
}

type store struct {
	commonStore spi.Store
	name        string
	close       closer
}

func (s *store) Put(key string, value, tagsBytes []byte) error {
	var tags []spi.Tag

	var err error

	if len(tagsBytes) > 0 {
		err = json.Unmarshal(tagsBytes, &tags)
		if err != nil {
			return fmt.Errorf("failed to unmarshal tags: %w", err)
		}
	}

	err = s.commonStore.Put(key, value, tags...)
	if err != nil {
		return fmt.Errorf("failed to put value in common store: %w", err)
	}

	return nil
}

// Get fetches the record based on key.
func (s *store) Get(key string) ([]byte, error) {
	res, err := s.commonStore.Get(key)
	if err != nil {
		// The errors returned from the common provider will just have the error text without any distinct
		// error types like the ones defined in aries-framework-go/spi/storage. In order to still allow for the use of
		// errors.Is(err, spi.ErrDataNotFound) in higher level functions,
		// we need to wrap spi.ErrDataNotFound back in if we detect it.
		if strings.Contains(err.Error(), spi.ErrDataNotFound.Error()) {
			return nil, fmt.Errorf("failed to get value from common store: %s: %w",
				err.Error(), spi.ErrDataNotFound)
		}

		return nil, fmt.Errorf("failed to get value from common store: %w", err)
	}

	return res, err
}

func (s *store) GetTags(key string) ([]byte, error) {
	tags, err := s.commonStore.GetTags(key)
	if err != nil {
		// The errors returned from the common provider will just have the error text without any distinct
		// error types like the ones defined in aries-framework-go/spi/storage. In order to still allow for the use of
		// errors.Is(err, spi.ErrDataNotFound) in higher level functions,
		// we need to wrap spi.ErrDataNotFound back in if we detect it.
		if strings.Contains(err.Error(), spi.ErrDataNotFound.Error()) {
			return nil, fmt.Errorf("failed to get tags from common store: %s: %w",
				err.Error(), spi.ErrDataNotFound)
		}

		return nil, fmt.Errorf("failed to get tags from common store: %w", err)
	}

	tagsBytes, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tags: %w", err)
	}

	return tagsBytes, nil
}

func (s *store) GetBulk(keysBytes []byte) ([]byte, error) {
	
	var keys []string
	err := json.Unmarshal(keysBytes, &keys)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal keys: %w", err)
	}

	values, err := s.commonStore.GetBulk(keys...)
	if err != nil {
		return nil, fmt.Errorf("failed to get values from common store: %w", err)
	}

	valuesBytes, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal values: %w", err)
	}

	return valuesBytes, nil
}

func (s *store) Query(expression string, pageSize int) (api.Iterator, error) {
	var queryOptions spi.QueryOptions
	queryOptions.PageSize = 25

	commonIterator, err := s.commonStore.Query(expression, spi.WithPageSize(pageSize))
	if err != nil {
		return nil, fmt.Errorf("failed to query common store: %w", err)
	}

	return &iterator{commonIterator: commonIterator}, nil
}

func (s *store) Delete(key string) error {
	err := s.commonStore.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete in common store: %w", err)
	}

	return nil
}

func (s *store) Batch(operationsBytes []byte) error {
	var operations []spi.Operation
	
	err := json.Unmarshal(operationsBytes, &operations)
	if err != nil {
		return fmt.Errorf("failed to unmarshal operations: %w", err)
	}

	err = s.commonStore.Batch(operations)
	if err != nil {
		return fmt.Errorf("failed to execute batch operations in common store: %w", err)
	}

	return nil
}

func (s *store) Flush() error {
	err := s.commonStore.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush common store: %w", err)
	}

	return nil
}

func (s *store) Close() error {
	s.close(s.name)

	err := s.commonStore.Close()
	if err != nil {
		return fmt.Errorf("failed to close common store: %w", err)
	}

	return nil
}

type iterator struct {
	commonIterator spi.Iterator
}

func (i *iterator) Next() (bool, error) {
	next, err := i.commonIterator.Next()
	if err != nil {
		return false, fmt.Errorf("failed to move the entry pointer in the common iterator: %w", err)
	}

	return next, nil
}

func (i *iterator) Key() (string, error) {
	key, err := i.commonIterator.Key()
	if err != nil {
		return "", fmt.Errorf("failed to get key from common iterator: %w", err)
	}

	return key, nil
}

func (i *iterator) Value() ([]byte, error) {
	value, err := i.commonIterator.Value()
	if err != nil {
		return nil, fmt.Errorf("failed to get value from common iterator: %w", err)
	}

	return value, nil
}

func (i *iterator) Tags() ([]byte, error) {
	tags, err := i.commonIterator.Tags()
	if err != nil {
		return nil, fmt.Errorf("failed to get tags from common iterator: %w", err)
	}

	tagsBytes, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tags: %w", err)
	}

	return tagsBytes, nil
}

func (i *iterator) TotalItems() (int, error) {
	return i.commonIterator.TotalItems()
}

func (i *iterator) Close() error {
	err := i.commonIterator.Close()
	if err != nil {
		return fmt.Errorf("failed to close common iterator: %w", err)
	}

	return nil
}
