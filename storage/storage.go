package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	buckets = []string{"data", "annotations", "config", "history"}
)

type DataPoint struct {
	ChartID   string             `json:"chart_id"`
	Timestamp int64              `json:"timestamp"`
	Values    map[string]float64 `json:"values"`
}

type Annotation struct {
	ID        string `json:"id"`
	ChartID   string `json:"chart_id"`
	Timestamp int64  `json:"timestamp"`
	Text      string `json:"text"`
	Pinned    bool   `json:"pinned"`
}

type Store struct {
	db   *bolt.DB
	mu   sync.RWMutex
	path string
}

func NewStore(dbPath string) (*Store, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists([]byte(b)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, path: dbPath}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) PutDataPoint(dp *DataPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(dp)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		key := fmt.Sprintf("%s:%d", dp.ChartID, dp.Timestamp)
		return b.Put([]byte(key), data)
	})
}

func (s *Store) PutHistoryPoint(dp *DataPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(dp)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("history"))
		key := fmt.Sprintf("%s:%d", dp.ChartID, dp.Timestamp)
		return b.Put([]byte(key), data)
	})
}

func (s *Store) GetDataPoints(chartID string, since, until int64) ([]*DataPoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var points []*DataPoint
	prefix := chartID + ":"
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		c := b.Cursor()
		for k, v := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
			if since > 0 {
				parts := strings.SplitN(string(k), ":", 2)
				if len(parts) == 2 {
					ts, _ := strconv.ParseInt(parts[1], 10, 64)
					if ts < since {
						continue
					}
					if until > 0 && ts > until {
						break
					}
				}
			}
			var dp DataPoint
			if err := json.Unmarshal(v, &dp); err == nil {
				points = append(points, &dp)
			}
		}
		return nil
	})
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
	return points, err
}

func (s *Store) GetHistoryPoints(chartID string, since, until int64) ([]*DataPoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var points []*DataPoint
	prefix := chartID + ":"
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("history"))
		c := b.Cursor()
		for k, v := c.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = c.Next() {
			if since > 0 {
				parts := strings.SplitN(string(k), ":", 2)
				if len(parts) == 2 {
					ts, _ := strconv.ParseInt(parts[1], 10, 64)
					if ts < since {
						continue
					}
					if until > 0 && ts > until {
						break
					}
				}
			}
			var dp DataPoint
			if err := json.Unmarshal(v, &dp); err == nil {
				points = append(points, &dp)
			}
		}
		return nil
	})
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp < points[j].Timestamp
	})
	return points, err
}

func (s *Store) ClearChartData(chartID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		prefix := []byte(chartID + ":")
		c := b.Cursor()
		var keys [][]byte
		for k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, _ = c.Next() {
			keys = append(keys, append([]byte{}, k...))
		}
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ClearChartHistory(chartID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("history"))
		prefix := []byte(chartID + ":")
		c := b.Cursor()
		var keys [][]byte
		for k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, _ = c.Next() {
			keys = append(keys, append([]byte{}, k...))
		}
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ClearAllHistory() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("history")); err != nil {
			return err
		}
		_, err := tx.CreateBucket([]byte("history"))
		return err
	})
}

func (s *Store) CountHistoryPoints(chartID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("history"))
		prefix := []byte(chartID + ":")
		c := b.Cursor()
		for k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, _ = c.Next() {
			count++
		}
		return nil
	})
	return count, err
}

func (s *Store) PutAnnotation(a *Annotation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("annotations"))
		return b.Put([]byte(a.ID), data)
	})
}

func (s *Store) GetAnnotations(chartID string) ([]*Annotation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var annotations []*Annotation
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("annotations"))
		return b.ForEach(func(k, v []byte) error {
			var a Annotation
			if err := json.Unmarshal(v, &a); err != nil {
				return nil
			}
			if chartID == "" || a.ChartID == chartID {
				annotations = append(annotations, &a)
			}
			return nil
		})
	})
	return annotations, err
}

func (s *Store) DeleteAnnotation(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("annotations"))
		return b.Delete([]byte(id))
	})
}

func (s *Store) SaveConfig(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("config"))
		return b.Put([]byte("config"), data)
	})
}

func (s *Store) LoadConfig() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("config"))
		data = b.Get([]byte("config"))
		return nil
	})
	if data == nil {
		data = []byte("{}")
	}
	return data, err
}

func (s *Store) TimeRange(chartID string) (oldest, newest int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("history"))
		prefix := []byte(chartID + ":")
		c := b.Cursor()
		if k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)) {
			parts := strings.SplitN(string(k), ":", 2)
			if len(parts) == 2 {
				oldest, _ = strconv.ParseInt(parts[1], 10, 64)
			}
		}
		c.Last()
		if k, _ := c.Prev(); k != nil {
			for ; k != nil; k, _ = c.Prev() {
				if strings.HasPrefix(string(k), string(prefix)) {
					parts := strings.SplitN(string(k), ":", 2)
					if len(parts) == 2 {
						newest, _ = strconv.ParseInt(parts[1], 10, 64)
					}
					break
				}
			}
		}
		return nil
	})
	return
}

func (s *Store) DBPath() string {
	return s.path
}
