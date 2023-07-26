package util

import "encoding/json"

type RawJsonMap map[string]json.RawMessage

func (m RawJsonMap) Upsert(key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}

	m[key] = b
	return nil
}

func (m RawJsonMap) MustUpsert(key string, value any) {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}

	m[key] = b
}
