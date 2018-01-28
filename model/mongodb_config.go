package model

type MongoDbConfig struct {
  Hosts []string `json:"hosts,omitempty"`
  DbName string `json:"dbName,omitempty"`
  ConnOpts struct {
    MaxPoolSize int `json:"maxPoolSize,omitempty"`
  } `json:"connOpts,omitempty"`
}


