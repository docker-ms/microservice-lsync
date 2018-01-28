package consul

import (
	"math/rand"
	"os"
	"strings"
	"time"

	consulApi "github.com/hashicorp/consul/api"

	"lsync/util"
)

var ConsulAgentPool consulAgentPool

func (caPool consulAgentPool) PickRandomly() *consulApi.Client {
	if len(caPool) == 1 {
		return caPool[0]
	} else {
		rand.Seed(time.Now().Unix())
		return caPool[rand.Intn(len(caPool))]
	}
}

// Add non-exported stuffs below.

type consulAgentPool []*consulApi.Client

func init() {
	var consulHosts []string

	if strings.Contains(os.Getenv("MS_SERVICE_TAG"), "localhost") || strings.Contains(os.Getenv("SERVICE_TAG_SUFFIX"), "localhost") {
		consulHosts = []string{
			"10.0.3.159:65499",
			"10.0.3.159:65498",
			"10.0.3.159:65497",
		}
	} else {
		consulHosts = []string{
			"consul_agent_client_0:65401",
			"consul_agent_client_1:65401",
			"consul_agent_client_2:65401",
		}
	}

	consulAgents := make(consulAgentPool, 0, len(consulHosts))

	for _, consulHost := range consulHosts {
		config := consulApi.DefaultConfig()
		config.Address = consulHost
		agent, err := consulApi.NewClient(config)
		if err != nil {
			util.Logger.Warn(err)
		} else {
			consulAgents = append(consulAgents, agent)
		}
	}

	ConsulAgentPool = consulAgents
}
