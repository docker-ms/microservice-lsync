package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	opentracing "github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"

	"lsync/api"
	"lsync/boltdb"
	"lsync/consul"
	"lsync/generator"
	"lsync/model"
	"lsync/mongodb"
	"lsync/util"

	pbMsLsync "microservice/lsync"
)

func initTracerForZipkin(zipkinUrl, hostPort, serviceName string) {
	if collector, e := zipkin.NewHTTPCollector(zipkinUrl); e == nil {
		if tracer, e0 := zipkin.NewTracer(zipkin.NewRecorder(collector, false, hostPort, serviceName)); e0 == nil {
			opentracing.InitGlobalTracer(tracer)
		} else {
			util.Logger.Panic("Could not init tracer for Zipkin.")
			panic(e0)
		}
	} else {
		util.Logger.Panic("Could not init collector for Zipkin.")
		panic(e)
	}
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	util.InitSnapshotPoint()

	// ------------------------------------------------------------- For Zipkin -----------------------------------------------------------------
	zipkinUrl := "http://micro02.sgdev.vcube.com:64800/api/v1/spans"

	if strings.Index(os.Getenv("MS_SERVICE_TAG"), "localhost") == -1 {
		zipkinUrl = "http://zipkin_server_0:9411/api/v1/spans"
	}

	hostname, _ := os.Hostname()

	initTracerForZipkin(zipkinUrl, hostname, os.Getenv("MS_SERVICE_TAG"))
	// ------------------------------------------------------------- End for Zipkin -------------------------------------------------------------

	// Connect to consul to fetch MongoDB configuration info.
	kvPair, _, err := consul.ConsulAgentPool.PickRandomly().KV().Get("mongodb/gate", nil)

	if err != nil {
		util.Logger.Panic("Could not fetch MongoDB configuration info.")
		panic(err)
	}

	var mongodbConfig model.MongoDbConfig

	if err := json.Unmarshal(kvPair.Value, &mongodbConfig); err != nil {
		util.Logger.Panic("Failed to parse MongoDB configuration.")
		panic(err)
	}

	// Connect to MongoDB.
	mongodb.Connect(&mongodbConfig)

	defer func() {
		for _, session := range mongodb.ConnSessions {
			session.Close()
		}
	}()

	// Init boltdb for user buckets.
	boltdb.InitBoltDB(boltdb.BoltdbUserBucketsDataFilePath)

	go generator.SetUpCronJob()

	// Start GRPC server.
	if listener, err := net.Listen("tcp", fmt.Sprintf(":%d", 53547)); err == nil {
		lsyncGrpcServer := grpc.NewServer()
		pbMsLsync.RegisterLsyncServer(lsyncGrpcServer, &api.LsyncServer{})
		lsyncGrpcServer.Serve(listener)
	} else {
		grpclog.Fatalf("Failed to listen: %v", err)
	}

}
