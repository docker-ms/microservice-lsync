package api

import (
	"os"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	"lsync/api/v1"
	"lsync/constant"
	"lsync/generator"
	"lsync/util"

	pbMs "microservice"
	pbMsLsync "microservice/lsync"
	pbMsLsyncV1 "microservice/lsync/v1"
)

type LsyncServer struct{}

func (s *LsyncServer) HealthCheck(ctx context.Context, in *pbMs.Empty) (*pbMs.HealthCheckRes, error) {
	return &pbMs.HealthCheckRes{
		MsServiceTag: os.Getenv("MS_SERVICE_TAG"),
	}, nil
}

func (s *LsyncServer) LsyncV1(in *pbMsLsyncV1.LsyncReq, stream pbMsLsync.Lsync_LsyncV1Server) error {
	if generator.IS_SNAPSHOT_DELTA_GEN_IP {
		return grpc.Errorf(codes.Internal, constant.SnapshotDeltaGenIP)
	}

	// ------------------------------------------------------------- For Zipkin -----------------------------------------------------------------

	ctx := getIncomingCtxFromGRPC(stream.Context(), opentracing.GlobalTracer(), "LsyncV1")

	if span := opentracing.SpanFromContext(ctx); span != nil {
		defer span.Finish()

		ext.SpanKindRPCServer.Set(span)
		span.SetTag("serviceType", "Go GRPC")
	}

	// ------------------------------------------------------------- End for Zipkin -------------------------------------------------------------

	return v1.Lsync(in, stream)
}

func (s *LsyncServer) AckV1(ctx context.Context, in *pbMsLsyncV1.AckReq) (*pbMs.BoolRes, error) {
	if generator.IS_SNAPSHOT_DELTA_GEN_IP {
		return nil, grpc.Errorf(codes.Internal, constant.SnapshotDeltaGenIP)
	}
	return v1.Ack(in)
}

func (s *LsyncServer) QueryLastSeenV1(ctx context.Context, in *pbMsLsyncV1.QueryLastSeenReq) (*pbMsLsyncV1.QueryLastSeenRes, error) {
	if generator.IS_SNAPSHOT_DELTA_GEN_IP {
		return nil, grpc.Errorf(codes.Internal, constant.SnapshotDeltaGenIP)
	}
	return v1.QueryLastSeen(in)
}

func (s *LsyncServer) XCacheV1(ctx context.Context, in *pbMsLsyncV1.DeviceBucketContent) (*pbMs.BoolRes, error) {
	return v1.Cache(in)
}

// Add non-exported stuffs below.

// ------------------------------------------------------------- For Zipkin -----------------------------------------------------------------

type metadataReader struct {
	*metadata.MD
}

func (mr metadataReader) ForeachKey(handler func(key, val string) error) error {
	for k, vals := range *mr.MD {
		for _, v := range vals {
			if e := handler(k, v); e != nil {
				return e
			}
		}
	}
	return nil
}

func getIncomingCtxFromGRPC(ctx context.Context, tracer opentracing.Tracer, opName string) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)

	if incomingCtx, e := tracer.Extract(opentracing.TextMap, metadataReader{&md}); e == nil && e != opentracing.ErrSpanContextNotFound {
		span := tracer.StartSpan(opName, ext.RPCServerOption(incomingCtx))
		return opentracing.ContextWithSpan(ctx, span)
	} else {
		util.Logger.Panic("Opentracing tracer could not extract metadata from incoming context.")
		panic(e)
	}
}

// ------------------------------------------------------------- End for Zipkin -------------------------------------------------------------
