// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// StopSyncWithLeader stop to sync the region with leader.
func (s *RegionSyncer) StopSyncWithLeader() {
	s.reset()
	s.Lock()
	close(s.closed)
	s.closed = make(chan struct{})
	s.Unlock()
	s.wg.Wait()
}

func (s *RegionSyncer) reset() {
	s.Lock()
	defer s.Unlock()

	if s.cancel == nil {
		return
	}
	s.cancel()
	s.cancel, s.ctx = nil, nil
}

func (s *RegionSyncer) establish(addr string) (ClientStream, error) {
	s.reset()

	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	opt := grpc.WithInsecure()
	if len(s.securityConfig.CAPath) != 0 {
		certificates := []tls.Certificate{}
		if len(s.securityConfig.CertPath) != 0 && len(s.securityConfig.KeyPath) != 0 {
			// Load the client certificates from disk
			certificate, e := tls.LoadX509KeyPair(s.securityConfig.CertPath, s.securityConfig.KeyPath)
			if e != nil {
				return nil, errors.Errorf("could not load client key pair: %s", e)
			}
			certificates = append(certificates, certificate)
		}

		// Create a certificate pool from the certificate authority
		certPool := x509.NewCertPool()
		ca, e := ioutil.ReadFile(s.securityConfig.CAPath)
		if e != nil {
			return nil, errors.Errorf("could not read ca certificate: %s", e)
		}

		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to append ca certs")
		}

		creds := credentials.NewTLS(&tls.Config{
			Certificates: certificates,
			RootCAs:      certPool,
		})

		opt = grpc.WithTransportCredentials(creds)
	}

	cc, err := grpc.Dial(u.Host, opt, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(msgSize)))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(s.server.Context())
	client, err := pdpb.NewPDClient(cc).SyncRegions(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	err = client.Send(&pdpb.SyncRegionRequest{
		Header:     &pdpb.RequestHeader{ClusterId: s.server.ClusterID()},
		Member:     s.server.GetMemberInfo(),
		StartIndex: s.history.GetNextIndex(),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	s.Lock()
	s.ctx, s.cancel = ctx, cancel
	s.Unlock()
	return client, nil
}

// StartSyncWithLeader starts to sync with leader.
func (s *RegionSyncer) StartSyncWithLeader(addr string) {
	s.wg.Add(1)
	s.RLock()
	closed := s.closed
	s.RUnlock()
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-closed:
				return
			default:
			}
			// establish client
			client, err := s.establish(addr)
			if err != nil {
				if ev, ok := status.FromError(err); ok {
					if ev.Code() == codes.Canceled {
						return
					}
				}
				log.Error("server failed to establish sync stream with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), zap.Error(err))
				time.Sleep(time.Second)
				continue
			}
			log.Info("server starts to synchronize with leader", zap.String("server", s.server.Name()), zap.String("leader", s.server.GetLeader().GetName()), zap.Uint64("request-index", s.history.GetNextIndex()))
			for {
				resp, err := client.Recv()
				if err != nil {
					log.Error("region sync with leader meet error", zap.Error(err))
					if err = client.CloseSend(); err != nil {
						log.Error("failed to terminate client stream", zap.Error(err))
					}
					time.Sleep(time.Second)
					break
				}
				if s.history.GetNextIndex() != resp.GetStartIndex() {
					log.Warn("server sync index not match the leader",
						zap.String("server", s.server.Name()),
						zap.Uint64("own", s.history.GetNextIndex()),
						zap.Uint64("leader", resp.GetStartIndex()),
						zap.Int("records-length", len(resp.GetRegions())))
					// reset index
					s.history.ResetWithIndex(resp.GetStartIndex())
				}
				for _, r := range resp.GetRegions() {
					err = s.server.GetStorage().SaveRegion(r)
					if err == nil {
						s.history.Record(core.NewRegionInfo(r, nil))
					}
				}
			}
		}
	}()
}
