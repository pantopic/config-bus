// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server-side error
var (
	ErrGRPCEmptyKey      = status.New(codes.InvalidArgument, "etcdserver: key is not provided").Err()
	ErrGRPCKeyNotFound   = status.New(codes.InvalidArgument, "etcdserver: key not found").Err()
	ErrGRPCValueProvided = status.New(codes.InvalidArgument, "etcdserver: value is provided").Err()
	ErrGRPCLeaseProvided = status.New(codes.InvalidArgument, "etcdserver: lease is provided").Err()
	ErrGRPCTooManyOps    = status.New(codes.InvalidArgument, "etcdserver: too many operations in txn request").Err()
	ErrGRPCDuplicateKey  = status.New(codes.InvalidArgument, "etcdserver: duplicate key given in txn request").Err()
	ErrGRPCCompacted     = status.New(codes.OutOfRange, "etcdserver: mvcc: required revision has been compacted").Err()
	ErrGRPCFutureRev     = status.New(codes.OutOfRange, "etcdserver: mvcc: required revision is a future revision").Err()
	ErrGRPCNoSpace       = status.New(codes.ResourceExhausted, "etcdserver: mvcc: database space exceeded").Err()

	ErrGRPCLeaseNotFound    = status.New(codes.NotFound, "etcdserver: requested lease not found").Err()
	ErrGRPCLeaseExist       = status.New(codes.FailedPrecondition, "etcdserver: lease already exists").Err()
	ErrGRPCLeaseTTLTooLarge = status.New(codes.OutOfRange, "etcdserver: too large lease TTL").Err()

	ErrGRPCWatchCanceled = status.New(codes.Canceled, "etcdserver: watch canceled").Err()

	ErrGRPCMemberExist            = status.New(codes.FailedPrecondition, "etcdserver: member ID already exist").Err()
	ErrGRPCPeerURLExist           = status.New(codes.FailedPrecondition, "etcdserver: Peer URLs already exists").Err()
	ErrGRPCMemberNotEnoughStarted = status.New(codes.FailedPrecondition, "etcdserver: re-configuration failed due to not enough started members").Err()
	ErrGRPCMemberBadURLs          = status.New(codes.InvalidArgument, "etcdserver: given member URLs are invalid").Err()
	ErrGRPCMemberNotFound         = status.New(codes.NotFound, "etcdserver: member not found").Err()
	ErrGRPCMemberNotLearner       = status.New(codes.FailedPrecondition, "etcdserver: can only promote a learner member").Err()
	ErrGRPCLearnerNotReady        = status.New(codes.FailedPrecondition, "etcdserver: can only promote a learner member which is in sync with leader").Err()
	ErrGRPCTooManyLearners        = status.New(codes.FailedPrecondition, "etcdserver: too many learner members in cluster").Err()
	ErrGRPCClusterIdMismatch      = status.New(codes.FailedPrecondition, "etcdserver: cluster ID mismatch").Err()

	ErrGRPCRequestTooLarge        = status.New(codes.InvalidArgument, "etcdserver: request is too large").Err()
	ErrGRPCRequestTooManyRequests = status.New(codes.ResourceExhausted, "etcdserver: too many requests").Err()

	ErrGRPCRootUserNotExist     = status.New(codes.FailedPrecondition, "etcdserver: root user does not exist").Err()
	ErrGRPCRootRoleNotExist     = status.New(codes.FailedPrecondition, "etcdserver: root user does not have root role").Err()
	ErrGRPCUserAlreadyExist     = status.New(codes.FailedPrecondition, "etcdserver: user name already exists").Err()
	ErrGRPCUserEmpty            = status.New(codes.InvalidArgument, "etcdserver: user name is empty").Err()
	ErrGRPCUserNotFound         = status.New(codes.FailedPrecondition, "etcdserver: user name not found").Err()
	ErrGRPCRoleAlreadyExist     = status.New(codes.FailedPrecondition, "etcdserver: role name already exists").Err()
	ErrGRPCRoleNotFound         = status.New(codes.FailedPrecondition, "etcdserver: role name not found").Err()
	ErrGRPCRoleEmpty            = status.New(codes.InvalidArgument, "etcdserver: role name is empty").Err()
	ErrGRPCAuthFailed           = status.New(codes.InvalidArgument, "etcdserver: authentication failed, invalid user ID or password").Err()
	ErrGRPCPermissionNotGiven   = status.New(codes.InvalidArgument, "etcdserver: permission not given").Err()
	ErrGRPCPermissionDenied     = status.New(codes.PermissionDenied, "etcdserver: permission denied").Err()
	ErrGRPCRoleNotGranted       = status.New(codes.FailedPrecondition, "etcdserver: role is not granted to the user").Err()
	ErrGRPCPermissionNotGranted = status.New(codes.FailedPrecondition, "etcdserver: permission is not granted to the role").Err()
	ErrGRPCAuthNotEnabled       = status.New(codes.FailedPrecondition, "etcdserver: authentication is not enabled").Err()
	ErrGRPCInvalidAuthToken     = status.New(codes.Unauthenticated, "etcdserver: invalid auth token").Err()
	ErrGRPCInvalidAuthMgmt      = status.New(codes.InvalidArgument, "etcdserver: invalid auth management").Err()
	ErrGRPCAuthOldRevision      = status.New(codes.InvalidArgument, "etcdserver: revision of auth store is old").Err()

	ErrGRPCNoLeader                   = status.New(codes.Unavailable, "etcdserver: no leader").Err()
	ErrGRPCNotLeader                  = status.New(codes.FailedPrecondition, "etcdserver: not leader").Err()
	ErrGRPCLeaderChanged              = status.New(codes.Unavailable, "etcdserver: leader changed").Err()
	ErrGRPCNotCapable                 = status.New(codes.Unavailable, "etcdserver: not capable").Err()
	ErrGRPCStopped                    = status.New(codes.Unavailable, "etcdserver: server stopped").Err()
	ErrGRPCTimeout                    = status.New(codes.Unavailable, "etcdserver: request timed out").Err()
	ErrGRPCTimeoutDueToLeaderFail     = status.New(codes.Unavailable, "etcdserver: request timed out, possibly due to previous leader failure").Err()
	ErrGRPCTimeoutDueToConnectionLost = status.New(codes.Unavailable, "etcdserver: request timed out, possibly due to connection lost").Err()
	ErrGRPCTimeoutWaitAppliedIndex    = status.New(codes.Unavailable, "etcdserver: request timed out, waiting for the applied index took too long").Err()
	ErrGRPCUnhealthy                  = status.New(codes.Unavailable, "etcdserver: unhealthy cluster").Err()
	ErrGRPCCorrupt                    = status.New(codes.DataLoss, "etcdserver: corrupt cluster").Err()
	ErrGPRCNotSupportedForLearner     = status.New(codes.Unavailable, "etcdserver: rpc not supported for learner").Err()
	ErrGRPCBadLeaderTransferee        = status.New(codes.FailedPrecondition, "etcdserver: bad leader transferee").Err()

	ErrGRPCClusterVersionUnavailable     = status.New(codes.Unavailable, "etcdserver: cluster version not found during downgrade").Err()
	ErrGRPCWrongDowngradeVersionFormat   = status.New(codes.InvalidArgument, "etcdserver: wrong downgrade target version format").Err()
	ErrGRPCInvalidDowngradeTargetVersion = status.New(codes.InvalidArgument, "etcdserver: invalid downgrade target version").Err()
	ErrGRPCDowngradeInProcess            = status.New(codes.FailedPrecondition, "etcdserver: cluster has a downgrade job in progress").Err()
	ErrGRPCNoInflightDowngrade           = status.New(codes.FailedPrecondition, "etcdserver: no inflight downgrade job").Err()

	ErrGRPCCanceled         = status.New(codes.Canceled, "etcdserver: request canceled").Err()
	ErrGRPCDeadlineExceeded = status.New(codes.DeadlineExceeded, "etcdserver: context deadline exceeded").Err()
)
