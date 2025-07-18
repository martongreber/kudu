// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/consensus/quorum_util.h"

#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace consensus {

// Handy notation of membership types used by AddPeer(), etc.
constexpr auto N = RaftPeerPB::NON_VOTER;           // NOLINT(readability-identifier-naming)
constexpr auto U = RaftPeerPB::UNKNOWN_MEMBER_TYPE; // NOLINT(readability-identifier-naming)
constexpr auto V = RaftPeerPB::VOTER;               // NOLINT(readability-identifier-naming)

// The various possible health statuses.
const auto kHealthStatuses = { '?', '-', 'x', '+' };

typedef std::pair<string, bool> Attr;

static void SetOverallHealth(HealthReportPB* health_report,
                             char overall_health) {
  switch (overall_health) {
    case '+':
      health_report->set_overall_health(HealthReportPB::HEALTHY);
      break;
    case '-':
      health_report->set_overall_health(HealthReportPB::FAILED);
      break;
    case 'x':
      health_report->set_overall_health(HealthReportPB::FAILED_UNRECOVERABLE);
      break;
    case '?':
      health_report->set_overall_health(HealthReportPB::UNKNOWN);
      break;
    default:
      FAIL() << overall_health << ": unexpected replica health status";
      break;
  }
}

// Add a consensus peer into the specified configuration.
static void AddPeer(RaftConfigPB* config,
                    const string& uuid,
                    RaftPeerPB::MemberType type,
                    std::optional<char> overall_health = std::nullopt,
                    vector<Attr> attrs = {}) {
  RaftPeerPB* peer = config->add_peers();
  peer->set_permanent_uuid(uuid);
  peer->mutable_last_known_addr()->set_host(uuid + ".example.com");
  peer->set_member_type(type);
  if (overall_health) {
    auto* health_report = peer->mutable_health_report();
    SetOverallHealth(health_report, *overall_health);
  }
  if (!attrs.empty()) {
    auto* attrs_pb = peer->mutable_attrs();
    for (const auto& attr : attrs) {
      if (attr.first == "PROMOTE") {
        attrs_pb->set_promote(attr.second);
      } else if (attr.first == "REPLACE") {
        attrs_pb->set_replace(attr.second);
      } else {
        FAIL() << attr.first << ": unexpected attribute to set";
      }
    }
  }
}

using RaftMemberSpec = pair<string, RaftPeerPB::MemberType>;

static RaftConfigPB CreateConfig(const vector<RaftMemberSpec>& specs) {
  RaftConfigPB config;
  for (const auto& spec : specs) {
    AddPeer(&config, spec.first, spec.second);
  }
  return config;
}

static void PromotePeer(RaftConfigPB* config, const string& peer_uuid) {
  RaftPeerPB* peer_pb;
  const Status s = GetRaftConfigMember(config, peer_uuid, &peer_pb);
  if (!s.ok()) {
    FAIL() << peer_uuid << ": " << s.ToString();
  }
  peer_pb->set_member_type(V);
  //peer_pb->mutable_attrs()->clear_promote();
  peer_pb->mutable_attrs()->set_promote(false);
}

static void RemovePeer(RaftConfigPB* config, const string& peer_uuid) {
  if (!RemoveFromRaftConfig(config, peer_uuid)) {
    FAIL() << peer_uuid << ": peer is not in the config";
  }
}

static void SetPeerHealth(RaftConfigPB* config, const string& uuid, char health) {
  RaftPeerPB* peer_pb;
  const Status s = GetRaftConfigMember(config, uuid, &peer_pb);
  if (!s.ok()) {
    FAIL() << "unexpected failure from GetRaftConfigMember(): " << s.ToString();
  }
  SetOverallHealth(peer_pb->mutable_health_report(), health);
}

TEST(QuorumUtilTest, TestMemberExtraction) {
  RaftConfigPB config;
  AddPeer(&config, "A", V);
  AddPeer(&config, "B", V);
  AddPeer(&config, "C", V);

  // Basic test for GetRaftConfigMember().
  RaftPeerPB* peer_pb;
  Status s = GetRaftConfigMember(&config, "invalid", &peer_pb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(GetRaftConfigMember(&config, "A", &peer_pb));
  ASSERT_EQ("A", peer_pb->permanent_uuid());

  // Basic test for GetRaftConfigLeader().
  ConsensusStatePB cstate;
  *cstate.mutable_committed_config() = config;
  s = GetRaftConfigLeader(&cstate, &peer_pb);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  cstate.set_leader_uuid("B");
  ASSERT_OK(GetRaftConfigLeader(&cstate, &peer_pb));
  ASSERT_EQ("B", peer_pb->permanent_uuid());
}

TEST(QuorumUtilTest, TestDiffConsensusStates) {
  ConsensusStatePB old_cs;
  AddPeer(old_cs.mutable_committed_config(), "A", V);
  AddPeer(old_cs.mutable_committed_config(), "B", V);
  AddPeer(old_cs.mutable_committed_config(), "C", V);
  old_cs.set_current_term(1);
  old_cs.set_leader_uuid("A");
  old_cs.mutable_committed_config()->set_opid_index(1);

  // Simple case of no change.
  EXPECT_EQ("no change",
            DiffConsensusStates(old_cs, old_cs));

  // Simulate a leader change.
  {
    auto new_cs = old_cs;
    new_cs.set_leader_uuid("B");
    new_cs.set_current_term(2);

    EXPECT_EQ("term changed from 1 to 2, "
              "leader changed from A (A.example.com) to B (B.example.com)",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate eviction of a peer.
  {
    auto new_cs = old_cs;
    new_cs.mutable_committed_config()->set_opid_index(2);
    new_cs.mutable_committed_config()->mutable_peers()->RemoveLast();

    EXPECT_EQ("config changed from index 1 to 2, "
              "VOTER C (C.example.com) evicted",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate addition of a peer.
  {
    auto new_cs = old_cs;
    new_cs.mutable_committed_config()->set_opid_index(2);
    AddPeer(new_cs.mutable_committed_config(), "D", N);

    EXPECT_EQ("config changed from index 1 to 2, "
              "NON_VOTER D (D.example.com) added",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate change of a peer's member type.
  {
    auto new_cs = old_cs;
    new_cs.mutable_committed_config()->set_opid_index(2);
    new_cs.mutable_committed_config()
      ->mutable_peers()->Mutable(2)->set_member_type(N);

    EXPECT_EQ("config changed from index 1 to 2, "
              "C (C.example.com) changed from VOTER to NON_VOTER",
              DiffConsensusStates(old_cs, new_cs));
  }

  // Simulate change from no leader to a leader
  {
    auto no_leader_cs = old_cs;
    no_leader_cs.clear_leader_uuid();
    auto new_cs = old_cs;
    new_cs.set_current_term(2);

    EXPECT_EQ("term changed from 1 to 2, "
              "leader changed from <none> to A (A.example.com)",
              DiffConsensusStates(no_leader_cs, new_cs));
  }

  // Simulate gaining a pending config
  {
    auto pending_config_cs = old_cs;
    pending_config_cs.mutable_pending_config();
    EXPECT_EQ("now has a pending config: ", DiffConsensusStates(old_cs, pending_config_cs));
  }

  // Simulate losing a pending config
  {
    auto pending_config_cs = old_cs;
    pending_config_cs.mutable_pending_config();
    EXPECT_EQ("no longer has a pending config: ", DiffConsensusStates(pending_config_cs, old_cs));
  }

  // Simulate a change in a pending config
  {
    auto before_cs = old_cs;
    AddPeer(before_cs.mutable_pending_config(), "A", V);
    auto after_cs = before_cs;
    after_cs.mutable_pending_config()
      ->mutable_peers()->Mutable(0)->set_member_type(N);

    EXPECT_EQ("pending config changed, A (A.example.com) changed from VOTER to NON_VOTER",
              DiffConsensusStates(before_cs, after_cs));
  }
}

// Unit test for the variants of GetConsensusRole().
TEST(QuorumUtilTest, TestGetConsensusRole) {
  const auto LEADER = RaftPeerPB::LEADER;
  const auto FOLLOWER = RaftPeerPB::FOLLOWER;
  const auto LEARNER = RaftPeerPB::LEARNER;
  const auto NON_PARTICIPANT = RaftPeerPB::NON_PARTICIPANT;

  // 3-argument variant of GetConsensusRole().
  const auto config1 = CreateConfig({ {"A", V}, {"B", V}, {"C", N} });
  ASSERT_EQ(LEADER, GetConsensusRole("A", "A", config1));
  ASSERT_EQ(FOLLOWER, GetConsensusRole("B", "A", config1));
  ASSERT_EQ(FOLLOWER, GetConsensusRole("A", "", config1));
  ASSERT_EQ(LEARNER, GetConsensusRole("C", "A", config1));
  ASSERT_EQ(LEARNER, GetConsensusRole("C", "C", config1)); // Illegal.
  ASSERT_EQ(NON_PARTICIPANT, GetConsensusRole("D", "A", config1));
  ASSERT_EQ(NON_PARTICIPANT, GetConsensusRole("D", "D", config1)); // Illegal.
  ASSERT_EQ(NON_PARTICIPANT, GetConsensusRole("", "A", config1)); // Illegal.
  ASSERT_EQ(NON_PARTICIPANT, GetConsensusRole("", "", config1)); // Illegal.

  // 2-argument variant of GetConsensusRole().
  const auto config2 = CreateConfig({ {"A", V}, {"B", V}, {"C", V} });
  ConsensusStatePB cstate;
  *cstate.mutable_committed_config() = config1;
  *cstate.mutable_pending_config() = config2;
  cstate.set_leader_uuid("A");
  ASSERT_EQ(LEADER, GetConsensusRole("A", cstate));
  ASSERT_EQ(FOLLOWER, GetConsensusRole("B", cstate));
  ASSERT_EQ(FOLLOWER, GetConsensusRole("C", cstate));
  ASSERT_EQ(NON_PARTICIPANT, GetConsensusRole("D", cstate));
  cstate.set_leader_uuid("D");
  ASSERT_EQ(NON_PARTICIPANT, GetConsensusRole("D", cstate)); // Illegal.

  // Test GetParticipantRole() on the participants in the config.
  for (const auto& peer : config2.peers()) {
    ASSERT_EQ(GetParticipantRole(peer, cstate),
              GetConsensusRole(peer.permanent_uuid(), cstate));
  }
}

TEST(QuorumUtilTest, TestIsRaftConfigVoter) {
  RaftConfigPB config;
  AddPeer(&config, "A", V);
  AddPeer(&config, "B", N);
  AddPeer(&config, "C", U);

  // The case when membership type is not specified. That sort of configuration
  // would not pass VerifyRaftConfig(), though. Anyway, that should result
  // in non-voter since the member_type is initialized with UNKNOWN_MEMBER_TYPE.
  const string no_member_type_peer_uuid = "D";
  RaftPeerPB* no_member_type_peer = config.add_peers();
  no_member_type_peer->set_permanent_uuid(no_member_type_peer_uuid);
  no_member_type_peer->mutable_last_known_addr()->set_host(
      no_member_type_peer_uuid + ".example.com");

  ASSERT_TRUE(IsRaftConfigVoter("A", config));
  ASSERT_FALSE(IsRaftConfigVoter("B", config));
  ASSERT_FALSE(IsRaftConfigVoter("C", config));
  ASSERT_FALSE(IsRaftConfigVoter(no_member_type_peer_uuid, config));

  RaftPeerPB* peer_a;
  ASSERT_OK(GetRaftConfigMember(&config, "A", &peer_a));
  RaftPeerPB* peer_b;
  ASSERT_OK(GetRaftConfigMember(&config, "B", &peer_b));
  ASSERT_FALSE(ReplicaTypesEqual(*peer_a, *peer_b));
  ASSERT_TRUE(ReplicaTypesEqual(*peer_b, *peer_b));
  RaftPeerPB* peer_c;
  ASSERT_OK(GetRaftConfigMember(&config, "C", &peer_c));
  ASSERT_FALSE(ReplicaTypesEqual(*peer_b, *peer_c));
}

// Verify basic functionality of the kudu::consensus::ShouldAddReplica() utility
// function.
TEST(QuorumUtilTest, ShouldAddReplica) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V);
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", V);
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
    // The configuration is under-replicated, but there are not enough healthy
    // voters to commit the configuration change.
    EXPECT_FALSE(ShouldAddReplica(config, 4));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '?');
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
    // The configuration is under-replicated, but there are not enough healthy
    // voters to commit the configuration change.
    EXPECT_FALSE(ShouldAddReplica(config, 4));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, health_status);
    AddPeer(&config, "B", V, health_status);
    AddPeer(&config, "C", V, health_status);
    // The configuration is under-replicated, but there are not enough healthy
    // voters to commit the configuration change.
    EXPECT_FALSE(ShouldAddReplica(config, 4));
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '-');
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    // The configuration is under-replicated, but there are not enough healthy
    // voters to commit the configuration change.
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", N, '+');
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", N, '+');
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    // The configuration is under-replicated, but there are not enough healthy
    // voters to commit the configuration change.
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, health_status);
    AddPeer(&config, "C", N, '+');
    // The configuration is over-replicated already.
    EXPECT_FALSE(ShouldAddReplica(config, 1));
    // Not enough voters to commit the change.
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, health_status);
    AddPeer(&config, "C", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldAddReplica(config, 1));
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    // The configuration is under-replicated, but there are not enough healthy
    // voters to commit the configuration change.
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, health_status);
    AddPeer(&config, "C", N, health_status, {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldAddReplica(config, 1));
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, health_status);
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '?');
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    // The catalog manager should wait for a definite health status of replica
    // 'C' before making decision whether to add replica for replacement or not.
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    EXPECT_TRUE(ShouldAddReplica(config, 3));
    EXPECT_FALSE(ShouldAddReplica(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", N, '+');
    EXPECT_TRUE(ShouldAddReplica(config, 4));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
    EXPECT_FALSE(ShouldAddReplica(config, 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_TRUE(ShouldAddReplica(config, 4));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
    EXPECT_FALSE(ShouldAddReplica(config, 2));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, health_status);
    AddPeer(&config, "D", N, health_status);
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, health_status);
    AddPeer(&config, "D", N, '+');
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    // The non-voter replica does not have the PROMOTE attribute,
    // so a new one is needed.
    EXPECT_TRUE(ShouldAddReplica(config, 3));
    EXPECT_TRUE(ShouldAddReplica(config, 4));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, health_status);
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
    EXPECT_TRUE(ShouldAddReplica(config, 4));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, health_status);
    AddPeer(&config, "D", N, health_status, {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldAddReplica(config, 2));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, health_status);
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, health_status, {{"PROMOTE", true}});
    AddPeer(&config, "E", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, health_status);
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, health_status, {{"PROMOTE", true}});
    AddPeer(&config, "E", N, '+', {{"PROMOTE", false}});
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, health_status);
    AddPeer(&config, "C", V, health_status);
    // If honoring the health of the replica's majority, the catalog manager
    // will not add a new non-voter replica until the situation is resolved.
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, health_status);
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, health_status);
    AddPeer(&config, "E", V, '+');
    EXPECT_FALSE(ShouldAddReplica(config, 3));
    EXPECT_TRUE(ShouldAddReplica(config, 4));
    EXPECT_TRUE(ShouldAddReplica(config, 5));
  }
}

// Test that when tablet replicas are ignored for underreplication (e.g. due to
// maintenance mode of a tablet server), the decision to add a replica will
// actually ignore failures as appropriate.
TEST(QuorumUtilTest, ShouldAddReplicaIgnoreFailures) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", V, '+');
    // The failed server is ignored, and doesn't count towards being
    // under-replicated. Note: The server with unknown health also doesn't
    // count towards being under-replicated.
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "B" }));
    // While the server with unknown health doesn't count towards being
    // under-replicated, the failed server does. But since we require a
    // majority to add replicas, we can't add a replica.
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "A" }));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    // This is healthy, with or without ignoring failures.
    EXPECT_FALSE(ShouldAddReplica(config, 3));
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "A" }));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    // But when a healthy server is in maintenance mode, we should consider the
    // unhealthy server as failed and add a replica.
    EXPECT_TRUE(ShouldAddReplica(config, 3, { "B" }));
    // When the unhealthy server is in maintenance mode, we shouldn't add a
    // replica, since all three servers aren't considered failed.
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "A" }));
    // And when everything is in maintenance mode, we shouldn't add a replica
    // even though a majority exists.
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "A", "B", "C" }));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", V, '+');
    // A majority doesn't exist, so no matter what failures are being ignored,
    // we will not add a replica.
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "A" }));
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "A", "B" }));
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "A", "B", "C" }));
  }
  {
    // Ignored servers shouldn't change the decision when we really are
    // under-replicated.
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');

    // No majority present.
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "A" }));
    EXPECT_FALSE(ShouldAddReplica(config, 3, { "B" }));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    AddPeer(&config, "E", V, '+');
    // When both failed replicas are being ignored, we shouldn't add a replica.
    EXPECT_FALSE(ShouldAddReplica(config, 5, { "A", "B" }));
    // When only one of them is ignored, we should.
    EXPECT_TRUE(ShouldAddReplica(config, 5, { "A" }));
  }
}

// Test that when tablet replicas are ignored for underreplication, replace is
// still honored as appropriate.
TEST(QuorumUtilTest, ShouldAddReplicaHonorReplaceWhenIgnoringFailures) {
  // Even if the replica to replace is meant to be ignored on failure, we
  // should honor the replacement and try to add a replica.
  for (char health : { '+', '-', '?' }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, health, {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    EXPECT_TRUE(ShouldAddReplica(config, 3, { "A" }));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", V, '+');
    // Ignoring failures shouldn't impede our ability to add a replica when the
    // "ignored" server is actually healthy.
    EXPECT_TRUE(ShouldAddReplica(config, 3, { "A" }));
  }
}

TEST(QuorumUtilTest, ShouldAddReplicaHonorPromoteWhenIgnoringFailures) {
  // If one of our replicas to promote has failed, and we are supposed to
  // ignore its failure, we should not add a replica because of it.
  // And if they're healthy or unknown, we also shouldn't add a replica.
  for (char health : { '+', '-', '?' }) {
    {
      RaftConfigPB config;
      AddPeer(&config, "A", N, health, {{"PROMOTE", true}});
      AddPeer(&config, "B", V, '+');
      AddPeer(&config, "C", V, '+');
      EXPECT_FALSE(ShouldAddReplica(config, 3, { "A" }));
    }
    {
      RaftConfigPB config;
      AddPeer(&config, "A", N, health, {{"PROMOTE", true}});
      AddPeer(&config, "B", N, '-', {{"PROMOTE", true}});
      AddPeer(&config, "C", V, '+');
      AddPeer(&config, "D", V, '+');
      AddPeer(&config, "E", V, '+');
      EXPECT_FALSE(ShouldAddReplica(config, 5, { "A", "B" }));
      // But when there is a failure that isn't supposed to be ignored (B), we
      // should add a replica.
      EXPECT_TRUE(ShouldAddReplica(config, 5, { "A" }));
    }
  }
}

// Verify logic of the kudu::consensus::ShouldEvictReplica(), anticipating
// removal of a voter replica.
TEST(QuorumUtilTest, ShouldEvictReplicaVoters) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", V, '+');
    // Not safe to evict because we don't have enough healthy nodes to commit
    // the eviction.
    EXPECT_FALSE(ShouldEvictReplica(config, "C", 1));
    EXPECT_FALSE(ShouldEvictReplica(config, "C", 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", V);
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+', {{"REPLACE", false}});
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", V, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 4));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("C", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 4));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("B", to_evict);
  }
  for (char health_status : kHealthStatuses) {
    SCOPED_TRACE(Substitute("replica health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", V, health_status, {{"REPLACE", true}});
    // For replication factors <= 3 we will be able to commit the eviction of D
    // with only A and B, regardless of D's health and regardless of the
    // desired replication factor.
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 2, &to_evict));
    // The priority of voter replica replacement (decreasing):
    //   * failed & slated for replacement
    //   * failed
    //   * ...
    if (health_status == '-' || health_status == 'x') {
      EXPECT_EQ("D", to_evict);
    } else {
      EXPECT_EQ("C", to_evict);
    }
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    if (health_status == '-' || health_status == 'x') {
      EXPECT_EQ("D", to_evict);
    } else {
      EXPECT_EQ("C", to_evict);
    }
    if (health_status == 'x') {
      // Unrecoverably failed replica should be evicted even if the configuration
      // is not over-replicated if it's safe to commit the configuration change.
      ASSERT_TRUE(ShouldEvictReplica(config, "A", 4));
      EXPECT_EQ("D", to_evict);
    } else {
      // Since we are not over-replicated, we will not evict in this case.
      EXPECT_FALSE(ShouldEvictReplica(config, "A", 4));
    }
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '?');
    AddPeer(&config, "D", V, '+', {{"REPLACE", true}});

    // For the replication factor 3, it's too early to evict 'C': it might be
    // in a good health, actually (reported, say, next heartbeat). Evicting 'D'
    // at this step is not a good idea neither: the 'C' might appear to fail,
    // and then it's better to keep 'D' around to provide the required
    // replication factor. It's necessary to wait for more deterministic status
    // of replica 'C' before making proper eviction decision.
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));

    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 2, &to_evict));
    EXPECT_EQ("D", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 4));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("B", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, 'x');
    AddPeer(&config, "C", V, '+');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("B", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 2, &to_evict));
    EXPECT_EQ("B", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", V, 'x');

    // No majority to commit the change.
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 2));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V);
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", V);
    EXPECT_FALSE(ShouldEvictReplica(config, "", 2));
    EXPECT_FALSE(ShouldEvictReplica(config, "", 3));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, health_status);
    EXPECT_FALSE(ShouldEvictReplica(config, "", 3));
    EXPECT_FALSE(ShouldEvictReplica(config, "", 2));
  }
  for (auto health_status : { '-', 'x' }) {
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, health_status);
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("C", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 2));
    EXPECT_EQ("C", to_evict);
    if (health_status == '-') {
      EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    } else {
      ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
      EXPECT_EQ("C", to_evict);
    }
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '?');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", V, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "C", 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '?');
    AddPeer(&config, "C", V, 'x');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
  }
}

// Verify logic of the kudu::consensus::ShouldEvictReplica(), anticipating
// removal of a non-voter replica.
TEST(QuorumUtilTest, ShouldEvictReplicaNonVoters) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V);
    EXPECT_FALSE(ShouldEvictReplica(config, "", 1));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 1));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", N);
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 2));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("B", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", N, '+');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 2, &to_evict));
    EXPECT_EQ("C", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("C", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", N, '-', {{"PROMOTE", true}});
    string to_evict;
    // It's always safe to evict an unhealthy non-voter if we have enough
    // healthy voters to commit the config change.
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 2, &to_evict));
    EXPECT_EQ("B", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("B", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", N, '-');
    AddPeer(&config, "C", N);
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 2, &to_evict));
    EXPECT_EQ("B", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("B", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", N, '?');
    AddPeer(&config, "C", N, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 2));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("B", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", N);
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 2, &to_evict));
    EXPECT_EQ("C", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("C", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", N);
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 2));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    // Would evict a non-voter first, but it's not known whether the majority
    // of the voter replicas are on-line to commence the operation: that's
    // because the state of B is unknown. So, in this case the voter replica B
    // will be removed first.
    EXPECT_EQ("B", to_evict);

    RemovePeer(&config, "B");
    // Now, having just a single online replica, it's possible to evict the
    // failed non-voter replica C.
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("C", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V);
    AddPeer(&config, "C", N);
    EXPECT_FALSE(ShouldEvictReplica(config, "", 2));
    EXPECT_FALSE(ShouldEvictReplica(config, "", 1));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldEvictReplica(config, "B", 3));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 2, &to_evict));
    EXPECT_EQ("D", to_evict);
  }
  {
    // Make sure failed non-voter replicas are removed from the configuration to
    // avoid polluting all tablet servers with failed non-voter replicas.
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '-', {{"PROMOTE", true}});
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 4, &to_evict));
    EXPECT_EQ("D", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "C", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '?', {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldEvictReplica(config, "B", 3));
    EXPECT_FALSE(ShouldEvictReplica(config, "B", 4));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '-');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, 'x', {{"PROMOTE", true}});
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 4, &to_evict));
    EXPECT_EQ("D", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, 'x');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, 'x');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 4, &to_evict));
    EXPECT_EQ("D", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, 'x');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '-');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 3, &to_evict));
    EXPECT_EQ("A", to_evict);
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 4, &to_evict));
    EXPECT_EQ("A", to_evict);
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '-');
    AddPeer(&config, "C", N, '-', {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 2));
    // Would evict a non-voter first, but replica B is reported as failed and
    // the configuration does not have enough healthy voter replicas to have a
    // majority of votes. So, the voter replica B will be removed first.
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("B", to_evict);

    RemovePeer(&config, "B");
    // Now, having just a single online replica, it's possible to evict the
    // failed non-voter replica C.
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
    EXPECT_EQ("C", to_evict);
  }
}

TEST(QuorumUtilTest, DontEvictLeader) {
  const vector<string> replicas = { "A", "B", "C", "D" };
  RaftConfigPB config;
  AddPeer(&config, replicas[0], V, '+');
  AddPeer(&config, replicas[1], V, '+');
  AddPeer(&config, replicas[2], V, '+');
  AddPeer(&config, replicas[3], V, '+');

  // Exhaustively loop through all nodes, each as leader, when over-replicated
  // and ensure that the leader never gets evicted.
  for (const auto& leader : replicas) {
    SCOPED_TRACE(Substitute("leader $0", leader));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, leader, 3, &to_evict));
    ASSERT_NE(leader, to_evict);
  }
}

// This is a scenario for tablet configurations with more than the required
// number of voter replicas.
TEST(QuorumUtilTest, TooManyVoters) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '?');
    AddPeer(&config, "D", V, '-');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+');
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '-');
    AddPeer(&config, "D", V, '-');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_TRUE(to_evict == "C" || to_evict == "D") << to_evict;
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
}

// Basic scenarios involving replicas with the REPLACE attribute set.
TEST(QuorumUtilTest, ReplaceAttributeBasic) {
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    EXPECT_TRUE(ShouldAddReplica(config, 1));
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 1));
  }
  {
    // Regression test scenario for KUDU-2443.
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    EXPECT_FALSE(ShouldAddReplica(config, 1));
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 1));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "B", 1, &to_evict));
    EXPECT_EQ("A", to_evict);
  }
  {
    for (auto health_status : { '+', '-', '?', 'x' }) {
      SCOPED_TRACE(Substitute("health status '$0'", health_status));
      RaftConfigPB config;
      AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
      AddPeer(&config, "B", N, health_status);
      EXPECT_TRUE(ShouldAddReplica(config, 1));
      if (health_status == '+' || health_status == '?') {
        EXPECT_FALSE(ShouldEvictReplica(config, "A", 1));
      } else {
        string to_evict;
        ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
        EXPECT_EQ("B", to_evict);
      }
    }
  }
  // If a non-voter replica with PROMOTE=true is already in the Raft config,
  // no need to add an additional one if the health status of the non-voter
  // replica is HEALTHY or UNKNOWN.
  {
    for (auto health_status : { '+', '-', '?', 'x' }) {
      SCOPED_TRACE(Substitute("health status '$0'", health_status));
      RaftConfigPB config;
      AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
      AddPeer(&config, "B", N, health_status, {{"PROMOTE", true}});
      if (health_status == '+' || health_status == '?') {
        EXPECT_FALSE(ShouldAddReplica(config, 1));
      } else {
        EXPECT_TRUE(ShouldAddReplica(config, 1));
      }
      if (health_status == '+' || health_status == '?') {
        EXPECT_FALSE(ShouldEvictReplica(config, "A", 1));
      } else {
        string to_evict;
        ASSERT_TRUE(ShouldEvictReplica(config, "A", 1, &to_evict));
        EXPECT_EQ("B", to_evict);
      }
    }
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_FALSE(ShouldAddReplica(config, 3));

    for (const auto& leader_replica : { "B", "C", "D" }) {
      string to_evict;
      SCOPED_TRACE(Substitute("leader $0", leader_replica));
      ASSERT_TRUE(ShouldEvictReplica(config, leader_replica, 3, &to_evict));
      EXPECT_EQ("A", to_evict);
    }
  }
  for (auto health_status : { '-', '?', 'x' }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, health_status, {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    for (const auto& leader_replica : { "B", "C", "D" }) {
      SCOPED_TRACE(Substitute("health status '$0', leader $1",
                              health_status, leader_replica));
      string to_evict;
      ASSERT_TRUE(ShouldEvictReplica(config, leader_replica, 3, &to_evict));
      EXPECT_EQ("A", to_evict);
      EXPECT_FALSE(ShouldAddReplica(config, 3));
    }
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    AddPeer(&config, "E", V, '+');
    // There should be no attempt to evict the leader.
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_NE("A", to_evict);
    EXPECT_FALSE(ShouldAddReplica(config, 3));

    for (const auto& leader_replica : { "B", "C", "D", "E" }) {
      string to_evict;
      SCOPED_TRACE(Substitute("leader $0", leader_replica));
      ASSERT_TRUE(ShouldEvictReplica(config, leader_replica, 3, &to_evict));
      EXPECT_EQ("A", to_evict);
    }
  }
  for (auto replica_health : kHealthStatuses) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", V, replica_health, {{"REPLACE", true}});
    SCOPED_TRACE(Substitute("replica health status '$0'", replica_health));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    if (replica_health == '+') {
      EXPECT_NE("A", to_evict);
    } else {
      EXPECT_EQ("D", to_evict);
    }
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '?', '-', 'x' }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, health_status, {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", V, '+');
    for (const auto& leader_replica : { "B", "C", "D" }) {
      SCOPED_TRACE(Substitute("health status '$0', leader $1",
                              health_status, leader_replica));
      string to_evict;
      ASSERT_TRUE(ShouldEvictReplica(config, leader_replica, 3, &to_evict));
      EXPECT_EQ("A", to_evict);
      EXPECT_TRUE(ShouldAddReplica(config, 3));
    }
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", V, '-');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", V, '?');
    EXPECT_FALSE(ShouldEvictReplica(config, "B", 3));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '?', '-', 'x' }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '?');
    AddPeer(&config, "D", V, health_status, {{"REPLACE", true}});
    AddPeer(&config, "E", V, '+');
    SCOPED_TRACE(Substitute("health status '$0'", health_status));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
}

// Test specific to the scenarios where the leader replica itself marked with
// the 'REPLACE' attribute.
TEST(QuorumUtilTest, LeaderReplicaWithReplaceAttribute) {
  // Healthy excess voter replicas (both voters and non-voters) should not be
  // evicted when the leader is marked with the 'REPLACE' attribute.
  for (auto health_status : { '+', '?' }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, health_status, {{"PROMOTE", true}});
    SCOPED_TRACE(Substitute("non-voter replica with status '$0'", health_status));
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  for (auto health_status : { '+', '?' }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, health_status);
    SCOPED_TRACE(Substitute("voter replica with status '$0'", health_status));
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  for (auto promote : { false, true }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '-', {{"PROMOTE", promote}});
    SCOPED_TRACE(Substitute(
        "failed non-voter replica with PROMOTE attribute $0",
        promote ? "set" : "unset"));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '-');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    // Current algorithm is conservative in the cases like below, but we might
    // evict non-voter replica 'E' which does not have the PROMOTE attribute.
    // TODO(aserbin): clarify on this.
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    AddPeer(&config, "E", N, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    // The non-voter replica does not have the 'promote' attribute, so
    // it should be evicted since it's not going to become a voter anyway,
    // and we don't support standby non-voter replicas at this point.
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    AddPeer(&config, "E", N, '+');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("E", to_evict);
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    // In the case below the non-voter replica 'D' is not needed. The
    // configuration like that might be the result of an attempt to replace 'D'
    // which was previously reported as failed. However, by the time the newly
    // added replica caught up with the leader, replica 'E' was back on-line.
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    AddPeer(&config, "E", V, '+');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
}

// This test is specific for various scenarios when multiple replicas have the
// REPLACE attribute set.
TEST(QuorumUtilTest, MultipleReplicasWithReplaceAttribute) {
  for (auto replica_type : { N, V }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", replica_type, '-');
    SCOPED_TRACE(Substitute("replica of $0 type",
                            RaftPeerPB::MemberType_Name(replica_type)));
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (auto replica_health : { '+', '?' }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", N, replica_health);
    SCOPED_TRACE(Substitute("NON_VOTER replica with health status '$0'",
                            replica_health));
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (const auto& leader_replica : { "A", "B", "C" }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", V, '?');
    SCOPED_TRACE(Substitute("leader $0", leader_replica));
    EXPECT_FALSE(ShouldEvictReplica(config, leader_replica, 3));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (const auto& leader_replica : { "A", "C" }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    AddPeer(&config, "E", N, '+', {{"PROMOTE", true}});
    SCOPED_TRACE(Substitute("leader $0", leader_replica));
    EXPECT_FALSE(ShouldEvictReplica(config, leader_replica, 3));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", V, '+');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("B", to_evict);
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", V, '+');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_TRUE(to_evict == "B" || to_evict == "C");
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '+');
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    // The non-voter replica does not have the PROMOTE attribute, so it the
    // configuration should be considered under-replicated.
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  for (auto replica_status : { '+', '?' }) {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, replica_status, {{"PROMOTE", true}});
    EXPECT_FALSE(ShouldEvictReplica(config, "A", 3));
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+');
    AddPeer(&config, "C", V, '+');
    AddPeer(&config, "D", N, '-');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_EQ("D", to_evict);
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", V, '+');
    AddPeer(&config, "E", V, '+');
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", 3, &to_evict));
    EXPECT_TRUE(to_evict == "B" || to_evict == "C");
    EXPECT_TRUE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", V, '+');
    AddPeer(&config, "E", V, '+');
    AddPeer(&config, "F", V, '+');

    for (const string leader_replica : { "A", "B", "C", "D", "E", "F" }) {
      string to_evict;
      ASSERT_TRUE(ShouldEvictReplica(config, leader_replica, 3, &to_evict));
      EXPECT_TRUE(to_evict == "A" || to_evict == "B" || to_evict == "C");
      if (leader_replica == "A" || leader_replica == "B" || leader_replica == "C") {
        EXPECT_NE(leader_replica, to_evict);
      }
    }
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
  {
    RaftConfigPB config;
    AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
    AddPeer(&config, "D", N, '+', {{"PROMOTE", true}});
    AddPeer(&config, "E", N, '+', {{"PROMOTE", true}});
    AddPeer(&config, "F", N, '+', {{"PROMOTE", true}});

    for (const auto* const leader_replica : { "A", "B", "C" }) {
      // All non-voters are in good shape and not a single one has been
      // promoted yet.
      ASSERT_FALSE(ShouldEvictReplica(config, leader_replica, 3));
    }
    // No more replicas are needed for the replacement.
    EXPECT_FALSE(ShouldAddReplica(config, 3));
  }
}

// A scenario of replica replacement where replicas fall behind the log segment
// GC threshold and are replaced accordingly. This scenario is written to
// address scenarios like of KUDU-2342.
TEST(QuorumUtilTest, NewlyAddedNonVoterFallsBehindLogGC) {
  constexpr auto kReplicationFactor = 3;

  RaftConfigPB config;
  AddPeer(&config, "A", V, '+');
  AddPeer(&config, "B", V, '+');
  AddPeer(&config, "C", V, '+');

  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica B falls behind the log segment GC threshold. Since this is an
  // irreverisble failure, system tries to evict the replica right away.
  SetPeerHealth(&config, "B", 'x');
  string to_evict;
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_EQ("B", to_evict);

  RemovePeer(&config, to_evict);
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // Adding a non-voter to replace B.
  AddPeer(&config, "D", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The new non-voter replica becomes healthy.
  SetPeerHealth(&config, "D", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The new non-voter replica falls behind the log segment GC threshold. The
  // system should evict it before trying to add a replacement replica.
  SetPeerHealth(&config, "D", 'x');
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_EQ("D", to_evict);
  RemovePeer(&config, to_evict);

  // A new non-voter replica is needed.
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // Adding a non-voter to replace D.
  AddPeer(&config, "E", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The new non-voter replica 'E' becomes healthy.
  SetPeerHealth(&config, "E", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The newly added replica gets promoted to voter.
  PromotePeer(&config, "E");
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The new voter replica E falls behind the log segment GC threshold. The
  // replica should be evicted.
  SetPeerHealth(&config, "E", 'x');
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_EQ("E", to_evict);

  RemovePeer(&config, to_evict);
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // The system should add a replacement for the evicted replica.
  AddPeer(&config, "F", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The new non-voter replica 'F' becomes healthy.
  SetPeerHealth(&config, "F", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The newly added replica 'F' gets promoted to voter, all is well now.
  PromotePeer(&config, "F");
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));
}

// A scenario of replica replacement where the replica added for replacement
// of a failed one also fails. The system should end up replacing both failed
// replicas.
TEST(QuorumUtilTest, NewlyPromotedReplicaCrashes) {
  constexpr auto kReplicationFactor = 3;

  RaftConfigPB config;
  AddPeer(&config, "A", V, '+');
  AddPeer(&config, "B", V, '+');
  AddPeer(&config, "C", V, '+');

  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica B fails.
  SetPeerHealth(&config, "B", '-');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // Adding a non-voter to replace B.
  AddPeer(&config, "D", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The new non-voter replica becomes healthy.
  SetPeerHealth(&config, "D", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The newly added non-voter replica is promoted.
  PromotePeer(&config, "D");
  {
    // B would be evicted, if it's reported as is.
    string to_evict;
    ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
    EXPECT_EQ("B", to_evict);
  }
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // However, the newly promoted replica crashes prior to B getting evicted.
  // The system should add a new replica for replacement.

  // We cannot evict because we don't have enough healthy voters to commit
  // the eviction config change.
  SetPeerHealth(&config, "D", '?');
  string to_evict;
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_EQ("B", to_evict);
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  SetPeerHealth(&config, "D", '-');
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_TRUE(to_evict == "B" || to_evict == "D") << to_evict;
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  RemovePeer(&config, to_evict);
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  AddPeer(&config, "E", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  SetPeerHealth(&config, "E", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  PromotePeer(&config, "E");
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_TRUE(to_evict == "B" || to_evict == "D") << to_evict;
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  RemovePeer(&config, to_evict);
  // The processs converges: 3 voter replicas, all are healthy.
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));
}

// A scenario to verify that the catalog manager does not do anything unexpected
// in the 3-4-3 replica management mode when replica's health is flapping
// between HEALTHY and UNKNOWN (e.g., when leader replica changes).
TEST(QuorumUtilTest, ReplicaHealthFlapping) {
  constexpr auto kReplicationFactor = 3;

  // The initial tablet report after the tablet replica A has started and
  // become the leader.
  RaftConfigPB config;
  AddPeer(&config, "A", V, '+');
  AddPeer(&config, "B", V, '?');
  AddPeer(&config, "C", V, '?');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica B is reported as healthy.
  SetPeerHealth(&config, "B", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica C is reported as healthy.
  SetPeerHealth(&config, "C", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica B becomes the new leader.
  SetPeerHealth(&config, "A", '?');
  SetPeerHealth(&config, "B", '+');
  SetPeerHealth(&config, "C", '?');
  EXPECT_FALSE(ShouldEvictReplica(config, "B", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica A is reported as healthy; replica C fails.
  SetPeerHealth(&config, "A", '+');
  SetPeerHealth(&config, "B", '+');
  SetPeerHealth(&config, "C", '-');
  EXPECT_FALSE(ShouldEvictReplica(config, "B", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // A new non-voter replica has been added to replace failed replica C.
  AddPeer(&config, "D", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "B", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica A becomes the new leader.
  SetPeerHealth(&config, "A", '+');
  SetPeerHealth(&config, "B", '?');
  SetPeerHealth(&config, "C", '?');
  SetPeerHealth(&config, "D", '?');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // The new leader has contacted on-line replicas.
  SetPeerHealth(&config, "A", '+');
  SetPeerHealth(&config, "B", '+');
  SetPeerHealth(&config, "C", '?');
  SetPeerHealth(&config, "D", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica D catches up with the leader's WAL and gets promoted.
  PromotePeer(&config, "D");
  string to_evict;
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_EQ("C", to_evict);
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica D becomes the new leader.
  SetPeerHealth(&config, "A", '?');
  SetPeerHealth(&config, "B", '?');
  SetPeerHealth(&config, "C", '?');
  SetPeerHealth(&config, "D", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "D", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  SetPeerHealth(&config, "A", '+');
  SetPeerHealth(&config, "B", '+');
  SetPeerHealth(&config, "C", '?');
  SetPeerHealth(&config, "D", '+');
  ASSERT_TRUE(ShouldEvictReplica(config, "D", kReplicationFactor, &to_evict));
  EXPECT_EQ("C", to_evict);
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  SetPeerHealth(&config, "C", '-');
  ASSERT_TRUE(ShouldEvictReplica(config, "D", kReplicationFactor, &to_evict));
  EXPECT_EQ("C", to_evict);
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  RemovePeer(&config, "C");
  EXPECT_FALSE(ShouldEvictReplica(config, "D", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));
}

// A scenario to simulate the process of migrating all replicas of a tablet,
// where all replicas are marked for replacement simultaneously. This is a
// possible scenario when decommissioning multiple tablet servers/nodes at once.
TEST(QuorumUtilTest, ReplaceAllTabletReplicas) {
  constexpr auto kReplicationFactor = 3;

  // The initial tablet report after the tablet replica 'A' has started and
  // become the leader.
  RaftConfigPB config;
  AddPeer(&config, "A", V, '+', {{"REPLACE", true}});
  AddPeer(&config, "B", V, '+', {{"REPLACE", true}});
  AddPeer(&config, "C", V, '+', {{"REPLACE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // First non-voter replica added.
  AddPeer(&config, "D", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // Second non-voter replica added.
  AddPeer(&config, "E", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // Third non-voter replica added.
  AddPeer(&config, "F", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  SetPeerHealth(&config, "D", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica 'D' catches up with the leader's WAL and gets promoted.
  PromotePeer(&config, "D");
  string to_evict;
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_TRUE(to_evict == "B" || to_evict == "C");
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Evicting the replica selected by ShouldEvictReplica() above.
  RemovePeer(&config, to_evict);
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Non-voter replica 'F' become unavailable.
  SetPeerHealth(&config, "F", '-');
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  ASSERT_EQ("F", to_evict);
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // Evicting the failed non-voter replica, selected by ShouldEvictReplica() above.
  RemovePeer(&config, to_evict);
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_TRUE(ShouldAddReplica(config, kReplicationFactor));

  // Adding a new non-voter replica.
  AddPeer(&config, "G", N, '?', {{"PROMOTE", true}});
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // A newly added non-voter replica is in good shape.
  SetPeerHealth(&config, "G", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica 'E' is reported in good health.
  SetPeerHealth(&config, "E", '+');
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica 'E' catches up with the leader's WAL and gets promoted.
  PromotePeer(&config, "E");
  ASSERT_TRUE(ShouldEvictReplica(config, "A", kReplicationFactor, &to_evict));
  EXPECT_TRUE(to_evict == "B" || to_evict == "C");
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Evicting the replica selected by ShouldEvictReplica() above.
  RemovePeer(&config, to_evict);
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Replica 'G' catches up, but replica 'A' cannot yet be evicted since it's
  // a leader replica.
  PromotePeer(&config, "G");
  EXPECT_FALSE(ShouldEvictReplica(config, "A", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Leadership changes from 'A' to 'G', so now it's possible to evict 'A'.
  ASSERT_TRUE(ShouldEvictReplica(config, "G", kReplicationFactor, &to_evict));
  ASSERT_EQ("A", to_evict);
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));

  // Evicting the replica selected by ShouldEvictReplica() above. With that,
  // the replacement process of all the marked replicas is complete; no further
  // changes is necessary for the tablet's Raft configuration.
  RemovePeer(&config, to_evict);
  EXPECT_FALSE(ShouldEvictReplica(config, "G", kReplicationFactor));
  EXPECT_FALSE(ShouldAddReplica(config, kReplicationFactor));
}

} // namespace consensus
} // namespace kudu
