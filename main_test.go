package main //nolint:testpackage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-clustersync-mongodb/ha"
)

func TestActiveMemberAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		members []groupMember
		want    string
	}{
		{
			name: "returns active host:port",
			members: []groupMember{
				{InstanceID: "a", Host: "host-a", Port: 2242, Role: ha.RoleStandby},
				{InstanceID: "b", Host: "host-b", Port: 2243, Role: ha.RoleActive},
			},
			want: "host-b:2243",
		},
		{
			name: "empty when no active member",
			members: []groupMember{
				{InstanceID: "a", Host: "host-a", Port: 2242, Role: ha.RoleStandby},
			},
			want: "",
		},
		{
			name:    "empty when no members",
			members: nil,
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := &ResponseEnvelope{Group: groupInfo{Members: tt.members}}
			assert.Equal(t, tt.want, activeMemberAddr(env))
		})
	}
}

func TestActiveMemberAddrNilEnvelope(t *testing.T) {
	t.Parallel()

	// A single-instance deployment has no envelope; the helper must be nil-safe.
	assert.Empty(t, activeMemberAddr(nil))
}

func TestEnvelopeJSONShape(t *testing.T) {
	t.Parallel()

	env := &ResponseEnvelope{
		Me:   meInfo{InstanceID: "pcsm-xyz"},
		Role: ha.RoleStandby,
		Group: groupInfo{
			Name: "default",
			Term: 7,
			Members: []groupMember{
				{InstanceID: "pcsm-abc", Host: "host-2", Port: 2242, Role: ha.RoleActive},
				{InstanceID: "pcsm-xyz", Host: "host-1", Port: 2242, Role: ha.RoleStandby},
			},
		},
	}

	data, err := json.Marshal(startResponse{ResponseEnvelope: env, Ok: true})
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))

	// Top-level me/role/group present alongside the endpoint payload.
	assert.Contains(t, decoded, "me")
	assert.Contains(t, decoded, "role")
	assert.Contains(t, decoded, "group")
	assert.Equal(t, true, decoded["ok"])
	assert.Equal(t, "STANDBY", decoded["role"])

	me, ok := decoded["me"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "pcsm-xyz", me["instanceId"])

	group, ok := decoded["group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "default", group["name"])
	assert.InDelta(t, float64(7), group["term"], 0)

	members, ok := group["members"].([]any)
	require.True(t, ok)
	assert.Len(t, members, 2)
}

func TestEnvelopeOmittedForSingleNode(t *testing.T) {
	t.Parallel()

	// A single-instance deployment has a nil envelope: the response must carry
	// no me/role/group fields, i.e. be byte-identical to the pre-HA API.
	data, err := json.Marshal(startResponse{Ok: true})
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.NotContains(t, decoded, "me")
	assert.NotContains(t, decoded, "role")
	assert.NotContains(t, decoded, "group")
	assert.Equal(t, true, decoded["ok"])
}

// TestResponseEnvelopeRoundTrip guards the CLI decode path: a response carrying
// the envelope must unmarshal back into the response struct. encoding/json
// cannot decode into an embedded pointer to an unexported struct, so the
// envelope type must stay exported. Marshaling alone (server side) never
// exercises this; only decoding (client side) does.
func TestResponseEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()

	orig := statusResponse{
		ResponseEnvelope: &ResponseEnvelope{
			Me:   meInfo{InstanceID: "pcsm-xyz"},
			Role: ha.RoleStandby,
			Group: groupInfo{
				Name: "default",
				Term: 3,
				Members: []groupMember{
					{InstanceID: "pcsm-abc", Host: "host-2", Port: 2242, Role: ha.RoleActive},
				},
			},
		},
		Ok:    true,
		State: "running",
	}

	data, err := json.Marshal(orig)
	require.NoError(t, err)

	// This is exactly what the CLI client does; it errored before the envelope
	// type was exported.
	var decoded statusResponse
	require.NoError(t, json.Unmarshal(data, &decoded))

	require.NotNil(t, decoded.ResponseEnvelope)
	assert.Equal(t, "pcsm-xyz", decoded.Me.InstanceID)
	assert.Equal(t, ha.RoleStandby, decoded.Role)
	assert.Equal(t, "default", decoded.Group.Name)
	assert.Len(t, decoded.Group.Members, 1)
	assert.True(t, decoded.Ok)
}

func TestNotActiveResponseJSONShape(t *testing.T) {
	t.Parallel()

	na := notActiveResponse{
		ResponseEnvelope: &ResponseEnvelope{
			Me:   meInfo{InstanceID: "pcsm-xyz"},
			Role: ha.RoleStandby,
			Group: groupInfo{
				Members: []groupMember{
					{InstanceID: "pcsm-abc", Host: "host-2", Port: 2242, Role: ha.RoleActive},
				},
			},
		},
		Ok:      false,
		Err:     "not_active",
		Message: "This instance is STANDBY. Active is running on host-2:2242.",
	}

	data, err := json.Marshal(na)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, "not_active", decoded["error"])
	assert.Equal(t, false, decoded["ok"])
	assert.Contains(t, decoded["message"], "host-2:2242")
	assert.Contains(t, decoded, "group")
}
