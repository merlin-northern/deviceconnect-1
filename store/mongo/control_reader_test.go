package mongo

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/mendersoftware/deviceconnect/app"
	"github.com/mendersoftware/deviceconnect/model"
	"github.com/mendersoftware/go-lib-micro/identity"
	mstore "github.com/mendersoftware/go-lib-micro/store"
)

func TestPopControlMessage(t *testing.T) {
	testCases := []struct {
		Name string

		Ctx         context.Context
		SessionID   string
		ControlData string
		Messages    []*app.Control

		Error error
	}{
		{
			Name: "ok",

			Ctx: identity.WithContext(
				context.Background(),
				&identity.Identity{
					Tenant: "000000000000000000000000",
				},
			),
			SessionID: "00000000-0000-0000-0000-000000000000",
			//            0    1   2   3   4
			//echo -n -e '\x02\x20\x00\x08\x00' | gzip - | base64
			ControlData: "H4sIAOmlLWAAA2NSYOBgAABLgQnUBQAAAA==",
			Messages: []*app.Control{
				{
					Type:           app.DelayMessage, // offset0 \x02
					Offset:         32,               // offset 1-2 \x20\x00
					DelayMs:        8,                // offset 3-4 \x08\x00
					TerminalWidth:  0,
					TerminalHeight: 0,
				},
			},
		},
		{
			Name: "ok more than one",

			Ctx: identity.WithContext(
				context.Background(),
				&identity.Identity{
					Tenant: "000000000000000000000000",
				},
			),
			SessionID: "00000000-0000-0000-0000-000000000000",
			//            0    1   2   3   4
			//echo -n -e '\x02\x20\x00\x08\x00\x02\xfa\x3f\x00\x80\x01\x01\x04\x50\x00\x18\x00' | gzip - | base64
			ControlData: "H4sIAJ2rLmAAA2NSYOBgYPplz9DAyMgSwCDBAAADEJylEQAAAA==",
			Messages: []*app.Control{
				{
					Type:           app.DelayMessage, // offset 0 \x02
					Offset:         32,               // offset 1-2 \x20\x00
					DelayMs:        8,                // offset 3-4 \x08\x00
					TerminalWidth:  0,
					TerminalHeight: 0,
				},
				{
					Type:           app.DelayMessage, // offset 0 \x02
					Offset:         16378,            // offset 1-2 \x20\x00
					DelayMs:        32768,            // offset 3-4 \x08\x00
					TerminalWidth:  0,
					TerminalHeight: 0,
				},
				{
					Type:           app.ResizeMessage, // offset 0 \x01
					Offset:         1025,              // offset 1-2 \x20\x00
					DelayMs:        0,                 // offset 3-4 \x08\x00
					TerminalWidth:  80,                // offset 5-6
					TerminalHeight: 24,                // offset 7-8
				},
			},
		},
		{
			Name: "error unknown message type",

			Ctx: identity.WithContext(
				context.Background(),
				&identity.Identity{
					Tenant: "000000000000000000000001",
				},
			),
			SessionID: "00000000-0000-0000-0000-000000000000",
			//            0    1   2   3   4
			//echo -n -e '\x22\x20\x00\x08\x00' | gzip - | base64
			ControlData: "H4sIAHurLWAAA1NSYOBgAABPrsgVBQAAAA==",
		},
		{
			Name: "error buffer does not contain a full message",

			Ctx: identity.WithContext(
				context.Background(),
				&identity.Identity{
					Tenant: "000000000000000000000001",
				},
			),
			SessionID: "00000000-0000-0000-0000-000000000002",
			//            0    1   2   3   4
			//echo -n -e '\x02\x20\x00\x08' | gzip - | base64
			ControlData: "H4sIAOisLWAAA2NSYOAAAEXZ270EAAAA",
		},
		{
			Name: "error buffer too short to contain any message",

			Ctx: identity.WithContext(
				context.Background(),
				&identity.Identity{
					Tenant: "000000000000000000000001",
				},
			),
			SessionID: "00000000-0000-0000-0000-000000000002",
			//            0    1   2   3   4
			//echo -n -e '\x02\x20' | gzip - | base64
			ControlData: "H4sIAHuuLWAAA2NSAAC1UIFIAgAAAA==",
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.Name, func(t *testing.T) {
			db.Wipe()
			ds := &DataStoreMongo{client: db.Client()}
			defer ds.DropDatabase()

			database := db.Client().Database(mstore.DbNameForTenant(
				"000000000000000000000000", DbName,
			))
			collSess := database.Collection(ControlCollectionName)
			d, err := base64.StdEncoding.DecodeString(tc.ControlData)
			assert.NoError(t, err)

			_, err = collSess.InsertOne(nil, &model.ControlData{
				ID:        uuid.New(),
				SessionID: tc.SessionID,
				Control:   d,
				CreatedTs: time.Now().UTC(),
				ExpireTs:  time.Now().UTC(),
			})
			assert.NoError(t, err)

			c, err := collSess.Find(nil, bson.M{
				dbFieldSessionID: tc.SessionID,
			})
			assert.NotNil(t, c)
			assert.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()

			r := NewControlMessageReader(ctx, c)
			assert.NotNil(t, r)

			//TODO: scenario where Pop is used in a loop for r.Pop()!=nil {...}
			if len(tc.Messages) == 0 {
				m := r.Pop()
				assert.Nil(t, m)
			} else {
				actualMessages := make([]*app.Control, len(tc.Messages))
				for i, _ := range tc.Messages {
					actualMessages[i] = r.Pop()
				}
				assert.Equal(t, tc.Messages, actualMessages)
			}
		})
	}
}
