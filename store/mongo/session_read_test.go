package mongo

import (
	"context"
	"encoding/base64"
	"github.com/mendersoftware/deviceconnect/app"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	mopts "go.mongodb.org/mongo-driver/mongo/options"

	"github.com/mendersoftware/deviceconnect/model"
	"github.com/mendersoftware/go-lib-micro/identity"
	mstore "github.com/mendersoftware/go-lib-micro/store"
)

func TestReadRecordingWithControlData(t *testing.T) {
	testCases := []struct {
		Name string

		Ctx             context.Context
		SessionID       string
		RecordingData   []string
		ControlData     []string
		ReadBufferSize  int
		ExpectedString  string
		ControlMessages []*app.Control

		Error error
	}{
		{
			Name: "ok",

			Ctx: identity.WithContext(
				context.Background(),
				&identity.Identity{
					Tenant: "000000000000000000000002",
				},
			),
			SessionID: "00000000-0000-0000-0000-000000000002",
			RecordingData: []string{
				"H4sIAKOjMmAAA1NOTc7IV8jP5gIiAIEuTnMMAAAA",
				"H4sIAFtvMmAAA1WPwWrDMAyGz8pTCHaeB4PtPlqPlTU7LOwcPFvNTG0r2G6gbz85hNH5IiOk7/90Fwo6WrwlyymRrQ/dnu2Z8skHAsDenKl9EfHbJ2n8GwZsb2IV2SnOfkKMxqexUqlqYmi13ACVsZbmapIlwE/9su+1ig7RBk9JaG6dvLccZy6krjGs9HKJkiQZFAAu1YfSHQ87/TFoECMz+9VDnE5+WiEF/sxkdzMrlBfKsFBynBtgfD0c9TDu3t6Hr16VH/P49IyCm29w6mo2CYnfqO3Edp0gK2fqfgGMUrrGRAEAAA==",
			},
			ControlData: []string{
				"H4sIAJ2rLmAAA2NSYOBgYPplz9DAyMgSwCDBAAADEJylEQAAAA==",
			},
			ControlMessages: []*app.Control{
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
			ReadBufferSize: 512,
			ExpectedString: "#echo ok\nok\n#ls deviceconnect/\nDockerfile\t\t Makefile   bin\t\t deviceconnect\t     go.mod.orig  main_test.go\ttests\nDockerfile.acceptance\t README.md  client\t docker-compose.yml  go.sum\t  model\t\tutils\nLICENSE\t\t\t api\t    config\t docs\t\t     go.sum.orig  server\tvendor\nLIC_FILES_CHKSUM.sha256  app\t    config.yaml  go.mod\t\t     main.go\t  store\n",
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.Name, func(t *testing.T) {
			ds := &DataStoreMongo{client: db.Client()}
			defer ds.DropDatabase()

			database := db.Client().Database(mstore.DbNameForTenant(
				"000000000000000000000000", DbName,
			))
			collSess := database.Collection(RecordingsCollectionName)

			for i, _ := range tc.RecordingData {
				d, err := base64.StdEncoding.DecodeString(tc.RecordingData[i])
				assert.NoError(t, err)

				_, err = collSess.InsertOne(nil, &model.Recording{
					ID:        uuid.New(),
					SessionID: tc.SessionID,
					Recording: d,
					CreatedTs: time.Now().UTC(),
					ExpireTs:  time.Now().UTC(),
				})
				assert.NoError(t, err)
			}
			findOptions := mopts.Find()
			sortField := bson.M{
				"created_ts": 1,
			}
			findOptions.SetSort(sortField)
			c, err := collSess.Find(nil,
				bson.M{
					dbFieldSessionID: tc.SessionID,
				},
				findOptions,
			)
			assert.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
			defer cancel()
			reader := NewRecordingReader(ctx, c)
			assert.NotNil(t, reader)
			buffer := make([]byte, tc.ReadBufferSize)
			s := ""
			for err != io.EOF {
				assert.NoError(t, err)
				n, err := reader.Read(buffer[:])
				if err == io.EOF {
					break
				}
				s += string(buffer[:n])
				//t.Logf("read: %s/%d", string(buffer[:n]), n)
			}
			//t.Logf("read(final): %s/%d", s, len(s))
			assert.Equal(t, tc.ExpectedString, s)
		})
	}
}
