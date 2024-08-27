package client_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alovak/iso8583-client-demo/client"
	"github.com/moov-io/iso8583"
	"github.com/moov-io/iso8583/encoding"
	"github.com/moov-io/iso8583/field"
	"github.com/moov-io/iso8583/padding"
	"github.com/moov-io/iso8583/prefix"
	"github.com/moov-io/iso8583/sort"
	"github.com/stretchr/testify/require"
)

// We use field tags to map the struct fields to the ISO 8583 fields
type AcceptorInformation struct {
	MerchantName         string `iso8583:"01"`
	MerchantCategoryCode string `iso8583:"02"`
	MerchantPostalCode   string `iso8583:"03"`
	MerchantWebsite      string `iso8583:"04"`
}

type AuthorizationRequest struct {
	MTI                 string               `iso8583:"0"`
	PAN                 string               `iso8583:"2"`
	Amount              int64                `iso8583:"3"`
	TransactionDatetime string               `iso8583:"4"`
	Currency            string               `iso8583:"7"`
	CVV                 string               `iso8583:"8"`
	ExpirationDate      string               `iso8583:"9"`
	AcceptorInformation *AcceptorInformation `iso8583:"10"`
	STAN                string               `iso8583:"11"`
}

func TestClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Start the echo server in the background until the test is done
	startEchoServer(ctx)
	defer cancel()

	// Create a new message using the spec
	requestMessage := iso8583.NewMessage(spec)

	// Set the message fields
	err := requestMessage.Marshal(&AuthorizationRequest{
		MTI:                 "0100",
		PAN:                 "4242424242424242",
		Amount:              1000,
		TransactionDatetime: time.Now().UTC().Format("060102150405"),
		Currency:            "840",
		CVV:                 "7890",
		ExpirationDate:      "2512",
		AcceptorInformation: &AcceptorInformation{
			MerchantName:         "Merchant Name",
			MerchantCategoryCode: "1234",
			MerchantPostalCode:   "1234567890",
			MerchantWebsite:      "https://www.merchant.com",
		},
		STAN: "000001",
	})
	require.NoError(t, err)

	// Create a new client
	c := client.New("localhost:8080", spec)

	// Connect to the server
	err = c.Connect()
	require.NoError(t, err)

	// Close the connection when the test is done
	defer c.Close()

	// Send the message to the server and wait for the response
	response, err := c.Send(requestMessage)
	require.NoError(t, err)
	require.NotNil(t, response)

	// let's print the response message to STDOUT
	iso8583.Describe(response, os.Stdout)
}

func BenchmarkClientSend(b *testing.B) {
	// disable logging for the benchmark
	slog.SetLogLoggerLevel(slog.LevelError)

	ctx, cancel := context.WithCancel(context.Background())
	// Start the echo server in the background until the benchmark is done
	startEchoServer(ctx)
	defer cancel()

	// Create a new client
	c := client.New("localhost:8080", spec)

	// Connect to the server
	err := c.Connect()
	require.NoError(b, err)

	// Close the connection when the benchmark is done
	defer c.Close()

	// Run the benchmark in parallel
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Create a new message using the spec
			requestMessage := iso8583.NewMessage(spec)

			// Incrementing STAN value
			stan := getStan()

			// Set the message fields
			err := requestMessage.Marshal(&AuthorizationRequest{
				MTI:                 "0100",
				PAN:                 "4242424242424242",
				Amount:              1000,
				TransactionDatetime: time.Now().UTC().Format("060102150405"),
				Currency:            "840",
				CVV:                 "7890",
				ExpirationDate:      "2512",
				AcceptorInformation: &AcceptorInformation{
					MerchantName:         "Merchant Name",
					MerchantCategoryCode: "1234",
					MerchantPostalCode:   "1234567890",
					MerchantWebsite:      "https://www.merchant.com",
				},
				STAN: stan,
			})
			require.NoError(b, err)

			// Send the message to the server and wait for the response
			response, err := c.Send(requestMessage)
			require.NoError(b, err)
			require.NotNil(b, response)
		}
	})
}

var stan atomic.Uint32

func getStan() string {
	if stan.Load() == 999999 {
		stan.Store(0)
	}
	return fmt.Sprintf("%06d", stan.Add(1))
}

// We use same spec as in the previous article:
// https://github.com/alovak/cardflow-playground/blob/main/examples/message_test.go#L19
var spec *iso8583.MessageSpec = &iso8583.MessageSpec{
	Name: "ISO 8583 CardFlow Playgroud Specification",
	Fields: map[int]field.Field{
		0: field.NewString(&field.Spec{
			Length:      4,
			Description: "Message Type Indicator",
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
		}),
		1: field.NewBitmap(&field.Spec{
			Length:      8,
			Description: "Bitmap",
			Enc:         encoding.Binary,
			Pref:        prefix.Binary.Fixed,
		}),
		2: field.NewString(&field.Spec{
			Length:      19,
			Description: "Primary Account Number (PAN)",
			Enc:         encoding.BCD,
			Pref:        prefix.ASCII.LL,
		}),
		3: field.NewString(&field.Spec{
			Length:      6,
			Description: "Amount",
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
			Pad:         padding.Left('0'),
		}),
		4: field.NewString(&field.Spec{
			Length:      12,
			Description: "Transmission Date & Time", // YYMMDDHHMMSS
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
		}),
		5: field.NewString(&field.Spec{
			Length:      2,
			Description: "Approval Code",
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
		}),
		6: field.NewString(&field.Spec{
			Length:      6,
			Description: "Authorization Code",
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
		}),
		7: field.NewString(&field.Spec{
			Length:      3,
			Description: "Currency",
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
		}),
		8: field.NewString(&field.Spec{
			Length:      4,
			Description: "Card Verification Value (CVV)",
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
		}),
		9: field.NewString(&field.Spec{
			Length:      4,
			Description: "Card Expiration Date",
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
		}),
		10: field.NewComposite(&field.Spec{
			Length:      999,
			Description: "Acceptor Information",
			Pref:        prefix.ASCII.LLL,
			Tag: &field.TagSpec{
				Length: 2,
				Enc:    encoding.ASCII,
				Sort:   sort.StringsByInt,
			},
			Subfields: map[string]field.Field{
				"01": field.NewString(&field.Spec{
					Length:      99,
					Description: "Merchant Name",
					Enc:         encoding.ASCII,
					Pref:        prefix.ASCII.LL,
				}),
				"02": field.NewString(&field.Spec{
					Length:      4,
					Description: "Merchant Category Code (MCC)",
					Enc:         encoding.ASCII,
					Pref:        prefix.ASCII.Fixed,
				}),
				"03": field.NewString(&field.Spec{
					Length:      10,
					Description: "Merchant Postal Code",
					Enc:         encoding.ASCII,
					Pref:        prefix.ASCII.LL,
				}),
				"04": field.NewString(&field.Spec{
					Length:      299,
					Description: "Merchant Website",
					Enc:         encoding.ASCII,
					Pref:        prefix.ASCII.LLL,
				}),
			},
		}),
		11: field.NewString(&field.Spec{
			Length:      6,
			Description: "Systems Trace Audit Number (STAN)",
			Enc:         encoding.BCD,
			Pref:        prefix.BCD.Fixed,
		}),
	},
}
