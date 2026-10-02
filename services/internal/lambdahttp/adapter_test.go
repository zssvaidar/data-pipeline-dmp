package lambdahttp

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func request(method, path, query, body string, b64 bool) events.APIGatewayV2HTTPRequest {
	req := events.APIGatewayV2HTTPRequest{
		RawPath:         path,
		RawQueryString:  query,
		Body:            body,
		IsBase64Encoded: b64,
		Headers:         map[string]string{"content-type": "application/json"},
	}
	req.RequestContext.HTTP.Method = method
	return req
}

func TestHandlerRoundTrip(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.URL.Path != "/orders" || r.URL.Query().Get("x") != "1" ||
			r.Header.Get("Content-Type") != "application/json" || string(b) != `{"a":1}` {
			t.Errorf("unexpected request %s %s %v %q", r.Method, r.URL, r.Header, b)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"ok":true}`))
	})

	for _, b64 := range []bool{false, true} {
		body := `{"a":1}`
		if b64 {
			body = base64.StdEncoding.EncodeToString([]byte(body))
		}
		resp, err := Handler(h)(context.Background(), request(http.MethodPost, "/orders", "x=1", body, b64))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusCreated || resp.Body != `{"ok":true}` || resp.Headers["Content-Type"] != "application/json" {
			t.Fatalf("unexpected response %+v", resp)
		}
	}
}

func TestHandlerRejectsBadBase64(t *testing.T) {
	h := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("handler must not run") })
	resp, _ := Handler(h)(context.Background(), request(http.MethodPost, "/orders", "", "%%%", true))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}
