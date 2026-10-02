// Package lambdahttp serves a standard http.Handler behind API Gateway HTTP
// APIs (payload format 2.0), so the same handler runs locally and on Lambda.
package lambdahttp

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/events"
)

func Handler(h http.Handler) func(context.Context, events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	return func(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
		body := req.Body
		if req.IsBase64Encoded {
			b, err := base64.StdEncoding.DecodeString(body)
			if err != nil {
				return events.APIGatewayV2HTTPResponse{StatusCode: http.StatusBadRequest}, nil
			}
			body = string(b)
		}

		target := req.RawPath
		if target == "" {
			target = "/"
		}
		if req.RawQueryString != "" {
			target += "?" + req.RawQueryString
		}
		r := httptest.NewRequest(req.RequestContext.HTTP.Method, target, strings.NewReader(body))
		r = r.WithContext(ctx)
		for k, v := range req.Headers {
			r.Header.Set(k, v)
		}
		r.RemoteAddr = req.RequestContext.HTTP.SourceIP

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)

		resp := events.APIGatewayV2HTTPResponse{StatusCode: rec.Code, Headers: map[string]string{}}
		for k, v := range rec.Header() {
			resp.Headers[k] = strings.Join(v, ",")
		}
		out := rec.Body.Bytes()
		if utf8.Valid(out) {
			resp.Body = string(out)
		} else {
			resp.Body = base64.StdEncoding.EncodeToString(out)
			resp.IsBase64Encoded = true
		}
		return resp, nil
	}
}
