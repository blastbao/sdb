package sdb

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (sdb *SDB) mainHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		respEncoder := json.NewEncoder(w)

		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			resp := Response{
				Result: "NG",
				Error:  &Error{Message: fmt.Sprintf("internal: failed to read request body: %s", err)},
			}
			respEncoder.Encode(&resp)
			return
		}

		resp := sdb.ExecQuery(&req)
		if resp.Result != "OK" {
			w.WriteHeader(http.StatusInternalServerError)
			resp := Response{
				Result: "NG",
				Error:  &Error{Message: fmt.Sprintf("internal: failure %s", resp.Error.Message)},
			}
			respEncoder.Encode(&resp)
			return
		}

		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(&resp); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			resp := Response{
				Result: "NG",
				Error:  &Error{Message: fmt.Sprintf("internal: failed to write response %s", err)},
			}
			respEncoder.Encode(&resp)
			return
		}
	})
}
