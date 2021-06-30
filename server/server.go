package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/dty1er/sdb/engine"
	"github.com/dty1er/sdb/sdb"
)

type SDB interface {
	ExecuteQuery(req *sdb.Parameter) *sdb.Result
	Shutdown() error
}

type Server struct {
	sdb    SDB
	server *http.Server
}

func New(sdb SDB, port int) *Server {
	svr := &Server{sdb: sdb}

	server := &http.Server{}
	server.Addr = fmt.Sprintf(":%d", port)

	svr.server = server

	mux := http.NewServeMux()
	mux.Handle("/execute", svr.sdbHandler())

	server.Handler = mux

	return svr
}

type Request struct {
	Query string // Raw SQL
}

type Response struct {
	Code  string     // "OK" or "NG" for now
	RS    *ResultSet // filled when "OK"
	Error *Error     // filled when "NG"
}

type Error struct {
	Message string
}

type ResultSet struct {
	Message string
	Columns []string        // empty when insert, update, delete
	Values  []*engine.Tuple // empty when insert, update, delete
	Count   int             // empty when insert
}

func (s *Server) sdbHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		respEncoder := json.NewEncoder(w)

		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			resp := Response{
				Code:  "NG",
				Error: &Error{Message: fmt.Sprintf("internal: failed to read request body: %s", err)},
			}
			respEncoder.Encode(&resp)
			return
		}

		resp := s.sdb.ExecuteQuery(&sdb.Parameter{Query: req.Query})
		if resp.Code != "OK" {
			// TODO: define error codes and switch the response based on it
			w.WriteHeader(http.StatusInternalServerError)
			resp := Response{
				Code:  "NG",
				Error: &Error{Message: fmt.Sprintf("internal: failure %s", resp.Error.Message)},
			}
			respEncoder.Encode(&resp)
			return
		}

		vals := []*engine.Tuple{}
		for _, t := range resp.RS.Values {
			tpl := t.(*engine.Tuple)
			vals = append(vals, tpl)
		}

		res := &Response{
			Code: "OK",
			RS: &ResultSet{
				Message: resp.RS.Message,
				Columns: resp.RS.Columns,
				Values:  vals,
				Count:   resp.RS.Count,
			},
		}
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(&res); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			resp := Response{
				Code:  "NG",
				Error: &Error{Message: fmt.Sprintf("internal: failed to write response %s", err)},
			}
			respEncoder.Encode(&resp)
			return
		}
	})
}

func (s *Server) Run() error {
	fmt.Fprintf(os.Stdout, "sdb server started running\n")
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.sdb.Shutdown(); err != nil {
		return nil
	}

	if err := s.server.Shutdown(ctx); err != nil {
		return nil
	}

	return nil
}
