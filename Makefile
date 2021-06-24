cli:
	go run cmd/sdb/*.go

server:
	go run cmd/sdb/*.go server

debugpd:
	go run cmd/sdb/*.go debug -target pd

debugidx:
	go run cmd/sdb/*.go debug -target idx -idxName users_id

debugpg:
	go run cmd/sdb/*.go debug -target pg

debugcatalog:
	go run cmd/sdb/*.go debug -target catalog

clean:
	rm db/*
