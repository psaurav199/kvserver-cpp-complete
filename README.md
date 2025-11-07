# HTTP KV Server — Phase 1 (C++)

Minimal **Phase-1** HTTP key–value server in **C++17** with:
- A very small HTTP server (custom, blocking sockets + thread pool)
- In-memory **LRU cache** (configurable size; default 100)
- **MySQL** persistence (write-through on create/update; delete-through)
- REST-ish endpoints: `POST /kv` (JSON), `GET /kv/{key}`, `DELETE /kv/{key}`
- Basic logging & graceful shutdown (Ctrl+C)

> This is intentionally minimal for an easy Phase-1 demo. It is **not** a production HTTP implementation.

---

## Dependencies

- **CMake** 3.16+
- **C++17** compiler
- **MySQL client library** (`libmysqlclient`)
  - Ubuntu/Debian: `sudo apt-get install libmysqlclient-dev`
- POSIX sockets (Linux/Mac). Tested on Linux.

---

## Build

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
```

This builds the binary: `kvserver_cpp`

---

## Run MySQL (Docker quick start)

```bash
docker run -d --name mysql-kv -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=kv \
  mysql:8

# Create table
docker exec -i mysql-kv mysql -uroot -proot kv <<'SQL'
CREATE TABLE IF NOT EXISTS kv (
  k VARCHAR(255) PRIMARY KEY,
  v MEDIUMBLOB
);
SQL
```

---

## Run the server

Environment variables (with defaults):
- `ADDR` — listen address, default `0.0.0.0`
- `PORT` — listen port, default `8080`
- `CACHE_SIZE` — LRU capacity, default `100`
- `MYSQL_HOST` (default `127.0.0.1`)
- `MYSQL_PORT` (default `3306`)
- `MYSQL_USER` (default `root`)
- `MYSQL_PASS` (default `root`)
- `MYSQL_DB`   (default `kv`)

```bash
# from build/
./kvserver_cpp
# or override:
ADDR=0.0.0.0 PORT=8080 CACHE_SIZE=100 MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 MYSQL_USER=root MYSQL_PASS=root MYSQL_DB=kv ./kvserver_cpp
```

You should see:
```
[server] listening on 0.0.0.0:8080
```

---

## Endpoints

- `POST /kv`
  - Body JSON: `{"key":"K","value":"V"}`
  - Upserts into DB and updates cache
  - Returns `200 OK` on success; `400` on bad input; `500` on DB error

- `GET /kv/{key}`
  - Reads from cache; on miss, fetches from DB and fills cache
  - Returns `200 OK` with `{"value":"..."}`, or `404 Not Found`

- `DELETE /kv/{key}`
  - Deletes from DB first; if ok, evicts from cache
  - Returns `204 No Content` on success, `404` if not found

---

## Smoke Test

```bash
# Create/update
curl -s -X POST localhost:8080/kv \
  -H 'Content-Type: application/json' \
  -d '{"key":"alpha","value":"one"}'

# Read
curl -s localhost:8080/kv/alpha
# -> {"value":"one"}

# Delete
curl -s -X DELETE localhost:8080/kv/alpha -i
# -> HTTP/1.1 204 No Content
```

---

## Notes / Limits

- HTTP parsing is **minimal** (Content-Length required for POST; simple path parsing).
- JSON parsing is naive (extracts `"key"` and `"value"` strings). Keep inputs simple.
- DB queries escape inputs using `mysql_real_escape_string`, but prefer simple ASCII keys/values for Phase-1.
- The server uses a **thread pool** + a blocking accept loop.
- For Phase-2, you can swap this HTTP with a library (cpp-httplib/civetweb) and keep cache/DB code intact.
