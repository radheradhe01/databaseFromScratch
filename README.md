# VarmaDB (databaseFromScratch)

VarmaDB is a simple, educational SQL database engine written in Go. It supports basic SQL operations, in-memory and file-backed B+Tree storage, a Write-Ahead Log (WAL) for durability, and a command-line interface (CLI) for interactive use.

---

## Features

- **SQL Support:**  
  - `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`
  - Basic `WHERE`, `ORDER BY`, and `LIMIT` clauses
  - Secondary indexes (`CREATE INDEX`)  
  - Transactions: `BEGIN`, `COMMIT`, `ROLLBACK` (if implemented)

- **Storage:**  
  - In-memory and file-backed B+Tree
  - Write-Ahead Logging (WAL) for crash recovery

- **CLI:**  
  - Interactive shell for running SQL commands

---

## Getting Started

### 1. Prerequisites

- Go 1.18 or newer installed ([Download Go](https://golang.org/dl/))
- macOS, Linux, or Windows

### 2. Clone the Repository

```sh
git clone <your-repo-url>
cd database/databaseFromScratch
```

### 3. Build & Run

You can run the CLI directly:

```sh
go run .
```

Or build a binary:

```sh
go build -o varmadb
./varmadb
```

---

## Usage

When you start VarmaDB, you’ll see:

```
Welcome to the VarmaDB CLI!
Type SQL statements to interact with the database. Type 'exit' or 'quit' to leave.
varmaDB>
```

Type SQL commands at the prompt. For example:

```
varmaDB> CREATE TABLE users (id, name, age)
Table 'users' created.

varmaDB> INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
varmaDB> SELECT * FROM users
id: 1, name: Alice, age: 30
```

---

## Example Commands

### Table Management

```
CREATE TABLE users (id, name, age)
CREATE TABLE products (id, name, price)
```

### Insert Data

```
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)
```

### Query Data

```
SELECT * FROM users
SELECT name, age FROM users
```

### Update & Delete

```
UPDATE users SET age=31 WHERE name='Alice'
DELETE FROM users WHERE id=2
```

### Indexes

```
CREATE INDEX idx_users_name ON users(name)
```

### Transactions

```
BEGIN
INSERT INTO users (id, name, age) VALUES (3, 'Carol', 40)
COMMIT
```

---

## Data Persistence

- Data is stored in memory by default.
- WAL (Write-Ahead Log) is used for durability and crash recovery.
- To use file-backed B+Tree, see the comments in `db.go` for enabling disk storage.

---

## File Structure

- `db.go` — Main database logic, table catalog, B+Tree
- `sql.go` — SQL parsing and statement types
- `record.go` — Record serialization/deserialization
- `pager.go` — File paging and LRU cache
- `wal.go` — Write-Ahead Log implementation
- `wal.log` — WAL file (auto-created)
- `schemas.json` — (Optional) Table schemas

---

## Notes

- Only basic SQL syntax is supported.
- Table and column names are case-insensitive.
- For learning and experimentation; not production-ready.

---

## Troubleshooting

- If you see `(no rows)` after inserting, make sure you did not recreate the table after inserting data.
- Data is not persisted between runs unless using the file-backed B+Tree and WAL.

---

## License

MIT License

---

## Credits

Created by Bhavesh Varma.  
Inspired by educational database projects and B+Tree implementations.