1. Add a .env at the root of the infra project with DATABASE_URL=postgres://usr:pwd@localhost:5432/name-postgres changing for the name of your PostgreSQL database and usr/pwd.
2. cargo build
3. cargo run --bin write_objects_from_xlsx
