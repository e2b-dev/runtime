package dberrors

import (
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func IsNotFoundError(err error) bool {
	return errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows)
}

func IsUniqueConstraintViolation(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "23505"
	}

	return false
}

func IsForeignKeyViolation(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "23503"
	}

	return false
}

// IsCheckViolation reports a CHECK constraint rejection, which for a write
// taken straight from a request body means the caller sent something the
// column set does not permit — a client error rather than a server one.
func IsCheckViolation(err error) bool {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code == "23514"
	}

	return false
}

// ConstraintName names the constraint an error violated, or "" when the error
// is not a constraint violation. A statement carrying more than one constraint
// needs the name to say which rule the request actually broke, and those rules
// rarely map to the same response.
func ConstraintName(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.ConstraintName
	}

	return ""
}
