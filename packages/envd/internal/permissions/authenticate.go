package permissions

import (
	"context"
	"errors"
	"net/http"
	"os/user"

	"connectrpc.com/authn"
	"connectrpc.com/connect"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
)

func AuthenticateUsername(_ context.Context, req *http.Request) (any, error) {
	username, _, ok := req.BasicAuth()
	if !ok {
		// When no username is provided, ignore the authentication method (not all endpoints require it)
		// Missing user is then handled in the GetAuthUser function
		return nil, nil
	}

	u, err := GetUser(username)
	if err != nil {
		return nil, authn.Errorf("invalid username: '%s'", username)
	}

	return u, nil
}

func GetAuthUser(ctx context.Context, defaultUser string) (*user.User, error) {
	u, ok := authn.GetInfo(ctx).(*user.User)
	if !ok {
		username, err := execcontext.ResolveDefaultUsername(nil, defaultUser)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no user specified"))
		}

		u, err := GetUser(username)
		if err != nil {
			return nil, authn.Errorf("invalid default user: '%s'", username)
		}

		return u, nil
	}

	return u, nil
}
