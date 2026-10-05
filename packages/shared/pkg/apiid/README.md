# Resource IDs at HTTP boundaries

`apiid.ProjectID` and `apiid.WorkspaceID` accept a legacy UUID or the matching
public ID. Both inputs produce the same struct:

```go
projectID.UUID     // uuid.UUID
projectID.PublicID // canonical prj_ string
```

The existing `pkg/id` codecs remain unchanged. These structs are HTTP input
values; database and domain functions can continue taking `uuid.UUID`.

## Adding or changing an API ID

Update [`id`](../id/README.md#adding-or-changing-a-resource-id) and this package
in the same PR whenever adding a resource kind to either package. Follow the
linked checklist: `id` owns the codec, and `apiid` provides the matching input
struct and binding support. Do not add an API-only encoding or a new codec
without its API type. Keep existing kinds' encoding and validation changes,
tests, and documentation synchronized across both packages.

## OpenAPI and generated Gin handlers

Use the standard oapi-codegen type override on a string schema:

```yaml
projectID:
  name: projectID
  in: path
  required: true
  schema:
    type: string
    description: Project UUID or prj_ public ID.
    x-go-type: apiid.ProjectID
    x-go-type-import:
      name: apiid
      path: github.com/e2b-dev/infra/packages/shared/pkg/apiid
```

Regenerate the server and dependent clients using the API's generation command.
No middleware, registry, or generator fork is needed:

```go
func (s *Server) GetProject(c *gin.Context, projectID api.ProjectID) {
    project, err := s.projects.Get(c.Request.Context(), projectID.UUID)
    // Handle err and build the response as usual.
}
```

Use `apiid.WorkspaceID` for workspace parameters. Apply the same schema override
to query or header parameters and to JSON body properties or array items:

```yaml
workspace_id:
  type: string
  description: Workspace UUID or wrk_ public ID.
  x-go-type: apiid.WorkspaceID
  x-go-type-import:
    name: apiid
    path: github.com/e2b-dev/infra/packages/shared/pkg/apiid
```

After `c.ShouldBindJSON(&body)`, use `body.WorkspaceId.UUID` and
`body.WorkspaceId.PublicID`. Keep response UUID fields unchanged unless a
response-contract change is intended. Do not keep `format: uuid` on inputs
that also accept public IDs.

## Handwritten Gin binding

```go
var params struct {
    ProjectID apiid.ProjectID `uri:"projectID" binding:"required"`
}
if err := c.ShouldBindUri(&params); err != nil {
    c.AbortWithStatus(http.StatusBadRequest)
    return
}
projectUUID := params.ProjectID.UUID
```

`ShouldBindQuery` and `ShouldBindHeader` work with `form` and `header` tags.
`UnmarshalText` supports generated path binding; `Bind` supports oapi-codegen's
exploded query binding; `UnmarshalParam` supports Gin's own binders. Tests cover
each path because these binders do not all use the same interface.

## Construction and encoding

Use `apiid.NewProjectID(u)` or `apiid.NewWorkspaceID(u)` when constructing an ID
in Go. Decoders and constructors populate both fields. Malformed input and
wrong-kind public IDs fail without modifying the receiver. JSON accepts only
strings; encoding emits a public ID string, not a JSON object.

The fields are exported for direct reads. Treat decoded values as immutable;
Go cannot stop a caller from manually making them inconsistent. Encoding derives
the public string from `UUID`, rather than trusting a manually edited `PublicID`.
A zero-valued struct denotes an unset input; requiredness belongs in the request
schema or Gin validation. Use a pointer for optional inputs when absence matters.

Other APIs adopt the types by updating their schemas and regeneration, not by
installing middleware. This package does not change their contracts automatically.
