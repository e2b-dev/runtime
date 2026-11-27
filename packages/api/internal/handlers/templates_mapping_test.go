package handlers

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
)

// TemplateWithBuilds is a Template with builds. Every Template field, filled
// with a non-zero value, must come through templateWithBuilds unchanged, and
// TemplateWithBuilds must have no field Template lacks other than Builds.
func TestTemplateWithBuildsCopiesEveryTemplateField(t *testing.T) {
	t.Parallel()

	var template api.Template
	fillNonZero(reflect.ValueOf(&template).Elem())
	builds := []api.TemplateBuild{{CpuCount: 2}}

	got := reflect.ValueOf(templateWithBuilds(template, builds))
	want := reflect.ValueOf(template)

	for i := range want.NumField() {
		name := want.Type().Field(i).Name
		field := got.FieldByName(name)
		require.True(t, field.IsValid(), "TemplateWithBuilds has no %s", name)
		assert.Equal(t, want.Field(i).Interface(), field.Interface(), "%s is not copied", name)
	}

	for i := range got.NumField() {
		name := got.Type().Field(i).Name
		if name == "Builds" {
			continue
		}
		_, ok := want.Type().FieldByName(name)
		assert.True(t, ok, "TemplateWithBuilds has %s, which Template lacks", name)
	}
	assert.Equal(t, builds, got.FieldByName("Builds").Interface())
}

// The builds list reads a subset of a build's columns. Every field of its
// response must come out of that row: a field added to the build mapping
// without its column would otherwise be silently zero on this route.
func TestTemplateBuildRowFillsEveryAPIField(t *testing.T) {
	t.Parallel()

	var row queries.ListTemplateBuildsRow
	fillNonZero(reflect.ValueOf(&row).Elem())

	got := reflect.ValueOf(templateBuildRowToAPI(t.Context(), row))
	for i := range got.NumField() {
		assert.False(t, got.Field(i).IsZero(), "%s is not filled from the row", got.Type().Field(i).Name)
	}
}

// Every field of the template info, and of its default build, comes out of
// the info row.
func TestTemplateInfoRowFillsEveryAPIField(t *testing.T) {
	t.Parallel()

	var row queries.GetTeamTemplateInfoRow
	fillNonZero(reflect.ValueOf(&row).Elem())

	got := templateInfoToAPI(t.Context(), row)
	require.NotNil(t, got.DefaultBuild)

	for _, v := range []reflect.Value{reflect.ValueOf(got), reflect.ValueOf(*got.DefaultBuild)} {
		for i := range v.NumField() {
			assert.False(t, v.Field(i).IsZero(), "%s.%s is not filled from the row", v.Type().Name(), v.Type().Field(i).Name)
		}
	}
}

// fillNonZero sets every field reachable from v to a non-zero value.
func fillNonZero(v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint8:
		v.SetUint(1)
	case reflect.Array:
		fillNonZero(v.Index(0))
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillNonZero(v.Index(0))
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fillNonZero(v.Elem())
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			v.Set(reflect.ValueOf(time.Unix(1, 0).UTC()))

			return
		}
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				fillNonZero(v.Field(i))
			}
		}
	default:
		panic("fillNonZero: unhandled kind " + v.Kind().String())
	}
}

// A template reports the memory a create from it starts with: boot RAM for an
// ordinary template, the size it was taken at for a snapshot template of a grown
// sandbox. Every template mapper uses the same rule.
func TestTemplateMemoryIsTheCreateDefault(t *testing.T) {
	t.Parallel()

	maxMB, grown, boot := int64(4096), int64(3072), int64(1024)
	for _, tt := range []struct {
		name       string
		max, start *int64
		want       api.MemoryMB
	}{
		{name: "fixed template", want: 1024},
		{name: "template with a maximum", max: &maxMB, start: &boot, want: 1024},
		{name: "snapshot template of a grown sandbox", max: &maxMB, start: &grown, want: 3072},
		{name: "build without a recorded size", max: &maxMB, want: 1024},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, templateMemoryMB(1024, tt.max, tt.start))

			row := templateRow{BuildRamMb: 1024, BuildMaxRamMb: tt.max, BuildStartMemoryMb: tt.start}
			assert.Equal(t, tt.want, templateToAPI(t.Context(), row, false).MemoryMB)

			build := queries.EnvBuild{RamMb: 1024, MaxRamMb: tt.max, StartMemoryMb: tt.start}
			assert.Equal(t, tt.want, templateBuildToAPI(t.Context(), build).MemoryMB)

			buildRow := queries.ListTemplateBuildsRow{RamMb: 1024, MaxRamMb: tt.max, StartMemoryMb: tt.start}
			assert.Equal(t, tt.want, templateBuildRowToAPI(t.Context(), buildRow).MemoryMB)

			ram, buildID := int64(1024), uuid.New()
			info := queries.GetTeamTemplateInfoRow{DefaultBuildID: &buildID, DefaultBuildRamMb: &ram, DefaultBuildMaxRamMb: tt.max, DefaultBuildStartMemoryMb: tt.start}
			defaultBuild := templateInfoToAPI(t.Context(), info).DefaultBuild
			require.NotNil(t, defaultBuild)
			assert.Equal(t, tt.want, defaultBuild.MemoryMB)
		})
	}
}
