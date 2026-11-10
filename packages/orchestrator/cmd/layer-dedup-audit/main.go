// layer-dedup-audit is a READ-ONLY (mark-only) tool that finds historical
// cross-scope duplicate template layers in the build-cache bucket.
//
// Context: layer caching is content-addressed (hash = sha256 of layer
// content), but the storage path is prefixed by cacheScope, and the API
// historically hardcoded cacheScope = teamID. So two teams that built the
// same layer each hold a private copy at:
//
//	{teamID}/files/{hash}.tar     <- the layer tarball (the big part)
//	{teamID}/index/{hash}         <- tiny JSON: {"template":{"build_id":...}}
//
// This tool lists the bucket, groups files/{hash}.tar objects by hash across
// every scope prefix, and reports the duplicates + wasted bytes + a candidate
// manifest of redundant copies that could be reclaimed once a shared
// cacheScope is adopted (see TEMPLATE_CACHE_SCOPE / PR #3749).
//
// It DELETES NOTHING. Physical deletion is a separate, operator-gated step
// that consumes the candidate manifest after review — same posture as the
// template-gc mark tool (#3742), which covers the OTHER bucket (memfile/
// rootfs keyed by buildID in the template bucket). The two are complementary:
//
//	#3742 template-gc : TEMPLATE_BUCKET  {buildID}/...   reclaim UNREFERENCED builds by liveness
//	this tool         : BUILD_CACHE_BUCKET {scope}/...   report CROSS-SCOPE DUPLICATE layers by content hash
//
// Neither merges the other's data. See DESIGN.md.
//
// Config (all via env, read at run time by the operator):
//
//	S3_ENDPOINT        e.g. https://tos-s3-cn-beijing.volces.com   (TOS/S3-compatible)
//	S3_REGION          e.g. cn-beijing
//	S3_BUCKET          the build-cache bucket name
//	S3_ACCESS_KEY_ID   / S3_SECRET_ACCESS_KEY
//	S3_USE_PATH_STYLE  "true" for TOS/MinIO
//	OUT_DIR            where to write manifests (default ./out)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type objInfo struct {
	Key          string    `json:"key"`
	Scope        string    `json:"scope"`
	Hash         string    `json:"hash"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

type hashGroup struct {
	Hash         string    `json:"hash"`
	Copies       []objInfo `json:"copies"`
	TotalBytes   int64     `json:"total_bytes"`
	WastedBytes  int64     `json:"wasted_bytes"` // all copies except one
	ScopeCount   int       `json:"scope_count"`
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func must(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintf(os.Stderr, "missing required env %s\n", k)
		os.Exit(2)
	}
	return v
}

func main() {
	ctx := context.Background()

	endpoint := must("S3_ENDPOINT")
	region := env("S3_REGION", "us-east-1")
	bucket := must("S3_BUCKET")
	ak := must("S3_ACCESS_KEY_ID")
	sk := must("S3_SECRET_ACCESS_KEY")
	pathStyle := env("S3_USE_PATH_STYLE", "true") == "true"
	outDir := env("OUT_DIR", "./out")

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir out:", err)
		os.Exit(1)
	}

	cli := s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: pathStyle,
		Credentials:  credentials.NewStaticCredentialsProvider(ak, sk, ""),
	})

	// List the whole bucket, keep only {scope}/files/{hash}.tar objects.
	var files []objInfo
	var totalObjs int
	var p *string
	for {
		out, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: p,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "list:", err)
			os.Exit(1)
		}
		for _, o := range out.Contents {
			totalObjs++
			key := aws.ToString(o.Key)
			scope, hash, ok := parseFilesKey(key)
			if !ok {
				continue
			}
			files = append(files, objInfo{
				Key:          key,
				Scope:        scope,
				Hash:         hash,
				Size:         aws.ToInt64(o.Size),
				LastModified: aws.ToTime(o.LastModified),
			})
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		p = out.NextContinuationToken
	}

	// Group by hash.
	byHash := map[string][]objInfo{}
	for _, f := range files {
		byHash[f.Hash] = append(byHash[f.Hash], f)
	}

	var dups []hashGroup
	var totalLayerBytes, totalWasted int64
	for h, copies := range byHash {
		// distinct scopes holding this hash
		scopes := map[string]bool{}
		var sum int64
		for _, c := range copies {
			scopes[c.Scope] = true
			sum += c.Size
			totalLayerBytes += c.Size
		}
		if len(scopes) <= 1 {
			continue // not a cross-scope duplicate
		}
		// wasted = sum of all but the largest single copy (keep one canonical)
		sizes := make([]int64, 0, len(copies))
		for _, c := range copies {
			sizes = append(sizes, c.Size)
		}
		sort.Slice(sizes, func(i, j int) bool { return sizes[i] > sizes[j] })
		wasted := sum - sizes[0]
		totalWasted += wasted
		sort.Slice(copies, func(i, j int) bool { return copies[i].Scope < copies[j].Scope })
		dups = append(dups, hashGroup{
			Hash: h, Copies: copies, TotalBytes: sum,
			WastedBytes: wasted, ScopeCount: len(scopes),
		})
	}
	sort.Slice(dups, func(i, j int) bool { return dups[i].WastedBytes > dups[j].WastedBytes })

	// Candidate manifest: every copy except one canonical per hash.
	// Canonical = oldest (smallest LastModified) so we keep the original.
	type candidate struct {
		Hash     string `json:"hash"`
		DeleteKey string `json:"delete_key"` // the redundant {scope}/files/{hash}.tar
		IndexKey  string `json:"index_key"`  // paired {scope}/index/{hash}
		Scope     string `json:"scope"`
		Size      int64  `json:"size"`
		KeepScope string `json:"keep_scope"`
	}
	var cands []candidate
	for _, g := range dups {
		keep := g.Copies[0]
		for _, c := range g.Copies {
			if c.LastModified.Before(keep.LastModified) {
				keep = c
			}
		}
		for _, c := range g.Copies {
			if c.Scope == keep.Scope {
				continue
			}
			cands = append(cands, candidate{
				Hash: g.Hash, DeleteKey: c.Key,
				IndexKey: path.Join(c.Scope, "index", c.Hash),
				Scope:    c.Scope, Size: c.Size, KeepScope: keep.Scope,
			})
		}
	}

	writeJSON(path.Join(outDir, "duplicates.json"), dups)
	writeJSON(path.Join(outDir, "candidates.json"), cands)

	fmt.Printf("bucket=%s objects_scanned=%d layer_tars=%d\n", bucket, totalObjs, len(files))
	fmt.Printf("total_layer_bytes=%s\n", human(totalLayerBytes))
	fmt.Printf("cross_scope_duplicate_hashes=%d\n", len(dups))
	fmt.Printf("reclaimable_wasted_bytes=%s (keep 1 copy per hash)\n", human(totalWasted))
	fmt.Printf("delete_candidates=%d  ->  %s/candidates.json (MARK ONLY, nothing deleted)\n", len(cands), outDir)
	if len(dups) > 0 {
		fmt.Println("top wasters:")
		for i, g := range dups {
			if i >= 10 {
				break
			}
			fmt.Printf("  %s  %d scopes  waste=%s\n", g.Hash[:min(16, len(g.Hash))], g.ScopeCount, human(g.WastedBytes))
		}
	}
}

// parseFilesKey matches "{scope}/files/{hash}.tar" and returns scope, hash.
func parseFilesKey(key string) (scope, hash string, ok bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 3 || parts[1] != "files" || !strings.HasSuffix(parts[2], ".tar") {
		return "", "", false
	}
	return parts[0], strings.TrimSuffix(parts[2], ".tar"), true
}

func writeJSON(p string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write", p, err)
	}
}

func human(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.2f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
