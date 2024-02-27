package fetcher

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/theupdateframework/go-tuf/v2/metadata"
)

const (
	TufFileNameAnnotation = "tuf.io/filename"
)

// RegistryFetcher implements Fetcher
type RegistryFetcher struct {
	httpUserAgent string
	metadataRepo  string
	metadataTag   string
	targetsRepo   string
	cache         *ImageCache
}

type ImageCache struct {
	cache map[string]v1.Image
}

func NewImageCache() *ImageCache {
	return &ImageCache{
		cache: make(map[string]v1.Image),
	}
}

// Get image from cache
func (c *ImageCache) Get(imgRef string) (v1.Image, bool) {
	img, found := c.cache[imgRef]
	return img, found
}

// Add image to cache
func (c *ImageCache) Put(imgRef string, img v1.Image) {
	c.cache[imgRef] = img
}

func NewRegistryFetcher(metadataRepo string, metadataTag string, targetsRepo string) *RegistryFetcher {
	return &RegistryFetcher{
		metadataRepo: metadataRepo,
		metadataTag:  metadataTag,
		targetsRepo:  targetsRepo,
		cache:        NewImageCache(),
	}
}

// DownloadFile downloads a file from an OCI registry, errors out if it failed,
// its length is larger than maxLength or the timeout is reached.
func (d *RegistryFetcher) DownloadFile(urlPath string, maxLength int64, timeout time.Duration) ([]byte, error) {
	imgRef, fileName, err := d.parseUrlPath(urlPath)
	if err != nil {
		return nil, err
	}

	// Check cache for image and pull if not found
	var img v1.Image
	var found bool
	if img, found = d.cache.Get(imgRef); !found {
		// Pull the image from registry
		var err error
		img, err = crane.Pull(imgRef,
			crane.WithUserAgent(d.httpUserAgent),
			crane.WithTransport(transportWithTimeout(timeout)),
			crane.WithAuth(authn.Anonymous),
			crane.WithAuthFromKeychain(authn.DefaultKeychain))
		if err != nil {
			return nil, err
		}
		// Cache the image
		d.cache.Put(imgRef, img)
	}

	// Search image manifest for file
	hash, err := findFileInManifest(img, fileName)
	if err != nil {
		// TODO - refactor Fetcher interface file not found error handling?
		return nil, &metadata.ErrDownloadHTTP{StatusCode: http.StatusNotFound}
	}

	// Get data from image layer
	data, err := getDataFromLayer(img, *hash, maxLength)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// getDataFromLayer returns the data from a layer in an image
func getDataFromLayer(img v1.Image, layerHash v1.Hash, maxLength int64) ([]byte, error) {
	fileLayer, err := img.LayerByDigest(layerHash)
	if err != nil {
		return nil, err
	}
	length, err := fileLayer.Size()
	if err != nil {
		return nil, err
	}
	// Error if the reported size is greater than what is expected.
	if length > maxLength {
		return nil, &metadata.ErrDownloadLengthMismatch{Msg: fmt.Sprintf("download failed, length %d is larger than expected %d", length, maxLength)}
	}
	content, err := fileLayer.Uncompressed()
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(content, maxLength+1))
	if err != nil {
		return nil, err
	}
	// Error if the reported size is greater than what is expected.
	length = int64(len(data))
	if length > maxLength {
		return nil, &metadata.ErrDownloadLengthMismatch{Msg: fmt.Sprintf("download failed, length %d is larger than expected %d", length, maxLength)}
	}
	return data, nil
}

// parseUrlPath maintains the Fetcher interface by parsing a URL path to an image reference and file name
func (d *RegistryFetcher) parseUrlPath(urlPath string) (imgRef string, fileName string, err error) {
	// Check if repo is target or metadata
	if strings.Contains(urlPath, d.targetsRepo) {
		// determine if the target path contains subdirectories and set image name accordingly
		// <repo>/<filename>          -> <repo>:<filename>, layer = <filename>
		// <repo>/<subdir>/<filename> -> <repo>:<subdir>  ,   img = <filename> -> layer = <filename>
		target := strings.TrimPrefix(urlPath, d.targetsRepo+"/")
		subdir, name, found := strings.Cut(target, "/")
		if found {
			fileName = name
			imgRef = fmt.Sprintf("%s:%s", d.targetsRepo, subdir)
		} else {
			fileName = target
			imgRef = fmt.Sprintf("%s:%s", d.targetsRepo, target)
		}
	} else if strings.Contains(urlPath, d.metadataRepo) {
		// build the metadata image name
		fileName = path.Base(urlPath)
		imgRef = fmt.Sprintf("%s:%s", d.metadataRepo, d.metadataTag)
	} else {
		return "", "", fmt.Errorf("urlPath: %s must be in metadata or targets repo", urlPath)
	}
	return imgRef, fileName, nil
}

// findFileInManifest searches the image manifest for a file with the given name and returns its digest
func findFileInManifest(img v1.Image, name string) (*v1.Hash, error) {
	// unmarshal manifest layers with annotations
	mf, err := img.RawManifest()
	if err != nil {
		return nil, err
	}
	type Layer struct {
		Annotations map[string]string `json:"annotations"`
		Digest      string            `json:"digest"`
	}
	type Layers struct {
		Layers []Layer `json:"layers"`
	}
	l := &Layers{}
	err = json.Unmarshal(mf, l)
	if err != nil {
		return nil, err
	}

	// find layer annotation with file name
	var layerDigest string
	for _, layer := range l.Layers {
		if layer.Annotations[TufFileNameAnnotation] == name {
			layerDigest = layer.Digest
			break
		}
	}
	if layerDigest == "" {
		return nil, fmt.Errorf("file %s not found in image", name)
	}

	// return layer digest as v1.Hash
	hash := new(v1.Hash)
	*hash, err = v1.NewHash(layerDigest)
	if err != nil {
		return nil, err
	}
	return hash, nil
}

// transportWithTimeout returns a http.RoundTripper with a specified timeout
func transportWithTimeout(timeout time.Duration) http.RoundTripper {
	// transport is based on go-containerregistry remote.DefaultTransport
	// with modifications to include a specified timeout
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: timeout,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   50,
	}
}
