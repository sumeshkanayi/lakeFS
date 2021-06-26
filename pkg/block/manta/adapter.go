package manta

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"regexp"

	"github.com/aws/aws-sdk-go/service/s3"
	triton "github.com/joyent/triton-go/v2"
	"github.com/joyent/triton-go/v2/authentication"
	"github.com/joyent/triton-go/v2/storage"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	BlockstoreType     = "manta"
	defaultMantaRoot   = "/stor"
	mantaUploadIDRegex = "(?m)^([a-f0-9]{8})([a-f0-9]{4})([a-f0-9]{4})([a-f0-9]{4})([a-f0-9]{12})$"
)

type Adapter struct {
	client             *storage.StorageClient
	uploadIDTranslator block.UploadIDTranslator

	path string
}

var (
	ErrPathNotWritable       = errors.New("path provided is not writable")
	ErrInventoryNotSupported = errors.New("inventory feature not implemented for local storage adapter")
	ErrInvalidUploadIDFormat = errors.New("invalid upload id format")
	ErrBadPath               = errors.New("bad path traversal blocked")
)

/*
func WithTranslator(t block.UploadIDTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIDTranslator = t
	}
}
*/

func NewAdapter(sc *storage.StorageClient, opts ...func(a *Adapter)) (*Adapter, error) {

	adapter := &Adapter{
		client:             sc,
		uploadIDTranslator: &block.NoOpTranslator{},
	}
	for _, opt := range opts {
		opt(adapter)
	}
	return adapter, nil
}

func resolveNamespace(obj block.ObjectPointer) (block.QualifiedKey, error) {
	qualifiedKey, err := block.ResolveNamespace(obj.StorageNamespace, obj.Identifier, obj.IdentifierType)
	if err != nil {
		return qualifiedKey, err
	}
	if qualifiedKey.StorageType != block.StorageTypeManta {
		return qualifiedKey, block.ErrInvalidNamespace
	}
	return qualifiedKey, nil
}

// verifyPath ensures that p is under the directory controlled by this adapter.  It does not
// examine the filesystem and can mistakenly error out when symbolic links are involved.
func (l *Adapter) verifyPath(p string) error {
	if !strings.HasPrefix(filepath.Clean(p), l.path) {
		return fmt.Errorf("%s: %w", p, ErrBadPath)
	}
	return nil
}

// maybeMkdir verifies path is allowed and runs f(path), but if f fails due to file-not-found
// MkdirAll's its dir and then runs it again.

func (l *Adapter) Put(ctx context.Context, obj block.ObjectPointer, size int64, reader io.Reader, _ block.PutOpts) error {
	_, err := resolveNamespace(obj)
	if err != nil {
		return err
	}

	metadata := make(map[string]string)

	objectPathFromStorageNameSpace := strings.ReplaceAll(obj.StorageNamespace, "manta://", "")
	objectPathWithBucket := path.Join(defaultMantaRoot, objectPathFromStorageNameSpace)

	err = l.client.Objects().Put(ctx, &storage.PutObjectInput{ForceInsert: true, ObjectReader: reader, ObjectPath: path.Join(objectPathWithBucket, obj.Identifier), ContentType: metadata["content-type"], ContentMD5: metadata["content-md5"], ContentLength: uint64(size)})

	return err
}

func (l *Adapter) Remove(ctx context.Context, obj block.ObjectPointer) error {
	objectPathFromStorageNameSpace := strings.ReplaceAll(obj.StorageNamespace, "manta://", "")

	objectPathWithBucket := path.Join(defaultMantaRoot, objectPathFromStorageNameSpace, obj.Identifier)

	error := l.client.Objects().Delete(ctx, &storage.DeleteObjectInput{ObjectPath: objectPathWithBucket})
	return error

}

func (l *Adapter) Copy(_ context.Context, sourceObj, destinationObj block.ObjectPointer) error {
	return nil
}

func (l *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int64) (string, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}
	r, err := l.Get(ctx, sourceObj, 0)
	if err != nil {
		return "", err
	}
	md5Read := block.NewHashingReader(r, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err = l.Put(ctx, block.ObjectPointer{StorageNamespace: destinationObj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	etag := "\"" + hex.EncodeToString(md5Read.Md5.Sum(nil)) + "\""
	return etag, err
}

func (l *Adapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber, startPosition, endPosition int64) (string, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}
	r, err := l.GetRange(ctx, sourceObj, startPosition, endPosition)
	if err != nil {
		return "", err
	}
	md5Read := block.NewHashingReader(r, block.HashFunctionMD5)
	fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	err = l.Put(ctx, block.ObjectPointer{StorageNamespace: destinationObj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})
	etag := "\"" + hex.EncodeToString(md5Read.Md5.Sum(nil)) + "\""
	return etag, err
}

func (l *Adapter) Get(ctx context.Context, obj block.ObjectPointer, size int64) (reader io.ReadCloser, err error) {
	bucketPathFromStorageNameSpace := strings.ReplaceAll(obj.StorageNamespace, "manta://", "")
	objectPath := path.Join(defaultMantaRoot, bucketPathFromStorageNameSpace, obj.Identifier)
	output, err := l.client.Objects().Get(ctx, &storage.GetObjectInput{ObjectPath: objectPath})
	output.ContentLength = uint64(size)
	return output.ObjectReader, err

}

func (l *Adapter) Walk(_ context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	p := filepath.Clean(path.Join(l.path, walkOpt.StorageNamespace, walkOpt.Prefix))
	return filepath.Walk(p, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		return walkFn(p)
	})
}

func (l *Adapter) Exists(_ context.Context, obj block.ObjectPointer) (bool, error) {
	return true, nil
}

func (l *Adapter) GetRange(_ context.Context, obj block.ObjectPointer, start int64, end int64) (io.ReadCloser, error) {
	fmt.Println("under get range")
	p := "/tmp/test.txt"
	f, err := os.Open(filepath.Clean(p))
	if err != nil {
		return nil, err
	}
	return &struct {
		io.Reader
		io.Closer
	}{
		Reader: io.NewSectionReader(f, start, end-start+1),
		Closer: f,
	}, nil
}

func (l *Adapter) GetProperties(ctx context.Context, obj block.ObjectPointer) (block.Properties, error) {
	fmt.Println("under Get properties", obj.Identifier, obj.StorageNamespace, obj.IdentifierType)
	newPath := strings.ReplaceAll(obj.StorageNamespace, "manta://", "")
	newPath = path.Join(defaultMantaRoot, newPath, obj.Identifier)
	fmt.Println("new path is", newPath)
	_, err := l.client.Objects().GetInfo(ctx, &storage.GetInfoInput{ObjectPath: newPath})
	if err != nil {
		return block.Properties{}, err
	}

	// No properties, just return that it exists
	return block.Properties{}, nil
}

func (l *Adapter) translateMantaUploadIDtoHex(uploadID string) string {
	fmt.Println("translate", strings.ReplaceAll(uploadID, "-", ""))

	return strings.ReplaceAll(uploadID, "-", "")

}

func (l *Adapter) translateUploadIDToMantaFormat(uploadID string) string {

	fmt.Println("to manta", uploadID)

	var outputStringArray []string
	pathMetadata := regexp.MustCompile(mantaUploadIDRegex)
	matches := pathMetadata.FindStringSubmatch(uploadID)

	for index := 1; index < len(matches); index++ {
		outputStringArray = append(outputStringArray, matches[index])

	}
	fmt.Println("to manta", strings.Join(outputStringArray, "-"))
	return strings.Join(outputStringArray, "-")

}

func (l *Adapter) CreateMultiPartUpload(ctx context.Context, obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (string, error) {

	fmt.Println("under create multipart", obj.Identifier, obj.StorageNamespace)
	objectPathFromStorageNameSpace := strings.ReplaceAll(obj.StorageNamespace, "manta://", "")
	objectPathWithBucket := path.Join(defaultMantaRoot, objectPathFromStorageNameSpace, obj.Identifier)

	fmt.Println("PATH IS", objectPathWithBucket)
	mBody := storage.CreateMpuBody{ObjectPath: objectPathWithBucket}
	fmt.Println("mbody is", mBody.ObjectPath)
	cmo, err := l.client.Objects().CreateMultipartUpload(ctx, &storage.CreateMpuInput{Body: mBody, ForceInsert: true})
	if err != nil {
		fmt.Println("error aayedaaaa", err)
		return "", nil
	}
	fmt.Println("cmo id is", cmo.Id)

	fmt.Println("translated id is", l.uploadIDTranslator.SetUploadID(l.translateMantaUploadIDtoHex(cmo.Id)))
	return l.uploadIDTranslator.SetUploadID(l.translateMantaUploadIDtoHex(cmo.Id)), nil
}

func (l *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	fmt.Println("under upload partt")
	fmt.Println("under upload partt", uploadID)

	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}

	md5Read := block.NewHashingReader(reader, block.HashFunctionMD5)

	lp, err := l.client.Objects().UploadPart(ctx, &storage.UploadPartInput{Id: l.translateUploadIDToMantaFormat(uploadID), PartNum: uint64(partNumber), ContentMD5: hex.EncodeToString(md5Read.Md5.Sum(nil))})
	fmt.Println("err is error")
	//fName := uploadID + fmt.Sprintf("-%05d", partNumber)
	//	err := l.Put(ctx, block.ObjectPointer{StorageNamespace: obj.StorageNamespace, Identifier: fName}, -1, md5Read, block.PutOpts{})

	return lp.Part, err
}

func (l *Adapter) AbortMultiPartUpload(_ context.Context, obj block.ObjectPointer, uploadID string) error {

	return nil
}

func (l *Adapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {

	lmop, err := l.client.Objects().ListMultipartUploadParts(ctx, &storage.ListMpuPartsInput{Id: l.translateUploadIDToMantaFormat(uploadID)})
	var parts []string
	var size int64
	for _, es := range lmop.Parts {
		parts = append(parts, es.ETag)
		size = size + es.Size
	}
	fmt.Println("error is", err)
	cob := storage.CommitMpuBody{Parts: parts}
	err = l.client.Objects().CommitMultipartUpload(ctx, &storage.CommitMpuInput{Id: uploadID, Body: cob})

	etag := computeETag(multipartList.Part) + "-" + strconv.Itoa(len(multipartList.Part))
	fmt.Println("error is", err)
	return &etag, size, nil
}

func computeETag(parts []*s3.CompletedPart) string {
	var etagHex []string
	for _, p := range parts {
		e := *p.ETag
		if strings.HasPrefix(e, "\"") && strings.HasSuffix(e, "\"") {
			e = e[1 : len(e)-1]
		}
		etagHex = append(etagHex, e)
	}
	s := strings.Join(etagHex, "")
	b, _ := hex.DecodeString(s)
	md5res := md5.Sum(b) //nolint:gosec
	csm := hex.EncodeToString(md5res[:])
	return csm
}

func (l *Adapter) ValidateConfiguration(_ context.Context, _ string) error {
	return nil
}

func (l *Adapter) GenerateInventory(_ context.Context, _ logging.Logger, _ string, _ bool, _ []string) (block.Inventory, error) {
	return nil, ErrInventoryNotSupported
}

func (l *Adapter) BlockstoreType() string {
	return BlockstoreType
}

func (l *Adapter) GetStorageNamespaceInfo() block.StorageNamespaceInfo {
	return block.DefaultStorageNamespaceInfo(BlockstoreType)
}

func (l *Adapter) RuntimeStats() map[string]string {
	return nil
}

func isValidUploadID(uploadID string) error {
	_, err := hex.DecodeString(uploadID)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidUploadIDFormat, err)
	}
	return nil
}

func NewMantaClient(mantaConfig params.Manta) *storage.StorageClient {

	keyID := mantaConfig.MantaKeyID
	accountName := os.Getenv("TRITON_ACCOUNT")
	keyMaterial := mantaConfig.MantaKeyPath
	userName := mantaConfig.MantaUser

	var signer authentication.Signer
	var err error

	if keyMaterial == "" {
		input := authentication.SSHAgentSignerInput{
			KeyID:       keyID,
			AccountName: accountName,
			Username:    userName,
		}
		signer, err = authentication.NewSSHAgentSigner(input)
		if err != nil {
			log.Fatalf("Error Creating SSH Agent Signer: {{err}}", err)
		}
	} else {
		var keyBytes []byte
		if _, err = os.Stat(keyMaterial); err == nil {
			keyBytes, err = ioutil.ReadFile(keyMaterial)
			if err != nil {
				log.Fatalf("Error reading key material from %s: %s",
					keyMaterial, err)
			}
			block, _ := pem.Decode(keyBytes)
			if block == nil {
				log.Fatalf(
					"Failed to read key material '%s': no key found", keyMaterial)
			}

			if block.Headers["Proc-Type"] == "4,ENCRYPTED" {
				log.Fatalf(
					"Failed to read key '%s': password protected keys are\n"+
						"not currently supported. Please decrypt the key prior to use.", keyMaterial)
			}

		} else {
			keyBytes = []byte(keyMaterial)
		}

		input := authentication.PrivateKeySignerInput{
			KeyID:              keyID,
			PrivateKeyMaterial: keyBytes,
			AccountName:        accountName,
			Username:           userName,
		}
		signer, err = authentication.NewPrivateKeySigner(input)
		if err != nil {
			log.Fatalf("Error Creating SSH Private Key Signer: {{err}}", err)
		}
	}

	config := &triton.ClientConfig{
		MantaURL:    mantaConfig.MantaUrl,
		AccountName: mantaConfig.MantaUser,
		Username:    "",
		Signers:     []authentication.Signer{signer},
	}

	c, err := storage.NewClient(config)
	if err != nil {
		log.Fatalf("compute.NewClient: %s", err)
	}

	return c
}
