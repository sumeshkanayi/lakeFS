package manta

import (
	"context" //nolint:gosec
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
	"strings"
	"time"

	"regexp"

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
	partSuffix         = ".part_"
	marker             = " "
)

type Adapter struct {
	client             *storage.StorageClient
	uploadIDTranslator block.UploadIDTranslator
	accountName        string
}

var (
	ErrInventoryNotSupported = errors.New("inventory feature not implemented for mantastorage adapter")
	ErrInvalidUploadIDFormat = errors.New("invalid upload id format")
	ErrFinalizedMultiPart    = errors.New("cannot upload to a finalized multipart")
)

/*
func WithTranslator(t block.UploadIDTranslator) func(a *Adapter) {
	return func(a *Adapter) {
		a.uploadIDTranslator = t
	}
}
*/

type MultiPartIDTranslator struct {
}

func (m MultiPartIDTranslator) SetUploadID(uploadID string) string {

	return strings.ReplaceAll(uploadID, "-", "")

}
func (m MultiPartIDTranslator) TranslateUploadID(simulationID string) string {

	var outputStringArray []string
	pathMetadata := regexp.MustCompile(mantaUploadIDRegex)
	matches := pathMetadata.FindStringSubmatch(simulationID)

	for index := 1; index < len(matches); index++ {
		outputStringArray = append(outputStringArray, matches[index])

	}
	return strings.Join(outputStringArray, "-")
}
func (m MultiPartIDTranslator) RemoveUploadID(inputUploadID string) {

}

func NewAdapter(sc *storage.StorageClient, accountName string, opts ...func(a *Adapter)) (*Adapter, error) {

	adapter := &Adapter{
		client:             sc,
		uploadIDTranslator: MultiPartIDTranslator{},
		accountName:        accountName,
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

func (l *Adapter) Put(ctx context.Context, obj block.ObjectPointer, size int64, reader io.Reader, _ block.PutOpts) error {

	var err error

	defer reportMetrics("Put", time.Now(), &size, &err)

	_, err = resolveNamespace(obj)
	if err != nil {

		return err
	}

	metadata := make(map[string]string)

	objectPathFromStorageNameSpace := strings.ReplaceAll(obj.StorageNamespace, "manta://", "")
	objectPathWithBucket := path.Join(defaultMantaRoot, objectPathFromStorageNameSpace)

	return l.client.Objects().Put(ctx, &storage.PutObjectInput{ForceInsert: true, ObjectReader: reader, ObjectPath: path.Join(objectPathWithBucket, obj.Identifier), ContentType: metadata["content-type"], ContentMD5: metadata["content-md5"], ContentLength: uint64(size)})

}

func (l *Adapter) Remove(ctx context.Context, obj block.ObjectPointer) error {

	return l.client.Objects().Delete(ctx, &storage.DeleteObjectInput{ObjectPath: l.translateToMantaPath(obj)})

}

func (l *Adapter) Copy(ctx context.Context, sourceObj, destinationObj block.ObjectPointer) error {

	return l.client.SnapLinks().Put(ctx, &storage.PutSnapLinkInput{LinkPath: l.translateToMantaPath(destinationObj), SourcePath: l.translateToMantaPath(sourceObj)})

}

func (l *Adapter) UploadCopyPart(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber int64) (string, error) {
	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}
	r, err := l.Get(ctx, sourceObj, 0)
	if err != nil {
		return "", err
	}
	etag, err := l.UploadPart(ctx, destinationObj, 0, r, uploadID, partNumber)

	return etag, err
}

func (l *Adapter) UploadCopyPartRange(ctx context.Context, sourceObj, destinationObj block.ObjectPointer, uploadID string, partNumber, startPosition, endPosition int64) (string, error) {
	var err error
	defer reportMetrics("UploadCopyPartRange", time.Now(), nil, &err)
	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}
	r, err := l.GetRange(ctx, sourceObj, startPosition, endPosition)
	if err != nil {
		return "", err
	}
	etag, err := l.UploadPart(ctx, destinationObj, 0, r, uploadID, partNumber)

	if err != nil {
		return "", err

	}
	return etag, err
}

func (l *Adapter) Get(ctx context.Context, obj block.ObjectPointer, size int64) (reader io.ReadCloser, err error) {
	defer reportMetrics("Get", time.Now(), &size, &err)
	output, err := l.client.Objects().Get(ctx, &storage.GetObjectInput{ObjectPath: l.translateToMantaPath(obj)})
	//output.ContentLength = uint64(size)
	return output.ObjectReader, err

}

func (l *Adapter) Walk(ctx context.Context, walkOpt block.WalkOpts, walkFn block.WalkFunc) error {
	var err error
	log := l.log(ctx).WithField("operation", "Walk")
	pathBase := path.Base(walkOpt.Prefix)
	defer reportMetrics("Walk", time.Now(), nil, &err)
	bucketPath := strings.ReplaceAll(walkOpt.StorageNamespace, "manta://", "")
	var dirName string
	if pathDir := path.Dir(walkOpt.Prefix); pathDir == "." {

		dirName = path.Join(defaultMantaRoot, bucketPath)
	} else {
		dirName = path.Join(defaultMantaRoot, bucketPath, pathDir)
	}

	input := &storage.ListDirectoryInput{
		DirectoryName: dirName,
	}
	objs, err := l.client.Dir().List(ctx, input)
	if err != nil {
		log.WithError(err).
			WithField("input", input.DirectoryName).
			Error(err)
		return err
	}

	for _, obj := range objs.Entries {

		// If the base name of our prefix was found to be of type "directory"
		// than we need to pull the directory entries for that instead.

		if err := walkFn(obj.Name); err != nil {
			return err
		}

		if obj.Name == pathBase && obj.Type == "directory" {
			input.DirectoryName = path.Join(defaultMantaRoot, bucketPath, walkOpt.Prefix)
			objs, err = l.client.Dir().List(ctx, input)
			if err != nil {
				log.WithError(err).
					WithField("input", input.DirectoryName).
					Error(err)
				return err
			}

		}
	}

	return nil

}

func (l *Adapter) Exists(ctx context.Context, obj block.ObjectPointer) (bool, error) {
	var err error
	defer reportMetrics("Exists", time.Now(), nil, &err)
	fmt.Println("EXISTS.....")

	_, err = l.client.Objects().Get(ctx, &storage.GetObjectInput{ObjectPath: l.translateToMantaPath(obj)})
	if err != nil {
		return false, err
	}

	return true, err
}

func (l *Adapter) GetRange(ctx context.Context, obj block.ObjectPointer, start int64, end int64) (io.ReadCloser, error) {
	var err error
	defer reportMetrics("GetRange", time.Now(), nil, &err)
	output, err := l.client.Objects().Get(ctx, &storage.GetObjectInput{
		ObjectPath: l.translateToMantaPath(obj),
	})
	if err != nil {
		return nil, err
	}
	_, err = io.CopyN(io.Discard, output.ObjectReader, start)
	if err != nil {
		return nil, err
	}

	return &struct {
		io.Reader
		io.Closer
	}{
		Reader: io.LimitReader(output.ObjectReader, end-start+1),
		Closer: output.ObjectReader,
	}, err

}

func (l *Adapter) GetProperties(ctx context.Context, obj block.ObjectPointer) (block.Properties, error) {

	var err error
	defer reportMetrics("GetProperties", time.Now(), nil, &err)
	fmt.Println("GET PROPERTIES CALLED...")

	_, err = l.client.Objects().GetInfo(ctx, &storage.GetInfoInput{ObjectPath: l.translateToMantaPath(obj)})
	if err != nil {
		return block.Properties{}, err
	}

	// No properties, just return that it exists
	return block.Properties{}, nil
}

func (l *Adapter) CreateMultiPartUpload(ctx context.Context, obj block.ObjectPointer, _ *http.Request, _ block.CreateMultiPartUploadOpts) (string, error) {

	mBody := storage.CreateMpuBody{ObjectPath: l.translateToMantaPath(obj)}
	cmo, err := l.client.Objects().CreateMultipartUpload(ctx, &storage.CreateMpuInput{Body: mBody, ForceInsert: true})
	if err != nil {
		return "", nil
	}

	return l.uploadIDTranslator.SetUploadID(cmo.Id), nil
}

func (l *Adapter) UploadPart(ctx context.Context, obj block.ObjectPointer, _ int64, reader io.Reader, uploadID string, partNumber int64) (string, error) {
	uploadPart, err := l.GetMultiPartUpload(ctx, uploadID)

	if err := isValidUploadID(uploadID); err != nil {
		return "", err
	}

	if uploadPart.State == "done" {
		return "", ErrFinalizedMultiPart
	}

	//md5Read := block.NewHashingReader(reader, block.HashFunctionMD5)

	lp, err := l.client.Objects().UploadPart(ctx, &storage.UploadPartInput{Id: l.uploadIDTranslator.TranslateUploadID(uploadID), PartNum: uint64(partNumber), ObjectReader: reader})

	if err != nil {
		return "", err
	}
	return lp.Part, err
}

func (l *Adapter) AbortMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string) error {

	err := l.client.Objects().AbortMultipartUpload(ctx, &storage.AbortMpuInput{PartsDirectoryPath: path.Join(l.accountName, "uploads", uploadID[:3], l.uploadIDTranslator.TranslateUploadID(uploadID))})

	if err != nil {
		return err
	}
	l.uploadIDTranslator.RemoveUploadID(uploadID)
	return nil

}

func (l *Adapter) translateToMantaPath(obj block.ObjectPointer) string {
	objectPathFromStorageNameSpace := strings.ReplaceAll(obj.StorageNamespace, "manta://", "")
	objectPathWithBucket := path.Join(defaultMantaRoot, objectPathFromStorageNameSpace, obj.Identifier)
	return objectPathWithBucket

}

func (l *Adapter) CompleteMultiPartUpload(ctx context.Context, obj block.ObjectPointer, uploadID string, multipartList *block.MultipartUploadCompletion) (*string, int64, error) {

	lmop, err := l.client.Objects().ListMultipartUploadParts(ctx, &storage.ListMpuPartsInput{Id: l.uploadIDTranslator.TranslateUploadID(uploadID)})
	if err != nil {
		return nil, 0, err
	}
	var parts []string
	var size int64
	for _, es := range lmop.Parts {
		parts = append(parts, es.ETag)
		size = size + es.Size
	}

	cob := storage.CommitMpuBody{Parts: parts}
	err = l.client.Objects().CommitMultipartUpload(ctx, &storage.CommitMpuInput{Id: l.uploadIDTranslator.TranslateUploadID(uploadID), Body: cob})

	op, err := l.GetMultiPartUpload(ctx, uploadID)
	if err != nil {
		return nil, 0, err

	}

	output, err := l.client.Objects().Get(ctx, &storage.GetObjectInput{ObjectPath: op.TargetObject})
	if err != nil {
		return nil, -1, err
	}
	l.uploadIDTranslator.RemoveUploadID(uploadID)

	return &output.ETag, size, err
}

func (l *Adapter) GetMultiPartUpload(ctx context.Context, uploadID string) (*storage.GetMpuOutput, error) {
	return l.client.Objects().GetMultipartUpload(ctx, &storage.GetMpuInput{PartsDirectoryPath: path.Join(l.accountName, "uploads", uploadID[:3], l.uploadIDTranslator.TranslateUploadID(uploadID))})

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

func formatMultipartFilename(uploadID string, partNumber int64) string {
	// keep natural sort order with zero padding
	return fmt.Sprintf("%s"+partSuffix+"%05d", uploadID, partNumber)

}

func (a *Adapter) log(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx)
}
