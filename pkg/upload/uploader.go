package upload

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/block/polt/pkg/destinations"
)

type Uploader interface {
	Upload(ctx context.Context, randomSnip string, data []byte, schema *arrow.Schema) error
}
type ConfigLoader func(ctx context.Context, optFns ...func(*config.LoadOptions) error) (aws.Config, error)

func NewUploader(ctx context.Context, tp string, dstPath string, loader ConfigLoader) (Uploader, error) {
	if tp == destinations.LocalParquetFile.String() {
		return NewFileUploader(dstPath), nil
	} else if tp == destinations.S3File.String() {
		cfg, err := loader(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to load AWS SDK config, %w", err)
		}
		s3up, err := NewS3Uploader(dstPath, cfg)
		if err != nil {
			return nil, err
		}

		return s3up, nil
	}

	return nil, fmt.Errorf("unsupported destination type: %s", tp)
}
