package upload

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type S3Uploader struct {
	bucketName string
	key        string
	client     S3Client
}

func NewS3Uploader(dstPath string, cfg aws.Config) (*S3Uploader, error) {
	// Remove the "s3://" prefix if it exists.
	dstPath = strings.TrimPrefix(dstPath, "s3://")

	// Separate the bucket name and key
	index := strings.Index(dstPath, "/")
	if index == -1 {
		return nil, fmt.Errorf("invalid S3 path: %s", dstPath)
	}
	bucketName := dstPath[:index]

	key := dstPath[index+1:]
	// Create an S3 client
	client := s3.NewFromConfig(cfg)

	return &S3Uploader{
		client:     client,
		bucketName: bucketName,
		key:        key,
	}, nil
}

func (u *S3Uploader) Upload(ctx context.Context, suffix string, data []byte, _ *arrow.Schema) error {
	// Upload the buffer to S3
	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(u.bucketName),
		Key:         aws.String(u.key + "/" + suffix + ".parquet"),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to S3, %w", err)
	}

	return nil
}
