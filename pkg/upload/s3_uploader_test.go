package upload

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type mockS3Client struct {
	PutObjectCallBack func(ctx context.Context, params *s3.PutObjectInput, fns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, fns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return m.PutObjectCallBack(ctx, params, fns...)
}

func TestS3Uploader_Upload(t *testing.T) {
	s3testClient := &mockS3Client{
		PutObjectCallBack: func(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			require.Equal(t, "testBucket", *params.Bucket)
			require.Equal(t, "testKey/testSuffix.parquet", *params.Key)

			return &s3.PutObjectOutput{}, nil
		},
	}
	s3uploader := &S3Uploader{
		bucketName: "testBucket",
		key:        "testKey",
		client:     s3testClient,
	}
	err := s3uploader.Upload(context.Background(), "testSuffix", []byte("testData"), nil)
	require.NoError(t, err)
}
