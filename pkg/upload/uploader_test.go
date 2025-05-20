package upload

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/block/polt/pkg/destinations"
	"github.com/stretchr/testify/require"
)

func mockConfigLoader(_ context.Context, _ ...func(*config.LoadOptions) error) (aws.Config, error) {
	return aws.Config{}, nil
}

func TestNewUploader(t *testing.T) {
	uploader, err := NewUploader(context.TODO(), destinations.S3File.String(), "s3://testBucket/testDir/testSubDir", mockConfigLoader)
	require.NoError(t, err)
	s3uploader, ok := uploader.(*S3Uploader)
	require.True(t, ok)
	require.NotNil(t, s3uploader)
	require.Equal(t, "testBucket", s3uploader.bucketName)
	require.Equal(t, "testDir/testSubDir", s3uploader.key)

	uploader, err = NewUploader(context.TODO(), destinations.LocalParquetFile.String(), "testfile", mockConfigLoader)
	require.NoError(t, err)

	fileUploader, ok := uploader.(*FileUploader)
	require.True(t, ok)
	require.NotNil(t, fileUploader)
}
