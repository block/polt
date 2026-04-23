package runner

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/siddontang/loggers"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLogger() loggers.Advanced {
	return logrus.New()
}

func newTestLogrus() *logrus.Logger {
	return logrus.New()
}

func TestDBConnFuncType(t *testing.T) {
	// Verify that DBConnFunc has the correct signature and can be assigned.
	var fn DBConnFunc
	assert.Nil(t, fn, "zero-value DBConnFunc should be nil")

	fn = func(dsn string, cfg *dbconn.DBConfig) (*sql.DB, error) {
		return nil, nil
	}
	assert.NotNil(t, fn)
}

func TestStageRunnerConfig_DBConnFuncNilByDefault(t *testing.T) {
	cfg := &StageRunnerConfig{}
	assert.Nil(t, cfg.DBConnFunc, "DBConnFunc should be nil by default for backward compatibility")
}

func TestArchiveRunnerConfig_DBConnFuncNilByDefault(t *testing.T) {
	cfg := &ArchiveRunnerConfig{}
	assert.Nil(t, cfg.DBConnFunc, "DBConnFunc should be nil by default for backward compatibility")
}

func TestNewStageRunner_StoresDBConnFunc(t *testing.T) {
	called := false
	fn := func(dsn string, cfg *dbconn.DBConfig) (*sql.DB, error) {
		called = true
		return nil, nil
	}

	sr, err := NewStageRunner(&StageRunnerConfig{
		DBConnFunc:      fn,
		TargetChunkTime: 1,
	}, newTestLogger())
	require.NoError(t, err)
	require.NotNil(t, sr)
	assert.NotNil(t, sr.dbConnFunc, "dbConnFunc should be stored on StageRunner")

	// Call it to verify it's the same function
	_, _ = sr.dbConnFunc("", nil)
	assert.True(t, called)
}

func TestNewStageRunner_NilDBConnFunc(t *testing.T) {
	sr, err := NewStageRunner(&StageRunnerConfig{
		TargetChunkTime: 1,
	}, newTestLogger())
	require.NoError(t, err)
	require.NotNil(t, sr)
	assert.Nil(t, sr.dbConnFunc, "dbConnFunc should be nil when not provided")
}

func TestNewArchiveRunner_StoresDBConnFunc(t *testing.T) {
	called := false
	fn := func(dsn string, cfg *dbconn.DBConfig) (*sql.DB, error) {
		called = true
		return nil, nil
	}

	ar, err := NewArchiveRunner(&ArchiveRunnerConfig{
		DBConnFunc: fn,
	}, newTestLogrus())
	require.NoError(t, err)
	require.NotNil(t, ar)
	assert.NotNil(t, ar.dbConnFunc, "dbConnFunc should be stored on ArchiveRunner")

	_, _ = ar.dbConnFunc("", nil)
	assert.True(t, called)
}

func TestNewArchiveRunner_NilDBConnFunc(t *testing.T) {
	ar, err := NewArchiveRunner(&ArchiveRunnerConfig{}, newTestLogrus())
	require.NoError(t, err)
	require.NotNil(t, ar)
	assert.Nil(t, ar.dbConnFunc, "dbConnFunc should be nil when not provided")
}

func TestDBConnFunc_ReceivesCorrectArgs(t *testing.T) {
	var receivedDSN string
	var receivedConfig *dbconn.DBConfig

	fn := func(dsn string, cfg *dbconn.DBConfig) (*sql.DB, error) {
		receivedDSN = dsn
		receivedConfig = cfg
		return nil, errors.New("test: not a real db")
	}

	sr, err := NewStageRunner(&StageRunnerConfig{
		DBConnFunc:      fn,
		Host:            "myhost:3306",
		Username:        "myuser",
		Password:        "mypass",
		Database:        "mydb",
		Threads:         8,
		LockWaitTimeout: 30,
		TargetChunkTime: 1,
	}, newTestLogger())
	require.NoError(t, err)

	// Call setupDBAndCheckIfRunSucceeded which will invoke dbConnFunc
	_, _ = sr.setupDBAndCheckIfRunSucceeded(t.Context())

	expectedDSN := "myuser:mypass@tcp(myhost:3306)/mydb"
	assert.Equal(t, expectedDSN, receivedDSN, "DBConnFunc should receive the constructed DSN")
	assert.NotNil(t, receivedConfig, "DBConnFunc should receive the dbconn config")
	assert.Equal(t, 9, receivedConfig.MaxOpenConnections, "MaxOpenConnections should be threads+1")
}

func TestDBConnFunc_ErrorPropagated(t *testing.T) {
	expectedErr := errors.New("custom connection error")
	fn := func(dsn string, cfg *dbconn.DBConfig) (*sql.DB, error) {
		return nil, expectedErr
	}

	sr, err := NewStageRunner(&StageRunnerConfig{
		DBConnFunc:      fn,
		TargetChunkTime: 1,
	}, newTestLogger())
	require.NoError(t, err)

	_, err = sr.setupDBAndCheckIfRunSucceeded(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
}

func TestDBConnFunc_ArchiveRunner_ErrorPropagated(t *testing.T) {
	expectedErr := errors.New("custom archive connection error")
	fn := func(dsn string, cfg *dbconn.DBConfig) (*sql.DB, error) {
		return nil, expectedErr
	}

	ar, err := NewArchiveRunner(&ArchiveRunnerConfig{
		DBConnFunc: fn,
	}, newTestLogrus())
	require.NoError(t, err)

	_, err = ar.setupDBAndCheckIfRunSucceeded(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
}
