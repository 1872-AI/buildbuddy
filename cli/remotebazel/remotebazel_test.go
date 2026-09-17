package remotebazel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

// Used to mock logs streamed from the BuildBuddy server.
type scriptedBuildBuddyClient struct {
	bbspb.BuildBuddyServiceClient

	mu sync.Mutex
	// Results returned from successive Recv calls on the stream for each log,
	// keyed by log type; once a log's script is exhausted, Recv returns io.EOF.
	// printLogs reads the logs concurrently, so each gets its own script rather
	// than competing for one.
	scripts map[elpb.LogType][]scriptedRecv
	// ChunkId of each GetEventLog request: the initial request, plus the
	// chunk each reconnect resumed from.
	requestedChunkIDs []string
	// Simulates a server predating the split logs, which serves the build log
	// for any log type it does not recognize.
	legacyServer bool
}

type scriptedRecv struct {
	rsp *elpb.GetEventLogChunkResponse
	err error
	// If set, hook runs before the result is returned.
	hook func()
}

func (c *scriptedBuildBuddyClient) GetEventLog(ctx context.Context, req *elpb.GetEventLogChunkRequest, opts ...grpc.CallOption) (bbspb.BuildBuddyService_GetEventLogClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestedChunkIDs = append(c.requestedChunkIDs, req.GetChunkId())
	return &scriptedEventLogStream{client: c, logType: req.GetType()}, nil
}

// scriptedEventLogStream returns the scripted results, then ends the stream
// with io.EOF like a real server-side stream would.
type scriptedEventLogStream struct {
	grpc.ClientStream

	client  *scriptedBuildBuddyClient
	logType elpb.LogType
}

func (s *scriptedEventLogStream) Recv() (*elpb.GetEventLogChunkResponse, error) {
	c := s.client
	c.mu.Lock()
	if len(c.scripts[s.logType]) == 0 {
		c.mu.Unlock()
		return nil, io.EOF
	}
	next := c.scripts[s.logType][0]
	c.scripts[s.logType] = c.scripts[s.logType][1:]
	c.mu.Unlock()

	if next.hook != nil {
		next.hook()
	}
	if next.rsp != nil {
		// A real server reports which log it served, so a client can tell when
		// it was given the build log for a type the server did not recognize.
		next.rsp.ServedType = s.logType
		if c.legacyServer {
			next.rsp.ServedType = elpb.LogType_BUILD_LOG
		}
	}
	return next.rsp, next.err
}

func TestParseRemoteCliFlags(t *testing.T) {
	type testCase struct {
		name              string
		inputArgs         []string
		expectedOutput    []string
		expectedFlagValue map[string]string
		expectedError     bool
	}

	testCases := []testCase{
		{
			name: "one remote cli flag",
			inputArgs: []string{
				"--remote_runner=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
			},
		},
		{
			name: "one remote cli flag - space between val",
			inputArgs: []string{
				"--remote_runner",
				"val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
			},
		},
		{
			name: "multiple remote cli flags",
			inputArgs: []string{
				"--remote_runner=val",
				"--os=val2",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name: "repeated remote cli flags",
			inputArgs: []string{
				"--env=key=val",
				"--remote_runner=val",
				"--env=key2=val2",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"env":           "key=val,key2=val2",
			},
		},
		{
			name: "no flags",
			inputArgs: []string{
				"build",
				"//...",
			},
			expectedOutput: []string{
				"build",
				"//...",
			},
		},
		{
			name: "startup flags, but no cli flags",
			inputArgs: []string{
				"--output_base=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"build",
				"//...",
			},
		},
		{
			name: "startup flags, but no cli flags - space between value",
			inputArgs: []string{
				"--output_base",
				"val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base",
				"val",
				"build",
				"//...",
			},
		},
		{
			name: "mix of startup flags and cli flags - starting with cli flag",
			inputArgs: []string{
				"--os",
				"val2",
				"--output_base=val",
				"--remote_runner=val",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name: "mix of startup flags and cli flags - starting with startup flag",
			inputArgs: []string{
				"--output_base=val",
				"--os",
				"val2",
				"--remote_runner=val",
				"--system_rc",
				"build",
				"//...",
			},
			expectedOutput: []string{
				"--output_base=val",
				"--system_rc",
				"build",
				"//...",
			},
			expectedFlagValue: map[string]string{
				"remote_runner": "val",
				"os":            "val2",
			},
		},
		{
			name:              "empty",
			inputArgs:         []string{},
			expectedOutput:    []string{},
			expectedFlagValue: map[string]string{},
			expectedError:     true,
		},
		{
			name: "flags after the bazel command shouldn't be affected",
			inputArgs: []string{
				"--os",
				"val2",
				"build",
				"//...",
				"--os=untouched",
			},
			expectedOutput: []string{
				"build",
				"//...",
				"--os=untouched",
			},
			expectedFlagValue: map[string]string{
				"os": "val2",
			},
		},
		{
			name: "explicitly passing `bazel` should error",
			inputArgs: []string{
				"bazel",
				"build",
				"//...",
			},
			expectedError: true,
		},
		{
			name: "unexpected token before bazel command should error",
			inputArgs: []string{
				"random",
				"build",
				"//...",
			},
			expectedError: true,
		},
	}
	for _, tc := range testCases {
		actualOutput, err := parseRemoteCliFlags(tc.inputArgs)
		if tc.expectedError {
			require.Error(t, err, tc.name)
		} else {
			require.NoError(t, err, tc.name)
			require.Equal(t, tc.expectedOutput, actualOutput, tc.name)
		}

		for flag, expectedVal := range tc.expectedFlagValue {
			actualVal := RemoteFlagset.Lookup(flag).Value
			require.Equal(t, expectedVal, actualVal.String(), tc.name)
		}
	}
}

// TestPrintLogs_RoutesEachLogToItsOwnStream checks that the command's stdout
// lands on this process's stdout and nothing else does.
func TestPrintLogs_RoutesEachLogToItsOwnStream(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		scripts: map[elpb.LogType][]scriptedRecv{
			elpb.LogType_STDOUT_LOG: {
				{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("//some:target\n")}},
			},
			// The runner interleaves its narration into the command's stderr in
			// the order it was written, so this is one ordered stream.
			elpb.LogType_STDERR_LOG: {
				{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("Syncing existing repo...\nLoading: 1 packages loaded\n")}},
			},
		},
	}

	stdout, stderr, err := runPrintLogsWithCapturedOutput(t, client)

	require.NoError(t, err)
	require.Equal(t, "//some:target\n", stdout)
	require.Equal(t, "Syncing existing repo...\nLoading: 1 packages loaded\n", stderr)
}

// TestPrintLogs_FallsBackForAServerWithoutSplitLogs covers version skew: an
// older app serves the build log for a type it does not recognize.
func TestPrintLogs_FallsBackForAServerWithoutSplitLogs(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		legacyServer: true,
		scripts: map[elpb.LogType][]scriptedRecv{
			elpb.LogType_STDOUT_LOG: {{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("merged log\n")}}},
			elpb.LogType_STDERR_LOG: {{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("merged log\n")}}},
			elpb.LogType_BUILD_LOG:  {{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("merged log\n")}}},
		},
	}

	stdout, stderr, err := runPrintLogsWithCapturedOutput(t, client)

	require.NoError(t, err)
	// The old behaviour: everything on stderr, exactly once, and nothing on
	// stdout - not the merged log copied onto every stream.
	require.Empty(t, stdout)
	require.Equal(t, "merged log\n", stderr)
}

func TestPrintLogs(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		scripts: map[elpb.LogType][]scriptedRecv{elpb.LogType_STDOUT_LOG: {
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Analyzing: 1\n"),
				NextChunkId: "0001",
				Live:        true,
			}},
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Analyzing: 2\nBuilding.\n"),
				NextChunkId: "0002",
			}},
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer: []byte("Done.\n"),
			}},
		}},
	}

	out, err := runPrintLogsWithCapturedStdout(t, client)

	require.NoError(t, err)
	// Live chunks are streamed as they arrive rather than held until they are
	// finalized, so a local run and a remote one show output at the same point.
	require.Equal(t, "Analyzing: 1\nAnalyzing: 2\nBuilding.\nDone.\n", out)
}

// TestPrintLogs_LiveChunkRewrittenInPlace covers a chunk served again revised
// rather than merely longer, which a client assuming append-only would miss.
func TestPrintLogs_LiveChunkRewrittenInPlace(t *testing.T) {
	for _, tc := range []struct {
		name  string
		serve []string
		want  string
	}{
		{
			// The case a byte-length cursor cannot see at all: the revision is
			// exactly as long as what it replaces.
			name:  "same length",
			serve: []string{"1\n2\n", "1\n3\n"},
			want:  "1\n2\n3\n",
		},
		{
			name:  "shorter",
			serve: []string{"1\n22222\n", "1\n3\n"},
			want:  "1\n22222\n3\n",
		},
		{
			name:  "longer",
			serve: []string{"1\n2\n", "1\n33333\n"},
			want:  "1\n2\n33333\n",
		},
		{
			name:  "pure append is not disturbed",
			serve: []string{"1\n", "1\n2\n"},
			want:  "1\n2\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var script []scriptedRecv
			for _, buf := range tc.serve {
				script = append(script, scriptedRecv{rsp: &elpb.GetEventLogChunkResponse{
					Buffer:      []byte(buf),
					NextChunkId: "0000",
					Live:        true,
				}})
			}
			client := &scriptedBuildBuddyClient{
				scripts: map[elpb.LogType][]scriptedRecv{elpb.LogType_STDOUT_LOG: script},
			}

			out, err := runPrintLogsWithCapturedStdout(t, client)

			require.NoError(t, err)
			// stdout is a pipe here, so the revised row cannot be erased; what
			// matters is that the new content is shown and nothing is repeated.
			require.Equal(t, tc.want, out)
		})
	}
}

// TestPrintLogs_ChunkBoundaryDoesNotRepeatTheTail covers the stitch between
// chunks: the tail that did not fit is served again as the start of the next
// one, and must not be printed twice.
func TestPrintLogs_ChunkBoundaryDoesNotRepeatTheTail(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		scripts: map[elpb.LogType][]scriptedRecv{elpb.LogType_STDOUT_LOG: {
			// Live: two settled lines and a tail.
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("one\ntwo\ntail\n"),
				NextChunkId: "0000",
				Live:        true,
			}},
			// The chunk finalizes holding only the settled lines; "tail\n"
			// moves to the next chunk.
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("one\ntwo\n"),
				NextChunkId: "0001",
			}},
			// The next chunk therefore starts with the tail we already showed.
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("tail\nthree\n"),
				NextChunkId: "0001",
				Live:        true,
			}},
		}},
	}

	out, err := runPrintLogsWithCapturedStdout(t, client)

	require.NoError(t, err)
	require.Equal(t, "one\ntwo\ntail\nthree\n", out)
}

func TestPrintLogs_ReturnsStreamError(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		scripts: map[elpb.LogType][]scriptedRecv{elpb.LogType_STDOUT_LOG: {
			// Serve a finalized chunk, which should be printed.
			{rsp: &elpb.GetEventLogChunkResponse{
				Buffer:      []byte("Analyzing: 1\n"),
				NextChunkId: "0001",
			}},
			// Fail the stream with a non-retryable error. printLogs should
			// return the error rather than treating it as a clean end of
			// the log.
			{err: status.NotFoundError("invocation not found")},
		}},
	}

	out, err := runPrintLogsWithCapturedStdout(t, client)

	require.True(t, status.IsNotFoundError(err), "expected NotFound, got: %v", err)
	require.Equal(t, "Analyzing: 1\n", out)
}

// TestOutputOptions_Quiet checks the output settings reach the app as request
// fields rather than runner flags, which an older app could not parse.
func TestOutputOptions_Quiet(t *testing.T) {
	require.NotContains(t, runnerFlags(), "--quiet")
	require.False(t, outputOptions().GetQuiet())

	setQuietForTest(t)

	o := outputOptions()
	require.True(t, o.GetQuiet())
	// The split is always asked for: this CLI reads the split logs, and falls
	// back to the merged one only when the server does not serve them.
	require.True(t, o.GetSplitStreams())
	require.NotContains(t, runnerFlags(), "--quiet")
}

func TestParseRemoteCliFlags_Quiet(t *testing.T) {
	setQuietForTest(t)
	for _, tc := range []struct {
		name           string
		inputArgs      []string
		expectedOutput []string
	}{
		{
			name:           "long form",
			inputArgs:      []string{"--quiet", "build", "//..."},
			expectedOutput: []string{"build", "//..."},
		},
		{
			name:           "short form",
			inputArgs:      []string{"-q", "build", "//..."},
			expectedOutput: []string{"build", "//..."},
		},
		{
			name:           "explicit value",
			inputArgs:      []string{"--quiet=true", "build", "//..."},
			expectedOutput: []string{"build", "//..."},
		},
		{
			// A bare boolean flag must not consume the flag that follows it.
			name:           "startup flag after the short form",
			inputArgs:      []string{"-q", "--output_base=/tmp/base", "build", "//..."},
			expectedOutput: []string{"--output_base=/tmp/base", "build", "//..."},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, RemoteFlagset.Set("quiet", "false"))

			actualOutput, err := parseRemoteCliFlags(tc.inputArgs)

			require.NoError(t, err)
			require.Equal(t, tc.expectedOutput, actualOutput)
			require.True(t, *quiet)
		})
	}
}

// setQuietForTest restores the process-global flag value after the test.
func setQuietForTest(t *testing.T) {
	previous := *quiet
	*quiet = true
	t.Cleanup(func() { *quiet = previous })
}

func TestGitConfig_BranchAndSha(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, originalMasterHeadCommit := testgit.MakeTempRepo(t, map[string]string{"hello.txt": "exit 0"})

	// Create a remote branch
	testshell.Run(t, remoteRepoPath, "git checkout -B remote_b")
	remoteBranchHeadCommit := testgit.CommitFiles(t, remoteRepoPath, map[string]string{"new_file.txt": "exit 0"})
	testshell.Run(t, remoteRepoPath, "git checkout master")

	type testCase struct {
		name string

		localBranchExistsRemotely bool
		localCommitExistsRemotely bool
		unpushedLocalCommit       bool
		detachedHead              bool
		detachedHeadMoved         bool

		expectedBranch  string
		expectedCommit  string
		expectedPatches []string
	}

	testCases := []testCase{
		{
			name:                      "Local branch and commit exist remotely",
			localBranchExistsRemotely: true,
			localCommitExistsRemotely: true,
			expectedBranch:            "remote_b",
			expectedCommit:            remoteBranchHeadCommit,
			expectedPatches:           []string{},
		},
		{
			name:                      "Local branch does not exist remotely",
			localBranchExistsRemotely: false,
			localCommitExistsRemotely: false,
			expectedBranch:            "master",
			expectedCommit:            originalMasterHeadCommit,
			expectedPatches:           []string{"local_file.txt"},
		},
		{
			name:                      "Local commit does not exist remotely",
			localBranchExistsRemotely: true,
			localCommitExistsRemotely: false,
			expectedBranch:            "master",
			expectedCommit:            originalMasterHeadCommit,
			expectedPatches:           []string{"local_file.txt"},
		},
		{
			name:                "On master with an unpushed commit",
			unpushedLocalCommit: true,
			expectedBranch:      "master",
			expectedCommit:      originalMasterHeadCommit,
			expectedPatches:     []string{"local_only_commited_file.txt"},
		},
		{
			name:            "Detached HEAD without additional commits",
			detachedHead:    true,
			expectedBranch:  "master",
			expectedCommit:  originalMasterHeadCommit,
			expectedPatches: []string{"local_file.txt"},
		},
		{
			name:              "Detached HEAD with additional commits",
			detachedHead:      true,
			detachedHeadMoved: true,
			expectedBranch:    "master",
			expectedCommit:    originalMasterHeadCommit,
			expectedPatches:   []string{"detached_file.txt"},
		},
	}

	for i, tc := range testCases {
		// Setup a "local" repo
		localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
		err := os.Chdir(localRepoPath)
		require.NoError(t, err, tc.name)
		resetRepoRootPathForTest(t)

		if tc.unpushedLocalCommit {
			testgit.CommitFiles(t, localRepoPath, map[string]string{"local_only_commited_file.txt": "exit 0"})
		} else if tc.localBranchExistsRemotely {
			testshell.Run(t, localRepoPath, "git checkout remote_b")
		} else {
			testshell.Run(t, localRepoPath, "git checkout -B local_only")

			// Simulate that the remote master is ahead of the local master
			testshell.Run(t, remoteRepoPath, "git checkout master")
			newFileName := fmt.Sprintf("new_file%d.txt", i)
			_ = testgit.CommitFiles(t, remoteRepoPath, map[string]string{newFileName: "exit 0"})
		}
		if !tc.localCommitExistsRemotely {
			testgit.CommitFiles(t, localRepoPath, map[string]string{"local_file.txt": "exit 0"})
		}

		if tc.detachedHead {
			testshell.Run(t, localRepoPath, "git checkout --detach")
			if tc.detachedHeadMoved {
				// A commit in a detached-head condition updates the `git branch` output from "detached at"
				// to "detached from".
				testgit.CommitFiles(t, localRepoPath, map[string]string{"detached_file.txt": "exit 0"})
			}
		}

		config, err := Config()
		require.NoError(t, err, tc.name)

		require.Equal(t, tc.expectedBranch, config.Ref, tc.name)
		require.Equal(t, tc.expectedCommit, config.CommitSHA, tc.name)
		require.Equal(t, len(tc.expectedPatches), len(config.Patches), tc.name)
		if len(tc.expectedPatches) > 0 {
			require.Contains(t, string(config.Patches[0]), tc.expectedPatches[0], tc.name)
		}

		// Reset remote repo for future test cases
		testshell.Run(t, remoteRepoPath, "git checkout master && git clean -fdx && git reset --hard "+originalMasterHeadCommit)
	}
}

func TestGitConfig_FetchURL(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, _ := testgit.MakeTempRepo(t, map[string]string{"hello.txt": "exit 0"})
	remoteUrl := "file://" + remoteRepoPath

	testCases := []struct {
		name            string
		expectedURL     string
		multipleRemotes bool
		isRemoteCached  bool
	}{
		{
			name:        "One remote is configured",
			expectedURL: remoteUrl,
		},
		{
			name:            "Selected remote is cached",
			multipleRemotes: true,
			isRemoteCached:  true,
			expectedURL:     remoteUrl,
		},
	}

	for _, tc := range testCases {
		// Setup a "local" repo
		localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
		err := os.Chdir(localRepoPath)
		require.NoError(t, err, tc.name)
		resetRepoRootPathForTest(t)

		if tc.multipleRemotes {
			testshell.Run(t, localRepoPath, "git remote add extra "+remoteUrl)
		}
		if tc.isRemoteCached {
			testshell.Run(t, localRepoPath, fmt.Sprintf("git config --replace-all %s.%s extra", gitConfigSection, gitConfigRemoteBazelRemote))
		}

		config, err := Config()
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.expectedURL, config.URL)
	}
}

func TestGeneratingPatches(t *testing.T) {
	// Setup the "remote" repo
	remoteRepoPath, _ := testgit.MakeTempRepo(t, map[string]string{
		"hello.txt":      "echo HI",
		"b.bin":          "",
		"deleted.bin":    "\x00\x01\x02\x03\x04",
		"attributed.md":  "v1",
		".gitattributes": "attributed.md binary\n",
	})

	// Setup a "local" repo
	localRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
	// Remote bazel runs commands in the working directory, so make sure it
	// is set correctly
	err := os.Chdir(localRepoPath)
	require.NoError(t, err)
	resetRepoRootPathForTest(t)

	testshell.Run(t, localRepoPath, `
		# Generate a diff on a pre-existing file
		echo "echo HELLO" > hello.txt

		# Generate a diff for a new untracked file
		echo "echo BYE" > bye.txt

		# Generate a binary diff on a pre-existing file
		echo -ne '\x00\x01\x02\x03\x04' > b.bin

		# Generate a binary diff on an untracked file
		echo -ne '\x00\x01\x02\x03\x04' > b2.bin

		# Delete a pre-existing binary file
		rm deleted.bin

		# Diff a file git treats as binary by attribute, though its bytes are text
		echo "v2" > attributed.md
`)

	config, err := Config()
	require.NoError(t, err)

	all := ""
	for _, patchBytes := range config.Patches {
		all += string(patchBytes)
	}
	require.Contains(t, all, "HELLO")
	require.Contains(t, all, "BYE")
	// Every file git renders as binary needs the binary format, deletions and
	// attribute-marked files included.
	for _, binaryFile := range []string{"b.bin", "b2.bin", "deleted.bin", "attributed.md"} {
		require.Contains(t, all, binaryFile)
	}
	require.Equal(t, 4, strings.Count(all, "GIT binary patch"))

	// The runner applies the patchset; a binary patch without its full index line fails there.
	runnerRepoPath := testgit.MakeTempRepoClone(t, remoteRepoPath)
	for i, patchBytes := range config.Patches {
		patchPath := filepath.Join(t.TempDir(), fmt.Sprintf("%d.patch", i))
		require.NoError(t, os.WriteFile(patchPath, patchBytes, 0644))
		testshell.Run(t, runnerRepoPath, fmt.Sprintf("git apply %q", patchPath))
	}
	for _, file := range []string{"hello.txt", "bye.txt", "b.bin", "b2.bin", "attributed.md"} {
		want, err := os.ReadFile(filepath.Join(localRepoPath, file))
		require.NoError(t, err)
		got, err := os.ReadFile(filepath.Join(runnerRepoPath, file))
		require.NoError(t, err)
		require.Equal(t, want, got, "%s should match the local working tree", file)
	}
	require.NoFileExists(t, filepath.Join(runnerRepoPath, "deleted.bin"))
}

func TestWorkingDirectory(t *testing.T) {
	rootDir := t.TempDir()
	repoRoot := filepath.Join(rootDir, "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repoRoot, "subdir", "nested"), 0755))

	testCases := []struct {
		name              string
		workspaceFilePath string
		expectedDir       string
		expectedError     string
	}{
		{
			name:              "Repo root workspace",
			workspaceFilePath: filepath.Join(repoRoot, "MODULE.bazel"),
			expectedDir:       "",
		},
		{
			name:              "Nested workspace",
			workspaceFilePath: filepath.Join(repoRoot, "subdir", "MODULE.bazel"),
			expectedDir:       "subdir",
		},
		{
			name:              "Deeply nested workspace",
			workspaceFilePath: filepath.Join(repoRoot, "subdir", "nested", "MODULE.bazel"),
			expectedDir:       filepath.Join("subdir", "nested"),
		},
		{
			name:              "Workspace outside repo root",
			workspaceFilePath: filepath.Join(rootDir, "outside", "MODULE.bazel"),
			expectedError:     "outside repo root",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := workingDirectory(repoRoot, tc.workspaceFilePath)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedDir, dir)
		})
	}
}

func resetRepoRootPathForTest(t *testing.T) {
	storage.RepoRootPath = sync.OnceValues(func() (string, error) {
		return os.Getwd()
	})
}

func TestParseArgs(t *testing.T) {
	t.Setenv("BUILDBUDDY_API_KEY", "test-api-key")

	bazelArgs, execArgs, err := parseArgs([]string{
		"--output_base", "/tmp/output_base",
		"test",
		"-c", "opt",
		"--config=remote_only",
		"--bes_backend=grpc://user-bes",
		"--remote_cache=grpc://user-cache",
		"--remote_header=x-custom=1",
		"//foo",
		"--",
		"--exec_arg",
	})

	require.NoError(t, err)
	require.Equal(t, []string{
		// Startup flags should be preserved.
		"--output_base=/tmp/output_base",
		"test",
		// Remote configs should be added immediately after the Bazel command.
		"--config=buildbuddy_remote_cache",
		"--config=buildbuddy_bes_results_url",
		"--config=buildbuddy_bes_backend",
		// Bazel flags should be canonicalized.
		"--compilation_mode=opt",
		// Config flags should not be expanded and passed through to the remote runner as is.
		"--config=remote_only",
		// Remote headers should be preserved.
		"--remote_header=x-custom=1",
		// API key should be set.
		"--remote_header=x-buildbuddy-api-key=test-api-key",
		"//foo",
	}, bazelArgs)
	// Exec args should be preserved.
	require.Equal(t, []string{"--exec_arg"}, execArgs)
}

func TestParseArgs_RunAddsRemoteArgsBeforeExecutableArgs(t *testing.T) {
	t.Setenv("BUILDBUDDY_API_KEY", "test-api-key")
	originalRunRemotely := *runRemotely
	*runRemotely = false
	t.Cleanup(func() { *runRemotely = originalRunRemotely })

	bazelArgs, execArgs, err := parseArgs([]string{
		"run",
		"--noremote_upload_local_results",
		"--remote_build_event_upload=minimal",
		"--script_path=/tmp/custom-run-script.sh",
		"@bazel-diff//cli:bazel-diff",
		"generate-hashes",
		"--",
		"--includeTargetType",
		"-w",
		".",
	})
	require.NoError(t, err)

	// Rejoin and re-split the args to ensure that they are still properly formatted.
	forwardedBazelArgs, forwardedExecArgs := arg.SplitExecutableArgs(
		arg.JoinExecutableArgs(bazelArgs, execArgs),
	)
	require.Equal(t, "run", arg.GetCommand(forwardedBazelArgs))
	require.Equal(t, []string{"@bazel-diff//cli:bazel-diff"}, arg.GetTargets(forwardedBazelArgs))
	require.ElementsMatch(t, []string{
		"buildbuddy_bes_backend",
		"buildbuddy_bes_results_url",
		"buildbuddy_remote_cache",
	}, arg.GetMulti(forwardedBazelArgs, "config"))
	require.Contains(t, forwardedBazelArgs, "--remote_upload_local_results")
	require.Equal(t, "minimal", arg.Get(forwardedBazelArgs, "remote_build_event_upload"))
	require.Equal(t,
		"$BUILDBUDDY_CI_RUNNER_ROOT_DIR/bazel-run-scripts/run.sh",
		arg.Get(forwardedBazelArgs, "script_path"),
	)
	require.Equal(t, []string{
		"generate-hashes",
		"--includeTargetType",
		"-w",
		".",
	}, forwardedExecArgs)
	require.Contains(t,
		quoteRemoteBazelArgs(bazelArgs),
		`--script_path="$BUILDBUDDY_CI_RUNNER_ROOT_DIR"/bazel-run-scripts/run.sh`,
	)
}

func TestEnvForLocalRun(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"RUNFILES_DIR=/old/runfiles",
		"RUNFILES_MANIFEST_FILE=/old/MANIFEST",
		"RUNFILES_MANIFEST_ONLY=1",
		"BUILD_WORKSPACE_DIRECTORY=/old/workspace",
		"BUILD_WORKING_DIRECTORY=/old/working-directory",
		"USER=test",
	}

	require.Equal(t, []string{
		"PATH=/usr/bin",
		"USER=test",
		"RUNFILES_DIR=/new/runfiles",
		"BUILD_WORKSPACE_DIRECTORY=/new/workspace",
		"BUILD_WORKING_DIRECTORY=/new/working-directory",
	}, envForLocalRun(env, "/new/runfiles", "/new/workspace", "/new/working-directory"))
}

func TestEnvForLocalRun_NoRunfiles(t *testing.T) {
	env := []string{
		"PATH=/usr/bin",
		"RUNFILES_DIR=/old/runfiles",
		"RUNFILES_MANIFEST_FILE=/old/MANIFEST",
		"BUILD_WORKSPACE_DIRECTORY=/old/workspace",
		"BUILD_WORKING_DIRECTORY=/old/working-directory",
		"USER=test",
	}

	require.Equal(t, []string{
		"PATH=/usr/bin",
		"USER=test",
		"BUILD_WORKSPACE_DIRECTORY=/new/workspace",
		"BUILD_WORKING_DIRECTORY=/new/working-directory",
	}, envForLocalRun(env, "", "/new/workspace", "/new/working-directory"))
}

func TestHasSupportingRunfiles(t *testing.T) {
	executablePath := "bazel-out/k8-fastbuild/bin/main.sh"
	executable := &bespb.Runfile{File: &bespb.File{Name: executablePath}}

	require.False(t, hasSupportingRunfiles([]*bespb.Runfile{executable}, nil, executablePath))
	require.True(t, hasSupportingRunfiles([]*bespb.Runfile{
		executable,
		{File: &bespb.File{Name: "bazel-out/k8-fastbuild/bin/main.sh.runfiles/_main/data.txt"}},
	}, nil, executablePath))
	require.True(t, hasSupportingRunfiles(
		[]*bespb.Runfile{executable},
		[]*bespb.Tree{{Name: "bazel-out/k8-fastbuild/bin/main.sh.runfiles/_main/data"}},
		executablePath,
	))
}

func TestQuoteRemoteBazelArgs_RunScriptEnvVarExpanded(t *testing.T) {
	// This flag should not be quoted with shlex.Quote, which explicitly prevents env var expansion.
	// The path should be quoted with double quotes, so the remote shell expands the BUILDBUDDY_CI_RUNNER_ROOT_DIR
	// env var.
	require.Equal(t,
		`--script_path="$BUILDBUDDY_CI_RUNNER_ROOT_DIR"/bazel-run-scripts/run.sh`,
		quoteRemoteBazelArgs([]string{runScriptPathFlag}),
	)
}

func TestGetRemoteRunnerTarget(t *testing.T) {
	for _, tc := range []struct {
		name       string
		envValue   string
		flagArgs   []string
		wantRunner string
	}{
		{
			name:       "neither flag nor env set",
			wantRunner: login.DefaultApiTarget,
		},
		{
			name:       "env set, no flag",
			envValue:   "grpcs://env-runner.dev",
			wantRunner: "grpcs://env-runner.dev",
		},
		{
			name:       "flag takes precedence over env",
			envValue:   "grpcs://env-runner.dev",
			flagArgs:   []string{"--remote_runner=grpc://flag-runner.dev", "build", "//..."},
			wantRunner: "grpc://flag-runner.dev",
		},
		{
			name:       "flag set, no env",
			flagArgs:   []string{"--remote_runner=grpcs://flag-runner.dev", "build", "//..."},
			wantRunner: "grpcs://flag-runner.dev",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BUILDBUDDY_REMOTE_RUNNER", tc.envValue)

			// Reset the remoteRunner flag to its default before each subtest so
			// prior parses don't leak.
			_ = RemoteFlagset.Set("remote_runner", login.DefaultApiTarget)

			actual := getRemoteRunnerTarget(tc.flagArgs)
			require.Equal(t, tc.wantRunner, actual)
		})
	}
}

// Helper to run the printLogs function with os.Stdout captured, returning the
// captured output and the error returned by printLogs.
func runPrintLogsWithCapturedStdout(t *testing.T, client bbspb.BuildBuddyServiceClient) (string, error) {
	stdout, _, err := runPrintLogsWithCapturedOutput(t, client)
	return stdout, err
}

// runPrintLogsWithCapturedOutput runs printLogs with both of this process's
// output streams captured, so which stream each log reached can be asserted.
func runPrintLogsWithCapturedOutput(t *testing.T, client bbspb.BuildBuddyServiceClient) (stdout, stderr string, printErr error) {
	outR, outW, err := os.Pipe()
	require.NoError(t, err)
	errR, errW, err := os.Pipe()
	require.NoError(t, err)
	oldStdout, oldStderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = outW, errW
	defer func() { os.Stdout, os.Stderr = oldStdout, oldStderr }()

	printErr = printLogs(t.Context(), client, "test-invocation-id")

	require.NoError(t, outW.Close())
	require.NoError(t, errW.Close())
	out, err := io.ReadAll(outR)
	require.NoError(t, err)
	errOut, err := io.ReadAll(errR)
	require.NoError(t, err)
	return string(out), string(errOut), printErr
}

// TestLogStream_WaitsForInvocationToBeCreated covers the window between
// dispatching a run and its invocation existing, where reads fail NotFound.
func TestLogStream_WaitsForInvocationToBeCreated(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		scripts: map[elpb.LogType][]scriptedRecv{
			elpb.LogType_STDOUT_LOG: {
				{err: status.NotFoundError("invocation not found")},
				{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("Done.\n")}},
			},
		},
	}

	out, err := runPrintLogsWithCapturedStdout(t, client)

	require.NoError(t, err)
	require.Equal(t, "Done.\n", out)
}

// TestLogStream_ReturnsNotFoundAfterFirstResponse covers the other side: once
// read from, a NotFound is a real error rather than a run yet to start.
func TestLogStream_ReturnsNotFoundAfterFirstResponse(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		scripts: map[elpb.LogType][]scriptedRecv{
			elpb.LogType_STDOUT_LOG: {
				{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("Analyzing: 1\n")}},
				{err: status.NotFoundError("invocation not found")},
			},
		},
	}

	out, err := runPrintLogsWithCapturedStdout(t, client)

	require.True(t, status.IsNotFoundError(err), "expected NotFound, got: %v", err)
	require.Equal(t, "Analyzing: 1\n", out)
}

// TestDetermineRemote_DoesNotPromptWithoutATerminal checks that no prompt is
// drawn when nothing can answer it, which would wedge the run.
func TestDetermineRemote_DoesNotPromptWithoutATerminal(t *testing.T) {
	repoPath, _ := testgit.MakeTempRepo(t, map[string]string{"a.txt": "a"})
	testshell.Run(t, repoPath, `git remote add fork https://github.com/example/fork.git`)
	testshell.Run(t, repoPath, `git remote add upstream https://github.com/example/upstream.git`)

	// Other tests in this binary chdir into temp dirs that are removed before
	// this one runs, which leaves the process with no working directory at all
	// and makes t.Chdir fail on its own Getwd. Land somewhere real first.
	_ = os.Chdir(os.TempDir())
	t.Chdir(repoPath)

	// os.Stdin is not a terminal under `bazel test`, which is the same
	// situation as a piped or scripted run.
	_, err := determineRemote()

	require.Error(t, err)
	require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got: %v", err)
	// The error has to say what to set, or a caller is left guessing.
	require.Contains(t, err.Error(), "remote-bazel-remote-name")
}

// TestLogSink_ClosesAStyleLeftOpen covers colour bleeding between the streams:
// the renderer emits only style changes, so a chunk can end with a colour set,
// which would tint whatever the other stream writes to the terminal next.
func TestLogSink_ClosesAStyleLeftOpen(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write string
		want  string
	}{
		{
			name:  "closes a colour the chunk left set",
			write: "Streaming build results to: \x1b[36mhttp://example/invocation/1\n",
			want:  "Streaming build results to: \x1b[36mhttp://example/invocation/1\n\x1b[0m",
		},
		{
			name:  "leaves an already closed style alone",
			write: "\x1b[32mINFO: \x1b[0mLoading\n",
			want:  "\x1b[32mINFO: \x1b[0mLoading\n",
		},
		{
			name:  "treats an empty parameter list as a reset",
			write: "\x1b[32mINFO: \x1b[mLoading\n",
			want:  "\x1b[32mINFO: \x1b[mLoading\n",
		},
		{
			name:  "adds nothing to output with no styles at all",
			write: "//cli/log:log\n//cli/log:log_test\n",
			want:  "//cli/log:log\n//cli/log:log_test\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			sink := &logSink{w: &buf, isTerminal: true, closesStyles: true}

			require.NoError(t, sink.live([]byte(tc.write)))

			require.Equal(t, tc.want, buf.String())
		})
	}
}

// TestLogSink_DoesNotWriteEscapesToANonTerminal checks a redirected stream gets
// the bytes and nothing else.
func TestLogSink_DoesNotWriteEscapesToANonTerminal(t *testing.T) {
	var buf bytes.Buffer
	sink := &logSink{w: &buf, isTerminal: false}

	require.NoError(t, sink.live([]byte("results \x1b[36mcoloured\n")))

	require.Equal(t, "results \x1b[36mcoloured\n", buf.String())
}

// TestLogSinks_StderrStyleDoesNotTintStdout drives the two sinks in the order
// that tinted the command's results with the colour of the URL before them.
func TestLogSinks_StderrStyleDoesNotTintStdout(t *testing.T) {
	// One buffer for both sinks: on a terminal they share the same device.
	var term bytes.Buffer
	// stdout does not close styles: it has to match a local run exactly.
	stdout := &logSink{w: &term, isTerminal: true}
	stderr := &logSink{w: &term, isTerminal: true, closesStyles: true}

	// stderr first, ending with the URL's colour still set - that is what the
	// renderer emits, because it carries style state to the next line.
	require.NoError(t, stderr.live([]byte("INFO: Streaming build results to: \x1b[36mhttp://example/invocation/1\n")))
	// Then the command's own output.
	require.NoError(t, stdout.live([]byte("//cli/log:log\n")))

	// The results must not inherit the colour: everything from the last style
	// before them has to be a reset.
	out := term.String()
	idx := strings.Index(out, "//cli/log:log")
	require.Greater(t, idx, 0)
	styles := sgrPattern.FindAllString(out[:idx], -1)
	require.NotEmpty(t, styles, "expected the stderr colour to be in the stream")
	require.False(t, leavesStyleOpen([]byte(out[:idx])),
		"results printed under an active style; stream was: %q", out)
}

// TestLogSink_NeverWritesEscapesToTheCommandsOwnStream checks the command's own
// stream matches a local run byte for byte, on a terminal as much as in a pipe.
func TestLogSink_NeverWritesEscapesToTheCommandsOwnStream(t *testing.T) {
	var buf bytes.Buffer
	// isTerminal, but not the diagnostics stream.
	sink := &logSink{w: &buf, isTerminal: true}

	require.NoError(t, sink.live([]byte("results \x1b[36mstill open\n")))

	require.Equal(t, "results \x1b[36mstill open\n", buf.String())
}

// TestPrintLogs_ReadsTheMergedLogWhenBothStreamsShareADestination covers
// cross-stream ordering: two readers race, so the merged log is the one to read
// when the interleaving is visible.
func TestPrintLogs_ReadsTheMergedLogWhenBothStreamsShareADestination(t *testing.T) {
	client := &scriptedBuildBuddyClient{
		scripts: map[elpb.LogType][]scriptedRecv{
			elpb.LogType_BUILD_LOG: {
				{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("INFO: Loading\n//some:target\nINFO: Done\n")}},
			},
			// Reading either of these would lose the interleaving, so neither
			// should be touched.
			elpb.LogType_STDOUT_LOG: {
				{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("//some:target\n")}},
			},
			elpb.LogType_STDERR_LOG: {
				{rsp: &elpb.GetEventLogChunkResponse{Buffer: []byte("INFO: Loading\nINFO: Done\n")}},
			},
		},
	}

	// One file for both streams, as a terminal or `> file 2>&1` gives.
	f, err := os.CreateTemp(t.TempDir(), "merged")
	require.NoError(t, err)
	oldStdout, oldStderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = f, f
	err = printLogs(t.Context(), client, "test-invocation-id")
	os.Stdout, os.Stderr = oldStdout, oldStderr
	require.NoError(t, err)

	require.NoError(t, f.Close())
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	// In order, and exactly once - not the split logs concatenated.
	require.Equal(t, "INFO: Loading\n//some:target\nINFO: Done\n", string(b))
}
