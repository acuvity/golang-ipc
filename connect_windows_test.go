package ipc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

const fileCreatePipeInstance = 0x4

func TestSharedPipeDACL(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := fmt.Sprintf("shared_dacl_%d", time.Now().UnixNano())

	s, err := StartServer(ctx, name, &ServerConfig{Encryption: true, UnmaskPermissions: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)

	// winio waits while all instances are busy, GetNamedSecurityInfo fails instead.
	dialCtx, dialCancel := context.WithTimeout(ctx, multiTimeout)
	defer dialCancel()

	pc, err := winio.DialPipeAccess(dialCtx, pipeBase+name, windows.READ_CONTROL)
	if err != nil {
		t.Fatalf("unable to open pipe: %v", err)
	}

	sd, err := windows.GetSecurityInfo(windows.Handle(pc.(interface{ Fd() uintptr }).Fd()), windows.SE_KERNEL_OBJECT, windows.DACL_SECURITY_INFORMATION)
	_ = pc.Close()
	if err != nil {
		t.Fatalf("unable to read pipe security: %v", err)
	}

	dacl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}

	authUsers, err := windows.CreateWellKnownSid(windows.WinAuthenticatedUserSid)
	if err != nil {
		t.Fatal(err)
	}

	full := map[string]bool{}
	for _, wk := range []windows.WELL_KNOWN_SID_TYPE{windows.WinLocalSystemSid, windows.WinBuiltinAdministratorsSid, windows.WinCreatorOwnerRightsSid} {
		sid, err := windows.CreateWellKnownSid(wk)
		if err != nil {
			t.Fatal(err)
		}
		full[sid.String()] = true
	}

	var sawAuthUsers bool
	for i := range uint32(dacl.AceCount) {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil {
			t.Fatal(err)
		}

		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			continue
		}

		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		mask := uint32(ace.Mask)

		switch {
		case sid.Equals(authUsers):
			sawAuthUsers = true
			if mask&fileCreatePipeInstance != 0 {
				t.Errorf("authenticated users can create pipe instances (mask 0x%x)", mask)
			}
			if mask&(windows.FILE_READ_DATA|windows.FILE_WRITE_DATA) != windows.FILE_READ_DATA|windows.FILE_WRITE_DATA {
				t.Errorf("authenticated users cannot read and write (mask 0x%x)", mask)
			}
		case full[sid.String()]:
		default:
			if mask&fileCreatePipeInstance != 0 {
				t.Errorf("unexpected principal %s can create pipe instances (mask 0x%x)", sid, mask)
			}
		}
	}

	if !sawAuthUsers {
		t.Error("authenticated users are not allowed to connect")
	}

	c := startTestClient(ctx, t, name)
	t.Cleanup(c.Close)
	c.waitStatus(t, "Connected")

	m := readServer(ctx, t, s, isStatus("Connected"))
	if err := m.Conn.Write(2, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := c.waitData(t); string(got.Data) != "hello" {
		t.Fatalf("unexpected data %q", got.Data)
	}
}

func TestSharedPipeClientAccess(t *testing.T) {

	name := pipeBase + fmt.Sprintf("shared_access_%d", time.Now().UnixNano())

	// No owner or administrators entries, so this process only gets the authenticated users access.
	sddl := fmt.Sprintf("D:P(A;;GA;;;SY)(A;;0x%x;;;AU)", sharedPipeClientAccess)

	first, err := createPipeInstance(name, sddl, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = windows.CloseHandle(first) }()

	dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := winio.DialPipeContext(dialCtx, name)
	if err != nil {
		t.Fatalf("client should be able to connect: %v", err)
	}
	_ = conn.Close()

	second, err := createPipeInstance(name, sddl, false)
	if err == nil {
		_ = windows.CloseHandle(second)
		t.Fatal("an authenticated user should not be able to create another instance of the pipe")
	}
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("expected access denied, got %v", err)
	}
}

func createPipeInstance(name, sddl string, first bool) (windows.Handle, error) {

	sd, err := windows.SecurityDescriptorFromString(sddl)
	if err != nil {
		return 0, err
	}

	sa := &windows.SecurityAttributes{SecurityDescriptor: sd}
	sa.Length = uint32(unsafe.Sizeof(*sa))

	flags := uint32(windows.PIPE_ACCESS_DUPLEX)
	if first {
		flags |= windows.FILE_FLAG_FIRST_PIPE_INSTANCE
	}

	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}

	return windows.CreateNamedPipe(path, flags, windows.PIPE_TYPE_BYTE|windows.PIPE_REJECT_REMOTE_CLIENTS, windows.PIPE_UNLIMITED_INSTANCES, 4096, 4096, 0, sa)
}
