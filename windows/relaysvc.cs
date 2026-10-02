// relaysvc.cs — Windows service wrapper for one axona relay slot.
//
// One instance per SCM service (axona-relay-01 … -NN). The SCM guarantees one
// wrapper per service. The wrapper owns an unnamed Job Object with
// KILL_ON_JOB_CLOSE and holds its only handle (not inheritable, no breakaway),
// so if the wrapper dies for any reason the relay dies with it: no orphan can
// outlive its service. The relay's launcher is created SUSPENDED, assigned to
// the job, then resumed, so no code runs outside the job.
//
// If the relay exits on its own the wrapper exits non-zero WITHOUT reporting
// SERVICE_STOPPED; the SCM treats that as a crash and applies the service's
// failure actions (restart). A service stop terminates the job.
//
// Build (on the host, no download):
//   C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe /nologo /out:relaysvc.exe relaysvc.cs
// Service binPath:
//   "C:\axona\relaysvc.exe" <repoDir> <logPath>
// Relay environment comes from the service's registry Environment value.

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.ServiceProcess;
using System.Threading;

public class RelayService : ServiceBase
{
    [StructLayout(LayoutKind.Sequential)]
    struct STARTUPINFO {
        public int cb; public string lpReserved, lpDesktop, lpTitle;
        public int dwX, dwY, dwXSize, dwYSize, dwXCountChars, dwYCountChars, dwFillAttribute, dwFlags;
        public short wShowWindow, cbReserved2; public IntPtr lpReserved2, hStdInput, hStdOutput, hStdError;
    }
    [StructLayout(LayoutKind.Sequential)]
    struct PROCESS_INFORMATION { public IntPtr hProcess, hThread; public int dwProcessId, dwThreadId; }
    [StructLayout(LayoutKind.Sequential)]
    struct JOBOBJECT_BASIC_LIMIT_INFORMATION {
        public long PerProcessUserTimeLimit, PerJobUserTimeLimit; public uint LimitFlags;
        public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize; public uint ActiveProcessLimit;
        public UIntPtr Affinity; public uint PriorityClass, SchedulingClass;
    }
    [StructLayout(LayoutKind.Sequential)]
    struct IO_COUNTERS { public ulong a, b, c, d, e, f; }
    [StructLayout(LayoutKind.Sequential)]
    struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
        public JOBOBJECT_BASIC_LIMIT_INFORMATION Basic; public IO_COUNTERS Io;
        public UIntPtr ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed;
    }

    [DllImport("kernel32.dll", SetLastError = true)] static extern IntPtr CreateJobObject(IntPtr sa, string name);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool SetInformationJobObject(IntPtr job, int cls, ref JOBOBJECT_EXTENDED_LIMIT_INFORMATION info, int len);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool AssignProcessToJobObject(IntPtr job, IntPtr proc);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool TerminateJobObject(IntPtr job, uint code);
    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    static extern bool CreateProcess(string app, string cmd, IntPtr pa, IntPtr ta, bool inherit, uint flags, IntPtr env, string cwd, ref STARTUPINFO si, out PROCESS_INFORMATION pi);
    [DllImport("kernel32.dll", SetLastError = true)] static extern uint ResumeThread(IntPtr thread);
    [DllImport("kernel32.dll", SetLastError = true)] static extern uint WaitForSingleObject(IntPtr h, uint ms);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool GetExitCodeProcess(IntPtr h, out uint code);
    [DllImport("kernel32.dll", SetLastError = true)] static extern bool CloseHandle(IntPtr h);

    const uint CREATE_SUSPENDED = 0x4, CREATE_NO_WINDOW = 0x08000000, CREATE_UNICODE_ENVIRONMENT = 0x400;
    const uint JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000;
    const int JobObjectExtendedLimitInformation = 9;

    readonly string repo, log;
    IntPtr job = IntPtr.Zero, proc = IntPtr.Zero;
    volatile bool stopping;

    RelayService(string repo, string log) { this.repo = repo; this.log = log; CanStop = true; CanShutdown = true; AutoLog = true; }

    public static void Main(string[] args)
    {
        if (args.Length < 2) { Console.Error.WriteLine("usage: relaysvc <repoDir> <logPath>"); Environment.Exit(2); }
        ServiceBase.Run(new RelayService(args[0], args[1]));
    }

    void Note(string s)
    {
        try { File.AppendAllText(log, DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") + " [relaysvc] " + s + Environment.NewLine); } catch { }
    }

    protected override void OnStart(string[] _)
    {
        string dir = Path.GetDirectoryName(log);
        Directory.CreateDirectory(dir);
        // One previous generation is kept; every start writes a fresh log, so a
        // readiness gate reads only this run's lines.
        try { if (File.Exists(log)) { string prev = log + ".1"; if (File.Exists(prev)) File.Delete(prev); File.Move(log, prev); } } catch { }

        job = CreateJobObject(IntPtr.Zero, null);   // unnamed, handle not inheritable
        if (job == IntPtr.Zero) throw new Exception("CreateJobObject failed " + Marshal.GetLastWin32Error());
        var info = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION();
        info.Basic.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;   // no BREAKAWAY_OK
        if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation, ref info, Marshal.SizeOf(info)))
            throw new Exception("SetInformationJobObject failed " + Marshal.GetLastWin32Error());

        string comspec = Environment.GetEnvironmentVariable("ComSpec") ?? @"C:\Windows\System32\cmd.exe";
        string cmd = "\"" + comspec + "\" /d /c node src\\index.js >> \"" + log + "\" 2>&1";
        var si = new STARTUPINFO(); si.cb = Marshal.SizeOf(si);
        PROCESS_INFORMATION pi;
        if (!CreateProcess(null, cmd, IntPtr.Zero, IntPtr.Zero, false, CREATE_SUSPENDED | CREATE_NO_WINDOW | CREATE_UNICODE_ENVIRONMENT, IntPtr.Zero, repo, ref si, out pi))
            throw new Exception("CreateProcess failed " + Marshal.GetLastWin32Error());
        if (!AssignProcessToJobObject(job, pi.hProcess)) {
            int e = Marshal.GetLastWin32Error();
            TerminateJobObject(job, 1);
            throw new Exception("AssignProcessToJobObject failed " + e + " (relay not started)");
        }
        ResumeThread(pi.hThread); CloseHandle(pi.hThread);
        proc = pi.hProcess;
        Note("started launcher pid " + pi.dwProcessId + " in job; repo=" + repo);

        var t = new Thread(Watch); t.IsBackground = true; t.Start();
    }

    void Watch()
    {
        WaitForSingleObject(proc, 0xFFFFFFFF);
        if (stopping) return;
        uint code; GetExitCodeProcess(proc, out code);
        Note("relay exited on its own, code " + code + "; exiting non-zero so the SCM restarts the service");
        TerminateJobObject(job, 1);
        Environment.Exit(1);
    }

    void Halt(string why)
    {
        stopping = true;
        Note("stop requested (" + why + "); terminating job");
        if (job != IntPtr.Zero) TerminateJobObject(job, 0);
        if (proc != IntPtr.Zero) WaitForSingleObject(proc, 15000);
    }

    protected override void OnStop() { Halt("stop"); }
    protected override void OnShutdown() { Halt("shutdown"); }
}
