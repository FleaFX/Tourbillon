namespace Tourbillon.Testing;

/// <summary>
/// The exception a simulated machine's file system throws when the machine crashed
/// during a file operation, and on every file operation until it restarts: an
/// <see cref="IOException"/>, the way a disk fails after a crash.
/// </summary>
sealed class SimulatedCrashException : IOException {
    /// <summary>
    /// The machine crashed during a file operation.
    /// </summary>
    public SimulatedCrashException()
        : base("The simulated machine crashed during a file operation.") {
    }
}
