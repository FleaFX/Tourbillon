namespace Tourbillon.Raft;

/// <summary>
/// The role of a replica in its group.
/// </summary>
enum ReplicaRole : byte {
    /// <summary>
    /// The replica follows a leader or waits for one.
    /// </summary>
    Follower = 0,

    /// <summary>
    /// The replica asks the group for votes.
    /// </summary>
    Candidate = 1,

    /// <summary>
    /// The replica leads the group.
    /// </summary>
    Leader = 2,
}
