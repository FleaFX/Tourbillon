namespace Tourbillon.Raft;

/// <summary>
/// The role of a copy in its group.
/// </summary>
enum ReplicaRole : byte {
    /// <summary>
    /// The copy follows a leader or waits for one.
    /// </summary>
    Follower = 0,

    /// <summary>
    /// The copy asks the group for votes.
    /// </summary>
    Candidate = 1,

    /// <summary>
    /// The copy leads the group.
    /// </summary>
    Leader = 2,
}
