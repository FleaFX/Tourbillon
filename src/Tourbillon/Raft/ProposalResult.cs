namespace Tourbillon.Raft;

/// <summary>
/// What became of a proposal.
/// </summary>
enum ProposalStatus : byte {
    /// <summary>
    /// The entry is committed.
    /// </summary>
    Accepted = 1,

    /// <summary>
    /// The entry will never be committed by this copy: it was not written, and a refusal
    /// is final.
    /// </summary>
    Refused = 2,

    /// <summary>
    /// The entry was written but this copy lost track of it: it may or may not commit.
    /// </summary>
    Unknown = 3,
}

/// <summary>
/// Why a proposal was refused.
/// </summary>
enum RefusalReason : byte {
    /// <summary>
    /// No refusal.
    /// </summary>
    None = 0,

    /// <summary>
    /// This copy is not the leader.
    /// </summary>
    NotLeader = 1,

    /// <summary>
    /// Storage is full: own log or too few followers have room for the entry.
    /// </summary>
    StorageFull = 2,
}

/// <summary>
/// The outcome of one proposal.
/// </summary>
/// <param name="Status">What became of the proposal.</param>
/// <param name="Index">The index of the entry; 0 when it was never written.</param>
/// <param name="Reason">Why it was refused; <see cref="RefusalReason.None"/> otherwise.</param>
/// <param name="Leader">The leader this copy knows of, when it is not the leader itself.</param>
readonly record struct ProposalResult(ProposalStatus Status, ulong Index, RefusalReason Reason, string? Leader) {
    /// <summary>
    /// The entry is committed at <paramref name="index"/>.
    /// </summary>
    public static ProposalResult Accepted(ulong index) => new(ProposalStatus.Accepted, index, RefusalReason.None, null);

    /// <summary>
    /// The entry was not written; it will never commit by this copy.
    /// </summary>
    public static ProposalResult Refused(RefusalReason reason, string? leader = null) => new(ProposalStatus.Refused, 0, reason, leader);

    /// <summary>
    /// The entry was written but this copy lost track of it.
    /// </summary>
    public static ProposalResult Unknown => new(ProposalStatus.Unknown, 0, RefusalReason.None, null);
}
