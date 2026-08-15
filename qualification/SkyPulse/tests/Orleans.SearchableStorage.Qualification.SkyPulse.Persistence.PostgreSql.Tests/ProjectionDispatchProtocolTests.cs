using Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Persistence.PostgreSql.Tests;

public sealed class ProjectionDispatchProtocolTests
{
    private static readonly ProjectionDispatchAction[] UpsertSequence =
    [
        ProjectionDispatchAction.PrepareHydration,
        ProjectionDispatchAction.UpsertSearchableIndex,
        ProjectionDispatchAction.Finalize,
    ];

    private static readonly ProjectionDispatchAction[] RemoveSequence =
    [
        ProjectionDispatchAction.RemoveSearchableIndex,
        ProjectionDispatchAction.Finalize,
    ];

    [Fact]
    public void UpsertPreparesHydrationBeforeExternalDiscovery()
    {
        Assert.Equal(UpsertSequence, ProjectionDispatchProtocol.GetActions(ProjectionOperation.Upsert));
    }

    [Fact]
    public void RemoveStopsExternalDiscoveryBeforeDeletingHydration()
    {
        Assert.Equal(RemoveSequence, ProjectionDispatchProtocol.GetActions(ProjectionOperation.Remove));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void UpsertSequenceIsStableForEveryObservedPrefix(int completedActionCount)
    {
        var firstAttempt = ProjectionDispatchProtocol.GetActions(ProjectionOperation.Upsert);
        var completedBeforeCrash = firstAttempt.Take(completedActionCount).ToArray();

        var requiredSequence = ProjectionDispatchProtocol.GetActions(ProjectionOperation.Upsert);

        Assert.Equal(UpsertSequence.Take(completedActionCount), completedBeforeCrash);
        Assert.Equal(UpsertSequence, requiredSequence);
        Assert.True(IndexOf(requiredSequence, ProjectionDispatchAction.PrepareHydration)
            < IndexOf(requiredSequence, ProjectionDispatchAction.UpsertSearchableIndex));
        Assert.Equal(ProjectionDispatchAction.Finalize, requiredSequence[^1]);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void RemoveSequenceIsStableForEveryObservedPrefix(int completedActionCount)
    {
        var firstAttempt = ProjectionDispatchProtocol.GetActions(ProjectionOperation.Remove);
        var completedBeforeCrash = firstAttempt.Take(completedActionCount).ToArray();

        var requiredSequence = ProjectionDispatchProtocol.GetActions(ProjectionOperation.Remove);

        Assert.Equal(RemoveSequence.Take(completedActionCount), completedBeforeCrash);
        Assert.Equal(RemoveSequence, requiredSequence);
        Assert.Equal(ProjectionDispatchAction.RemoveSearchableIndex, requiredSequence[0]);
        Assert.Equal(ProjectionDispatchAction.Finalize, requiredSequence[^1]);
    }

    private static int IndexOf(IReadOnlyList<ProjectionDispatchAction> actions, ProjectionDispatchAction action)
    {
        for (var index = 0; index < actions.Count; index++)
        {
            if (actions[index] == action)
            {
                return index;
            }
        }

        return -1;
    }
}
